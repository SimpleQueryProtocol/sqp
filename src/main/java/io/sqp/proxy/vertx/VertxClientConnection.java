/*
 * Copyright 2015 by Rothmeyer Consulting (http://www.rothmeyer.com/)
 * Author: Stefan Burnicki <stefan.burnicki@burnicki.net>
 *
 * This file is part of SQP.
 *
 * SQP is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License.
 *
 * SQP is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SQP.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.sqp.proxy.vertx;

import io.sqp.core.DataFormat;
import io.sqp.proxy.*;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.http.WebSocketFrame;
import io.vertx.core.streams.Pump;
import io.sqp.core.exceptions.SqpException;
import io.sqp.core.jackson.JacksonMessageDecoder;
import io.sqp.core.jackson.JacksonMessageEncoder;
import io.sqp.core.messages.SqpMessage;
import io.sqp.proxy.exceptions.InvalidFrameException;
import io.sqp.proxy.exceptions.ServerErrorException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Stefan Burnicki
 */
public class VertxClientConnection implements ClientConnection {
    private ExecutorService _executorService;
    private int _maxFrameSize;
    private ServerWebSocket _socket;
    private MessageReceiver _msgReceiver;
    private ClientSession _session;
    private DataFormat _messageFormat;
    private WebsocketSendQueueStream _wsSendQueueStream;
    private WebsocketWriteStream _wsWriteStream;
    private boolean _closed;
    private Pump _wsSendingPump;

    protected Logger logger;

    public VertxClientConnection(ExecutorService executorService, ServerWebSocket websocket, BackendConnectionPool connectionPool, int maxFrameSize) {
        _executorService = executorService;
        _maxFrameSize = maxFrameSize;
        _messageFormat = DataFormat.Text;
        _socket = websocket;
        _session = new ClientSession(connectionPool, this);
        _msgReceiver = new MessageReceiver(_session, new JacksonMessageDecoder());
        logger = Logger.getGlobal();
        initSendingPump();
        registerHandlers();
        logger.log(Level.INFO, "New client connected via websocket.");
    }

    private void initSendingPump() {
        _wsWriteStream = new WebsocketWriteStream(_socket, _maxFrameSize);
        _wsSendQueueStream = new WebsocketSendQueueStream();
        _wsSendQueueStream.streamEndedHandler(v -> _wsWriteStream.finishCurrentMessage());
        _wsSendQueueStream.streamStartedHandler(_wsWriteStream::setDataFormat);
        _wsSendingPump = Pump.pump(_wsSendQueueStream, _wsWriteStream);
        _wsSendingPump.start();
    }

    @Override
    public void reset() {
        _msgReceiver.reset();
    }

    @Override
    public void setMessageFormat(DataFormat format) {
        _messageFormat = format;
    }

    @Override
    public void sendMessage(SqpMessage message) {
        try {
            sendMessageInternal(message);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to encode message of type '" + message.getType() + "'. Closing connection");
            // try to write an error message
            ServerErrorException error = new ServerErrorException("Encoding the server message failed."
                    + " This is likely to be a server problem.");
            try {
                sendMessageInternal(error.toErrorMessage());
            } catch (IOException e1) {
                logger.log(Level.SEVERE, "Even sending an error message failed!");
            }
            // close that connection
            close();
        }
    }

    private void sendMessageInternal(SqpMessage message) throws IOException {
        if (_closed) {
            logger.log(Level.SEVERE, "Attempt to send message after socket close: " + message);
            return;
        }
        // first encode the message into a buffer
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        new JacksonMessageEncoder().encode(outputStream, _messageFormat, message);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        // than add a reader for this buffer to the sending queue
        _wsSendQueueStream.addStream(new AsyncInputStream(_executorService, inputStream), _messageFormat);
    }

    private boolean checkOpen(SqpMessage message) {
        if (_closed) {
            logger.log(Level.SEVERE, "Attempt to send message after socket close: " + message);
            return false;
        }
        return true;
    }

    @Override
    public void close() {
        _closed = true;
        _wsSendQueueStream.endHandler(v -> _socket.close());
    }

    @Override
    public void sendStream(InputStream inputStream, DataFormat format) {
        if (_closed) {
            logger.log(Level.SEVERE, "Attempt to send " + format + " stream after socket close.");
            return;
        }
        // simply add to sending queue
        _wsSendQueueStream.addStream(new AsyncInputStream(_executorService, inputStream), format);
    }

    public void handleFrame(WebSocketFrame frame) {
        ByteBuffer buf = new VertxByteBuffer(frame.binaryData());
        boolean isFinal = frame.isFinal();
        boolean isText = frame.isText();

        try {
            if (isText || frame.isBinary()) {
                DataFormat format = isText ? DataFormat.Text : DataFormat.Binary;
                _msgReceiver.newMessage(format, buf, isFinal);
            } else if (frame.isContinuation()) {
                _msgReceiver.continueMessage(buf, isFinal);
            } else {
                throw new InvalidFrameException("Invalid frame type");
            }
        } catch (SqpException e) {
            _session.handleError(e);
        }
    }

    public void handleClose(Void v) {
        // TODO: make the connectHandler settable
        _session.onClientClose();
    }

    private void registerHandlers() {
        _socket.closeHandler(this::handleClose);
        _socket.frameHandler(this::handleFrame);
    }
}
