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

package io.sqp.client.impl;

import io.sqp.client.exceptions.ConnectionException;
import io.sqp.client.exceptions.ErrorResponseException;
import io.sqp.client.exceptions.UnexpectedMessageException;
import io.sqp.core.DataFormat;
import io.sqp.core.jackson.JacksonMessageDecoder;
import io.sqp.core.messages.ErrorMessage;
import io.sqp.core.messages.SqpMessage;
import io.sqp.core.MessageDecoder;
import io.sqp.core.exceptions.DecodingException;
import io.sqp.core.exceptions.SqpException;
import io.sqp.core.messages.MessageType;

import javax.websocket.Session;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Stefan Burnicki
 */
public class MessageHandler {
    private SqpConnectionImpl _connection;
    private MessageDecoder _messageDecoder;
    private ResponseHandlerQueue _responseHandlerQueue;
    private boolean _waitingForLob;
    private CompletableFuture<byte[]> _lobConsumer;
    private Logger _logger;

    public MessageHandler(SqpConnectionImpl connection, Session session, Logger logger) {
        _connection = connection;
        _logger = logger;
        _messageDecoder = new JacksonMessageDecoder();
        _responseHandlerQueue = new ResponseHandlerQueue();
        session.addMessageHandler(Reader.class, this::receiveStrMessage);
        session.addMessageHandler(byte[].class, this::receiveBinMessage);
        _waitingForLob = false;
        _lobConsumer = null;
    }

    synchronized void close() {
        ResponseHandler handler;
        while ((handler = _responseHandlerQueue.current()) != null) {
            handler.fail(new ConnectionException("Connection closed."));
            _responseHandlerQueue.proceed();
        }
    }

    synchronized void setLobConsumer(CompletableFuture<byte[]> lobConsumer) {
        _lobConsumer = lobConsumer;
    }

    void addResponseHandler(ResponseHandler responseHandler) {
        _responseHandlerQueue.add(responseHandler);
    }

    private synchronized void receiveStrMessage(Reader msgReader) {
        // we need to synchronize decoding & message handling, otherwise the order of the messages
        // might mix up. Example: If this method is called with a long message first, and then with a short message
        // from a different thread (that is probably the case), we might end up with the second message being
        // decoded and handled before the first.
        // Synchronizing the whole block should assure that the first message blocks message handling until the first
        // message is handled properly.
        try {
            if (_waitingForLob) {
                receiveLob(readerToByteArray(msgReader));
                return;
            }
            handleSqpMessage(_messageDecoder.decode(msgReader));
        } catch (DecodingException e) {
            _connection.handleError(e);
        }
    }

    private synchronized void receiveBinMessage(byte[] bytes) {
        // TODO: there seems to be a bug in tyrus InputStream implementation with binary frames which results in
        // unforeseeable message orders and partial messages. This doesn't happen when we get the whole buffer at once.
        // So we do this until the bug is fixed. Then we should switch back to the InputStream implementation of
        // receiveBinMessage
        try {
            if (_waitingForLob) {
                receiveLob(bytes);
                return;
            }
            InputStream byteStream = new ByteArrayInputStream(bytes);
            handleSqpMessage(_messageDecoder.decode(DataFormat.Binary, byteStream));
        } catch (DecodingException e) {
            _connection.handleError(e);
        }
    }

    private void receiveLob(byte[] bytes) {
        _waitingForLob = false;
        if (_lobConsumer == null) {
            _logger.log(Level.WARNING, "Got an announced LOB but didn't have a consumer");
            return;
        }
        _lobConsumer.complete(bytes);
    }

    private void handleSqpMessage(SqpMessage msg) {
        ResponseHandler currentHandler = _responseHandlerQueue.current();

        // first check for a LOB expectation and change mode
        if (msg.isA(MessageType.LobAnnouncementMessage)) {
            _waitingForLob = true;
        }
        // first check for an error message
        if (msg.isA(MessageType.ErrorMessage)) {
            try {
                ErrorMessage errorMsg = msg.secureCast();
                SqpException error = new ErrorResponseException(errorMsg.getErrorType(), errorMsg.getMessage());
                _connection.handleError(error);
            } catch (DecodingException e) {
                _connection.handleError(e);
            }
            return;
        }

        // we check for handlers in queue to process the results
        boolean wasHandled = false;
        if (currentHandler != null) {
            // If the handler finished, release it and proceed to the new
            try {
                if (currentHandler.handle(msg)) {
                    _responseHandlerQueue.proceed();
                }
            } catch (SqpException e) {
                _connection.handleError(e);
                return;
            }
            wasHandled = true;
        }

        // now some default message actions
        if (msg.isA(MessageType.ReadyMessage)) {
            _connection.setState(ConnectionState.ReadyToSend);
            wasHandled = true;
        }

        // if the message wasn't handled, throw an error
        if (!wasHandled) {
            _connection.handleError(new UnexpectedMessageException("not awaiting any messages", msg));
        }
    }

    public boolean failCurrentResponseHandler(SqpException error) {
        ResponseHandler currentHandler = _responseHandlerQueue.current();
        if (currentHandler == null) {
            return false;
        }
        currentHandler.fail(error);
        _responseHandlerQueue.proceed();
        return true;
    }

    public byte[] readerToByteArray(Reader reader) throws DecodingException {
        char[] charBuffer = new char[100 * 1024];
        StringBuilder builder = new StringBuilder();
        int numCharsRead;
        try {
            while ((numCharsRead = reader.read(charBuffer, 0, charBuffer.length)) != -1) {
                builder.append(charBuffer, 0, numCharsRead);
            }
            reader.close();
            return builder.toString().getBytes(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new DecodingException("Failed to decode string message into byte array", e);
        }
    }
}
