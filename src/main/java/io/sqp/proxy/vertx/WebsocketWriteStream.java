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
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.http.WebSocketFrame;
import io.vertx.core.streams.WriteStream;

/**
 * @author Stefan Burnicki
 */
public class WebsocketWriteStream implements WriteStream<Buffer> {
    ServerWebSocket _socket;
    DataFormat _format;
    private int _maxFrameSize;
    private Buffer _buffer;
    private boolean _firstFrameWritten = false;
    private Handler<Void> _drainHandler;
    private Handler<Throwable> _exceptionHandler;
    private Buffer _finishBuffer;
    private boolean _waitForDrain;

    public WebsocketWriteStream(ServerWebSocket socket, int maxFrameSize) {
        _socket = socket;
        _buffer = Buffer.buffer(maxFrameSize + 1024);
        _maxFrameSize = maxFrameSize;
        _format = DataFormat.Text;
        _waitForDrain = false;
    }

    public void setDataFormat(DataFormat format) {
        _format = format;
    }

    public void finishCurrentMessage() {
        _finishBuffer = _buffer;
        _buffer = Buffer.buffer(_maxFrameSize + 1024);
        write();
    }

    @Override
    public WriteStream<Buffer> write(Buffer data) {
        _buffer.appendBuffer(data);
        write();
        return this;
    }

    @Override
    public WriteStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
        _exceptionHandler = handler;
        _socket.exceptionHandler(handler);
        return this;
    }

    @Override
    public WriteStream<Buffer> setWriteQueueMaxSize(int maxSize) {
        _socket.setWriteQueueMaxSize(Math.min(maxSize, _maxFrameSize));
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return _socket.writeQueueFull();
    }

    @Override
    public WriteStream<Buffer> drainHandler(Handler<Void> handler) {
        _drainHandler = handler;
        if (!_waitForDrain) {
            _socket.drainHandler(_drainHandler);
        }
        return this;
    }

    private void resumeWriting(Void v) {
        _socket.drainHandler(_drainHandler);
        _waitForDrain = false;
        write();
        // invoke once if possible
        if (!writeQueueFull() && _drainHandler != null) {
            _drainHandler.handle(v);
        }
    }

    private void write() {
        if (_finishBuffer != null) {
            flushFinish();
        }
        if (_buffer.length() >= _maxFrameSize) {
            flushBuffer();
        }
    }

    private void flushFinish() {
        int written = flushWhile(_finishBuffer, 0, _firstFrameWritten);
        _finishBuffer = _finishBuffer.getBuffer(written, _finishBuffer.length());
        if (_finishBuffer.length() == 0) {
            _finishBuffer = null;
            _firstFrameWritten = false;
        }
    }

    private void flushBuffer() {
        int written = flushWhile(_buffer, _maxFrameSize - 1, false);
        _buffer = _buffer.getBuffer(written, _buffer.length());
    }

    private int flushWhile(Buffer buffer, int maxLeft, boolean force) {
        int numLeft = buffer.length();
        int writtenTotal = 0;
        while ((numLeft > maxLeft || force)) {
            if (writeQueueFull()) {
                _waitForDrain = true;
                _socket.drainHandler(this::resumeWriting);
                break;
            }
            int numOut = Math.min(numLeft, _maxFrameSize);
            numLeft -= numOut;
            // set the final flag only if we're finishing and the buffer is empty
            boolean isFinal = _finishBuffer != null && numLeft == 0;

            flushSlice(buffer, writtenTotal, numOut, isFinal);
            writtenTotal += numOut;
            force = false;
        }
        return writtenTotal;
    }

    private void flushSlice(Buffer buffer, int from, int length, boolean isFinal) {
        // a slice is just a view on the same buffer: no need to copy something
        Buffer outBuf = buffer.slice(from, from + length);

        WebSocketFrame frame;
        if (!_firstFrameWritten) {
            if (_format == DataFormat.Binary) {
                frame = WebSocketFrame.binaryFrame(outBuf, isFinal);
            } else {
                frame = WebSocketFrame.textFrame(outBuf.toString("UTF-8"), isFinal);
            }
        } else {
            frame = WebSocketFrame.continuationFrame(outBuf, isFinal);
        }
        _socket.writeFrame(frame);
        _firstFrameWritten = true;
    }

}
