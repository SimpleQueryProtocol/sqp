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
import io.vertx.core.streams.ReadStream;

import java.util.LinkedList;
import java.util.Queue;

/**
 * @author Stefan Burnicki
 */
public class WebsocketSendQueueStream implements ReadStream<Buffer> {
    private boolean _isPaused;
    private Queue<QueuedStream> _streamQueue;
    private ReadStream<Buffer> _currentStream;
    private Handler<Void> _streamEndHandler;
    private Handler<DataFormat> _streamStartedHandler;
    private Handler<Buffer> _dataHandler;
    private Handler<Throwable> _exceptionHandler;
    private Handler<Void> _endHandler;

    public WebsocketSendQueueStream() {
        _streamQueue = new LinkedList<>();
    }

    public ReadStream<Buffer> addStream(ReadStream<Buffer> stream, DataFormat format) {
        stream.pause(); // make sure it's paused while in queue
        _streamQueue.add(new QueuedStream(stream, format));
        proceedToNextStream();
        return this;
    }

    public boolean hasActiveStream() {
        return _currentStream != null || !_streamQueue.isEmpty();
    }

    @Override
    public ReadStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
        _exceptionHandler = handler;
        return this;
    }

    @Override
    public ReadStream<Buffer> handler(Handler<Buffer> handler) {
        _dataHandler = handler;
        return this;
    }

    @Override
    public ReadStream<Buffer> pause() {
        _isPaused = true;
        if (_currentStream != null) {
            _currentStream.pause();
        }
        return this;
    }

    @Override
    public ReadStream<Buffer> resume() {
        _isPaused = false;
        if (_currentStream != null) {
            _currentStream.resume();
        } else {
            proceedToNextStream();
        }
        return this;
    }

    @Override
    public ReadStream<Buffer> endHandler(Handler<Void> endHandler) {
        _endHandler = endHandler;
        return this;
    }

    public ReadStream<Buffer> streamEndedHandler(Handler<Void> streamEndHandler) {
        _streamEndHandler = streamEndHandler;
        return this;
    }

    public ReadStream<Buffer> streamStartedHandler(Handler<DataFormat> streamStartedHandler) {
        _streamStartedHandler = streamStartedHandler;
        return this;
    }

    private void proceedToNextStream() {
        if (_currentStream != null) {
            return;
        }
        if (_streamQueue.isEmpty()) {
            if (_endHandler != null) {
                _endHandler.handle(null);
            }
            return;
        }
        // use next stream
        QueuedStream queuedStream = _streamQueue.poll();
        if (_streamStartedHandler != null) {
            _streamStartedHandler.handle(queuedStream.DataFormat);
        }
        _currentStream = queuedStream.Stream;

        // set handlers
        _currentStream.exceptionHandler(_exceptionHandler);
        _currentStream.endHandler(this::currentStreamEnded);
        _currentStream.handler(_dataHandler);

        // don't forget that queued streams are paused. Resume the current if possible
        if (!_isPaused) {
            _currentStream.resume();
        }
    }

    private void currentStreamEnded(Void v) {
        _currentStream = null;
        if (_streamEndHandler != null) {
            _streamEndHandler.handle(null);
        }
        proceedToNextStream();
    }

    private class QueuedStream {
        public final ReadStream<Buffer> Stream;
        public final DataFormat DataFormat;

        public QueuedStream(ReadStream<Buffer> stream, io.sqp.core.DataFormat dataFormat) {
            Stream = stream;
            DataFormat = dataFormat;
        }
    }
}
