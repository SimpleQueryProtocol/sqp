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

/**
 * Taken from the Wisdom Framework.
 * Ported to Vertx 3, Java 8, decoupled from apache commons by Stefan Burnicki <stefan.burnicki@burnicki.net>.
 */

package io.sqp.proxy.vertx;

import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.concurrent.ExecutorService;

/**
 * Reads an input stream in an asynchronous and Vert.X compliant way.
 * Instances acts a finite state machine with 3 different states: {@literal ACTIVE, PAUSED,
 * CLOSED}. The transition between the states depends on the control flow (i.e. the pump consuming the stream).
 */
public class AsyncInputStream implements ReadStream<Buffer> {

    /**
     * PAUSED state.
     */
    public static final int STATUS_PAUSED = 0;

    /**
     * ACTIVE state.
     */
    public static final int STATUS_ACTIVE = 1;

    /**
     * CLOSED state.
     */
    public static final int STATUS_CLOSED = 2;

    /**
     * Default chunk size.
     */
    static final int DEFAULT_CHUNK_SIZE = 8192;

    /**
     * An empty byte array.
     */
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    /**
     * The executor used to read the chunks.
     */
    private final ExecutorService executor;

    /**
     * A push back input stream wrapping the read input stream.
     */
    private final PushbackInputStream in;

    /**
     * The chunk size.
     */
    private final int chunkSize;

    /**
     * The current state.
     */
    private int state = STATUS_ACTIVE;

    /**
     * The close handler invoked when the stream is completed or closed.
     */
    private Handler<Void> closeHandler;

    /**
     * The data handler receiving the data read from the stream.
     */
    private Handler<Buffer> dataHandler;

    /**
     * The failure handler called when an error is encountered while reading the stream.
     */
    private Handler<Throwable> failureHandler;

    /**
     * The number of byte read form the input stream.
     */
    private int offset;
    private Context context;

    /**
     * Creates an instance of {@link AsyncInputStream}. This constructor uses the default
     * chunk size.
     *
     * @param executor the executor used to read the chunk
     * @param in       the input stream to read
     */
    public AsyncInputStream(ExecutorService executor, InputStream in) {
        this(executor, in, DEFAULT_CHUNK_SIZE);
    }

    /**
     * Creates an instance of {@link AsyncInputStream}.
     *
     * @param executor  the executor used to read the chunk
     * @param in        the input stream to read
     * @param chunkSize the chunk size
     */
    public AsyncInputStream(ExecutorService executor, InputStream in, int chunkSize) {
        if (in == null) {
            throw new NullPointerException("in");
        }
        if (chunkSize <= 0) {
            throw new IllegalArgumentException(
                    "chunkSize: " + chunkSize +
                            " (expected: a positive integer)");
        }

        if (in instanceof PushbackInputStream) {
            this.in = (PushbackInputStream) in;
        } else {
            this.in = new PushbackInputStream(in);
        }
        this.chunkSize = chunkSize;
        this.executor = executor;
    }

    /**
     * Gets the current state.
     *
     * @return the state
     */
    public int getState() {
        return state;
    }

    /**
     * Sets the end handler.
     *
     * @param endHandler the handler called when the stream is read completely.
     * @return the current {@link AsyncInputStream}
     */
    @Override
    public AsyncInputStream endHandler(Handler<Void> endHandler) {
        this.closeHandler = endHandler;
        return this;
    }

    /**
     * Sets the data handler. This method 'starts' the stream reading.
     *
     * @param handler the handler called with the read chunks from the backed input stream. Must not be {@code null}.
     * @return the current {@link AsyncInputStream}
     */
    @Override
    public AsyncInputStream handler(Handler<Buffer> handler) {
        if (handler == null) {
            throw new IllegalArgumentException("handler");
        }
        this.dataHandler = handler;
        doRead();
        return this;
    }

    /**
     * The method actually reading the stream.
     * Except the first calls, this method is executed within an Akka thread.
     */
    private void doRead() {
        if (context == null) {
            context = Vertx.currentContext();
        }
        if (state == STATUS_ACTIVE) {
            final Handler<Buffer> dataHandler = this.dataHandler;
            final Handler<Void> closeHandler = this.closeHandler;
            executor.submit(
                    () -> {
                        try {
                            final byte[] bytes = readChunk();

                            if (bytes == null || bytes.length == 0) {
                                // null or 0 means we reach the end of the stream, invoke the close handler.
                                state = STATUS_CLOSED;
                                closeQuietly(in);
                                context.runOnContext(event -> {
                                    if (closeHandler != null) {
                                        closeHandler.handle(null);
                                    }
                                });
                            } else {
                                // We still have data, dispatch it.
                                context.runOnContext(event -> {
                                    dataHandler.handle(Buffer.buffer(bytes));
                                    // The next chunk will be read in another call, and maybe another thread.
                                    // As the data was already given to the data handler, this is fine.
                                    doRead();
                                });
                            }
                        } catch (final Exception e) {
                            // Error detected, invokes the failure handler.
                            state = STATUS_CLOSED;
                            closeQuietly(in);
                            /**
                             * Invokes the failure handler.
                             * @param event irrelevant
                             */
                            context.runOnContext(event -> {
                                if (failureHandler != null) {
                                    failureHandler.handle(e);
                                }
                            });
                        }
                    });
        }
    }

    /**
     * Pauses the reading.
     *
     * @return the current {@code AsyncInputStream}
     */
    @Override
    public AsyncInputStream pause() {
        if (state == STATUS_ACTIVE) {
            state = STATUS_PAUSED;
        }
        return this;
    }

    /**
     * Resumes the reading.
     *
     * @return the current {@code AsyncInputStream}
     */
    @Override
    public AsyncInputStream resume() {
        switch (state) {
            case STATUS_CLOSED:
                throw new IllegalStateException("Cannot resume, already closed");
            case STATUS_PAUSED:
                state = STATUS_ACTIVE;
                doRead();
        }
        return this;
    }

    /**
     * Sets the failure handler.
     *
     * @param handler the failure handler.
     * @return the current {@link AsyncInputStream}
     */
    @Override
    public AsyncInputStream exceptionHandler(Handler<Throwable> handler) {
        this.failureHandler = handler;
        return this;
    }

    /**
     * Retrieves the number of read bytes.
     *
     * @return the number of read bytes
     */
    public long transferredBytes() {
        return offset;
    }

    /**
     * Checks whether or not the current stream is closed.
     *
     * @return {@code true} if the current {@link AsyncInputStream} is in the "CLOSED" state.
     */
    public boolean isClosed() {
        return state == STATUS_CLOSED;
    }

    /**
     * Checks whether or not we reach the end of the stream.
     *
     * @return {@code true} if we read the end of the stream, {@code false} otherwise
     * @throws Exception if the stream cannot be read.
     */
    public boolean isEndOfInput() throws Exception {
        int b = in.read();
        if (b < 0) {
            return true;
        } else {
            in.unread(b);
            return false;
        }
    }

    /**
     * Reads a chunk.
     * @return the read bytes, empty if we reached the end of the stream. The returned array has exactly the sisize
     * of the chunk.
     * @throws Exception if the stream cannot be read.
     */
    private byte[] readChunk() throws Exception {
        if (isEndOfInput()) {
            return EMPTY_BYTE_ARRAY;
        }

        try {
            // transfer to buffer
            byte[] tmp = new byte[chunkSize];
            int readBytes = in.read(tmp);
            if (readBytes <= 0) {
                return null;
            }
            byte[] buffer = new byte[readBytes];
            System.arraycopy(tmp, 0, buffer, 0, readBytes);
            offset += readBytes;
            return buffer;
        } catch (IOException e) {
            // Close the stream, and propagate the exception.
            closeQuietly(in);
            throw e;
        }
    }

    private static void closeQuietly(Closeable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (IOException e) {}
    }
}
