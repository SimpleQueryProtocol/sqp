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

import io.sqp.proxy.ByteBuffer;
import io.vertx.core.buffer.Buffer;

/**
 * @author Stefan Burnicki
 */
public class VertxByteBuffer implements ByteBuffer {
    private Buffer _buffer;

    public VertxByteBuffer(Buffer buffer) {
        _buffer = buffer;
    }

    public VertxByteBuffer() {
        _buffer = Buffer.buffer();
    }

    @Override
    public byte getByte(int i) {
        return _buffer.getByte(i);
    }

    @Override
    public ByteBuffer getBuffer(int start, int end) {
        return new VertxByteBuffer(_buffer.getBuffer(start, end));
    }

    @Override
    public int length() {
        return _buffer.length();
    }

    @Override
    public ByteBuffer append(ByteBuffer buffer) {
        if (buffer instanceof VertxByteBuffer) {
            _buffer.appendBuffer(((VertxByteBuffer) buffer)._buffer);
        }
        else {
            _buffer.appendBytes(buffer.getBytes());
        }
        return this;
    }

    @Override
    public ByteBuffer append(String string, String encoding) {
        _buffer.appendString(string, encoding);
        return this;
    }

    @Override
    public byte[] getBytes() {
        return _buffer.getBytes();
    }

    @Override
    public ByteBuffer append(byte b) {
        _buffer.appendByte(b);
        return this;
    }

    @Override
    public String toString(String encoding) {
        return _buffer.toString(encoding);
    }

    @Override
    public String toString() {
        return toString("UTF-8");
    }
}
