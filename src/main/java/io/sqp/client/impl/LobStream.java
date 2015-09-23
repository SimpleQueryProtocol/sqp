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

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletionException;

/**
 * @author Stefan Burnicki
 */
public class LobStream extends InputStream {
    private SqpConnectionImpl _connection;
    private String _lobId;
    private long _lobSize;
    private long _absolutePosition;
    private int _bufferPosition;
    private byte[] _buffer;

    public LobStream(SqpConnectionImpl connection, String lobId, long lobSize) {
        _lobId = lobId;
        _connection = connection;
        _lobSize = lobSize;
    }

    @Override
    public int read() throws IOException {
        if (_buffer == null || _bufferPosition >= _buffer.length) {
            _buffer = getData(_absolutePosition);
            _bufferPosition = 0;
        }
        if (_bufferPosition >= _buffer.length || _absolutePosition > _lobSize) {
            return -1;
        }
        _bufferPosition++;
        _absolutePosition++;
        // don't forget to mask, otherwise Java's cast to int will cast it signed
        return (0xFF & _buffer[_bufferPosition - 1]);
    }

    private byte[] getData(long absPosition) throws IOException {
        if (_lobSize >= 0 && absPosition >= _lobSize) {
            return new byte[0];
        }
        try {
            return _connection.getLob(_lobId, absPosition).join();
        } catch (CompletionException e) {
            throw new IOException("Failed to get new LOB data: " + e.getMessage(), e);
        }
    }

    @Override
    public long skip(long l) throws IOException {
        long newPos = _absolutePosition + l;
        if (newPos > _lobSize) {
            l = _lobSize - _absolutePosition;
        }
        _absolutePosition += l;
        _bufferPosition += l;
        return l;
    }

    @Override
    public void close() throws IOException {
        // TODO: one could implement a method to close the actual LOB on the server
    }

    public long getLobSize() {
        return _lobSize;
    }
}
