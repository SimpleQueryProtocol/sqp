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

package io.sqp.core.messages;

/**
 * @author Stefan Burnicki
 */
public class LobRequestMessage extends SqpMessage {
    private String _id;
    private long _offset = -1;
    private long _size = -1;

    public LobRequestMessage() {
        // for jackson decoding
    }

    public LobRequestMessage(String id, long offset, long size) {
        _id = id;
        _offset = offset;
        _size = size;
    }

    public String getId() {
        return _id;
    }

    public void setId(String id) {
        _id = id;
    }

    public long getOffset() {
        return _offset;
    }

    public void setOffset(long offset) {
        _offset = offset;
    }

    public long getSize() {
        return _size;
    }

    public void setSize(long size) {
        _size = size;
    }
}
