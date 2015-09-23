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

import io.sqp.core.Defaults;

/**
 * @author Stefan Burnicki
 */

public class FetchDataMessage extends SqpMessage {
    private String _cursorId = Defaults.DefaultCursorID;
    private int _position = -1;
    private int _maxFetch = -1;
    private boolean _forward = true;

    // necessary for decoding

    public FetchDataMessage() {}

    public FetchDataMessage(String cursorId, int position, int maxFetch, boolean forward) {
        _cursorId = cursorId;
        _position = position;
        _maxFetch = maxFetch;
        _forward = forward;
    }

    public void setCursorId(String cursorId) {
        _cursorId = cursorId;
    }

    public void setPosition(int position) {
        _position = position;
    }

    public void setMaxFetch(int maxFetch) {
        _maxFetch = maxFetch;
    }

    public void setForward(boolean forward) {
        _forward = forward;
    }

    public String getCursorId() {
        return _cursorId;
    }

    public int getPosition() {
        return _position;
    }

    public int getMaxFetch() {
        return _maxFetch;
    }

    public boolean isForward() {
        return _forward;
    }
}
