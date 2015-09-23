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
public class SimpleQueryMessage extends SqpMessage {
    private String _query;
    private int _maxFetch = -1;
    private String _cursorId = Defaults.DefaultCursorID;
    private boolean _scrollable = false;

    // necessary for decoding
    public SimpleQueryMessage() {}

    public SimpleQueryMessage(String query, boolean scrollable, String cursorId, int maxFetch) {
        _query = query;
        _maxFetch = maxFetch;
        _scrollable = scrollable;
        _cursorId = cursorId;
    }

    public void setQuery(String query) {
        _query = query;
    }

    public void setMaxFetch(int maxFetch) {
        _maxFetch = maxFetch;
    }

    public void setCursorId(String cursorId) {
        _cursorId = cursorId;
    }

    public void setScrollable(boolean scrollable) {
        _scrollable = scrollable;
    }

    public String getQuery() {
        return _query;
    }

    public int getMaxFetch() { return _maxFetch; }

    public String getCursorId() {
        return _cursorId;
    }

    public boolean isScrollable() {
        return _scrollable;
    }
}
