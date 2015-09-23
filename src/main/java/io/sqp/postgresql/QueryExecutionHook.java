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

package io.sqp.postgresql;

import org.postgresql.core.Field;
import org.postgresql.core.Query;
import org.postgresql.core.ResultCursor;

import java.util.List;

/**
 * @author Stefan Burnicki
 */
public class QueryExecutionHook extends ExecutionHook {
    private int _updateCount;
    private Cursor _receivedCursor;
    private IStatement _originalStatement;
    private PGConnection _connection;
    private boolean _scrollableCursor;

    public QueryExecutionHook(PGConnection connection, IStatement originalStatement, boolean scrollable) {
        _receivedCursor = null;
        _updateCount = 0;
        _originalStatement = originalStatement;
        _connection = connection;
        _scrollableCursor = scrollable;
    }

    public void handleResultRows(Query fromQuery, Field[] fields, List tuples, ResultCursor cursor) {
        if (_receivedCursor == null) {
            _receivedCursor = new Cursor(_connection, _originalStatement, fields, _scrollableCursor);
        }
        if (tuples.size() > 0) {
            _receivedCursor.addTuples(tuples, cursor);
        }
    }

    public void handleCommandStatus(String status, int updateCount, long insertOID) {
        _updateCount += updateCount;
    }

    public Cursor getReceivedCursor() {
        return _receivedCursor;
    }

    public int getUpdateCount() {
        return _updateCount;
    }
}
