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

import org.postgresql.core.Query;
import org.postgresql.core.QueryExecutor;
import io.sqp.backend.AsyncExecutor;
import io.sqp.backend.ResultHandler;
import io.sqp.backend.exceptions.ExecutionFailedException;
import io.sqp.backend.results.CursorDescriptionResult;
import io.sqp.backend.results.QueryResult;
import io.sqp.backend.results.UpdateQueryResult;

import java.sql.SQLException;

/**
 * @author Stefan Burnicki
 */
public class SimpleStatement implements IStatement {
    private PGConnection _connection;
    private AsyncExecutor _asyncExecutor;
    private String _sql;

    public SimpleStatement(PGConnection connection, String sql) {
        _connection = connection;
        _sql = sql;
        _asyncExecutor = connection.getAsyncExecutor();
    }


    public void execute(boolean autocommit, String cursorId, boolean scrollable, int maxRows, ResultHandler<QueryResult> resultHandler) {
        // TODO: modify flags to include cursor type (forward only), and describe
        Query simpleQuery = _connection.getQueryExecutor().createSimpleQuery(_sql);
        int flags = getFlags(autocommit) | QueryExecutor.QUERY_ONESHOT;
        _asyncExecutor.callAsync(() -> {
            // runs in different thread
            try {
                QueryExecutionHook executionHook = new QueryExecutionHook(_connection, this, scrollable);
                getQueryExecutor().execute(simpleQuery, null, executionHook, 0, 0, flags);
                return executionHook;
            } catch (SQLException e) {
                throw new ExecutionFailedException(e);
            }
        }, new ResultHandler<>(resultHandler::fail, queryResultHandler -> {
            // runs in original thread
            Cursor cursor = queryResultHandler.getReceivedCursor();
            if (cursor != null) {
                _connection.registerCursor(cursorId, cursor);
                resultHandler.handle(new CursorDescriptionResult(cursorId, scrollable, cursor.getColumnMetadata()));
                cursor.fetch(-1, maxRows, true, resultHandler);
            } else {
                resultHandler.handle(new UpdateQueryResult(queryResultHandler.getUpdateCount()));
            }
        }));
    }

    private int getFlags(boolean autocommit) {
        // server side cursors only work if we are not in autocommit mode. Otherwise we will just get all
        // data. We emulate the cursor behavior in the backend
        int flags;
        if (autocommit) {
            flags = QueryExecutor.QUERY_SUPPRESS_BEGIN;
        } else {
            flags = QueryExecutor.QUERY_FORWARD_CURSOR;
        }
        return flags;
    }

    private QueryExecutor getQueryExecutor() {
        return _connection.getQueryExecutor();
    }

    public AsyncExecutor getAsyncExecutor() {
        return _asyncExecutor;
    }
}
