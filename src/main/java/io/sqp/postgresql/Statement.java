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

import org.postgresql.core.ParameterList;
import org.postgresql.core.Query;
import org.postgresql.core.QueryExecutor;
import io.sqp.backend.AsyncExecutor;
import io.sqp.backend.ResultHandler;
import io.sqp.backend.SuccessHandler;
import io.sqp.backend.exceptions.ExecutionFailedException;
import io.sqp.backend.exceptions.PrepareFailedException;
import io.sqp.backend.results.CursorDescriptionResult;
import io.sqp.backend.results.QueryResult;
import io.sqp.backend.results.UpdateQueryResult;
import io.sqp.core.exceptions.SqpException;
import io.sqp.core.types.SqpValue;

import java.io.Closeable;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/**
 * @author Stefan Burnicki
 */
public class Statement implements Closeable, IStatement {
    private PGConnection _connection;
    private AsyncExecutor _asyncExecutor;
    private String _sql;
    private Query _preparedQuery;
    private ParameterList[] _parameterLists;

    private Statement(PGConnection connection, String sql) {
        _connection = connection;
        _sql = sql;
        _asyncExecutor = connection.getAsyncExecutor();
        _parameterLists = new ParameterList[0];
    }

    private void initialize(SuccessHandler successHandler) {
        _asyncExecutor.callAsync(() -> {
            _preparedQuery = getQueryExecutor().createParameterizedQuery(_sql);
            return null;
        }, new ResultHandler<>(
                error -> successHandler.fail(new PrepareFailedException(error.getMessage(), error)),
                result -> successHandler.succeed()
        ));
    }

    public static Statement create(PGConnection connection, String sql, SuccessHandler successHandler) {
        Statement statement = new Statement(connection, sql);
        statement.initialize(successHandler);
        return statement;
    }

    public void bind(List<List<SqpValue>> parameters) throws SqpException {
        _parameterLists = new ParameterList[parameters.size()];
        for (int i = 0; i < parameters.size(); i++) {
            _parameterLists[i] = createParameterList(parameters.get(i));
        }
    }

    public void execute(boolean autocommit, String cursorId, boolean scrollable, ResultHandler<QueryResult> resultHandler) {
        // TODO: modify flags to include cursor type (forward only), and describe
        final int flags = getFlags(autocommit);
        _asyncExecutor.callAsync(() -> {
            // runs in different thread
            try {
                QueryExecutionHook executionHook = new QueryExecutionHook(_connection, this, scrollable);
                ParameterList[] parameterLists = getParameterListsToExecute();
                Query[] queries = new Query[parameterLists.length];
                Collections.nCopies(parameterLists.length, _preparedQuery).toArray(queries);
                getQueryExecutor().execute(queries, parameterLists, executionHook, 0, 100, flags);
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
            } else {
                resultHandler.handle(new UpdateQueryResult(queryResultHandler.getUpdateCount()));
            }
        }));
    }

    private ParameterList[] getParameterListsToExecute() {
        if (_parameterLists.length < 1) {
            return new ParameterList[] { _preparedQuery.createParameterList() };
        }
        return _parameterLists;
    }

    private ParameterList createParameterList(List<SqpValue> parameters) throws SqpException {
        ParameterList parameterList = _preparedQuery.createParameterList();
        ParameterBinder binder = new ParameterBinder(_connection, parameterList);
        for (int i = 0; i < parameters.size(); i++) {
            binder.bindParameter(i, parameters.get(i));
        }
        return parameterList;
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

    @Override
    public void close() {
        // This should be a non-blocking operation. Actual close will happen when another query is executed
        if (_preparedQuery != null) {
            _preparedQuery.close();
        }
    }

    private QueryExecutor getQueryExecutor() {
        return _connection.getQueryExecutor();
    }

    public AsyncExecutor getAsyncExecutor() {
        return _asyncExecutor;
    }
}
