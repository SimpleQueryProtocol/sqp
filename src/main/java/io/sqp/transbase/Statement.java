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

package io.sqp.transbase;

import io.sqp.backend.exceptions.ExecutionFailedException;
import io.sqp.backend.exceptions.InternalErrorException;
import io.sqp.backend.exceptions.PrepareFailedException;
import io.sqp.core.ErrorAction;
import io.sqp.core.ErrorType;
import io.sqp.core.exceptions.InvalidOperationException;
import io.sqp.core.exceptions.SqpException;
import io.sqp.core.types.SqpValue;
import transbase.tbx.TBXCursor;
import transbase.tbx.TBXStoredQuery;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author Stefan Burnicki
 */
public class Statement implements IStatement {
    private Connection _connection;
    private TBXStoredQuery _storedQuery;
    private Cursor _associatedCursor;
    private boolean _autoClose;
    private boolean _useBatchUpdate;

    public Statement(Connection connection, TBNativeSQLFactory nativeSQLFactory, String sql) throws SqpException {
        _connection = connection;
        _useBatchUpdate = false;
        String nativeSQL = nativeSQLFactory.getNativeSQL(sql);
        try {
            _storedQuery = new TBXStoredQuery(nativeSQL, _connection.getTBXConnection());
        } catch (SQLException e) {
            throw new PrepareFailedException(e.getMessage(), e);
        }
    }

    public boolean isSelectQuery() {
        return _storedQuery.isQuerySelectClass();
    }

    public void close() throws SqpException {
        if (_storedQuery == null) {
            return;
        }
        if (_associatedCursor != null) {
            // if there is still a cursor associated, auto-close this statement when the cursor is closed
            _autoClose = true;
            return;
        }
        try {
            _storedQuery.close();
        } catch (SQLException e) {
            throw new SqpException(ErrorType.CloseFailed, "Could not close stored query.", ErrorAction.Recover, e);
        }
    }

    public Cursor executeSelect(Transaction transaction, boolean scrollable) throws SqpException {
        if (!isSelectQuery()) {
            throw new InternalErrorException("The backend internally invoked an update execution, but it was a select");
        }
        transaction.tryAutoCommit(Transaction.CommitType.SELECT_QUERY);
        int cursorMode = scrollable ? (TBXCursor.CURSOR_SCROLLABLE | TBXCursor.CURSOR_INSENSITIVE)
                                    : TBXCursor.CURSOR_FORWARD;
        try {
            TBXCursor tbxCursor = _storedQuery.open(transaction.getTBXTransaction(), cursorMode, -1);
            _associatedCursor = new Cursor(this, tbxCursor, scrollable, _connection.getResultConverter());
            return _associatedCursor;
        } catch (SQLException error) {
            // I don't know why that's necessary, but
            transaction.tryAutoCommit(Transaction.CommitType.UPDATE_QUERY);
            throw new ExecutionFailedException(error);
        }
    }

    public int executeUpdate(Transaction transaction) throws SqpException {
        if (isSelectQuery()) {
            throw new InternalErrorException("The backend internally invoked an update execution, but it was a select");
        }
        try {
            int updateCount;
            if (_useBatchUpdate) {
                updateCount = Arrays.stream(_storedQuery.runBatch(transaction.getTBXTransaction())).filter(i -> i > 0).sum();
            } else {
                updateCount = _storedQuery.run(transaction.getTBXTransaction());
            }
            transaction.tryAutoCommit(Transaction.CommitType.UPDATE_QUERY);
            return updateCount;
        } catch (SQLException e) {
            throw new ExecutionFailedException(e);
        }
    }

    public void releaseCursor(Cursor cursor) throws SqpException {
        if (_associatedCursor != cursor) {
            return; // we don't care about old associations
        }
        _associatedCursor = null;
        if (_autoClose) {
            close();
        }
    }

    public void bind(List<List<SqpValue>> parameters) throws SqpException {
        _useBatchUpdate = parameters.size() > 1;
        if (_useBatchUpdate && isSelectQuery()) {
            throw new InvalidOperationException("Cannot use batch execution for select statements.");
        }
        ParameterBinder binder = new ParameterBinder(_storedQuery, _connection.getCharEncodingFactory());
        binder.bindBatch(parameters);
    }
}
