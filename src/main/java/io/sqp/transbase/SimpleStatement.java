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
import io.sqp.backend.exceptions.PrepareFailedException;
import io.sqp.core.exceptions.SqpException;
import transbase.tbx.TBXCursor;
import transbase.tbx.TBXQuery;

import java.sql.SQLException;

/**
 * @author Stefan Burnicki
 */
public class SimpleStatement implements IStatement {
    private Connection _connection;
    private TBXQuery _query;
    private Cursor _associatedCursor;
    private int _affectedRows;

    public SimpleStatement(Connection connection, TBNativeSQLFactory nativeSQLFactory, String sql) throws SqpException {
        _connection = connection;
        String nativeSQL = nativeSQLFactory.getNativeSQL(sql);
        try {
            _query = new TBXQuery(nativeSQL);
        } catch (SQLException e) {
            throw new PrepareFailedException(e.getMessage(), e);
        }
    }

    public boolean execute(Transaction transaction, boolean scrollable) throws SqpException {
        transaction.tryAutoCommit(Transaction.CommitType.SELECT_QUERY);
        int cursorMode = scrollable ? (TBXCursor.CURSOR_SCROLLABLE | TBXCursor.CURSOR_INSENSITIVE)
                                    : TBXCursor.CURSOR_FORWARD;
        _associatedCursor = null;
        try {
            // I don't know why that's necessary, but
            transaction.tryAutoCommit(Transaction.CommitType.UPDATE_QUERY);
            Object result = _query.run(transaction.getTBXTransaction(), _connection.getTBXConnection(),
                                       cursorMode, -1);
            if (result instanceof TBXCursor) {
                TBXCursor tbxCursor = (TBXCursor) result;
                _associatedCursor = new Cursor(this, tbxCursor, scrollable, _connection.getResultConverter());
            } else {
                _affectedRows = result instanceof Integer ? (Integer) result : 0;
            }

            return _associatedCursor != null;
        } catch (SQLException error) {
            throw new ExecutionFailedException(error);
        } finally {
            // I don't know why that's necessary, but
            transaction.tryAutoCommit(Transaction.CommitType.UPDATE_QUERY);
        }
    }

    public Cursor getAssociatedCursor() {
        return _associatedCursor;
    }

    public int getAffectedRows() {
        return _affectedRows;
    }

    public void releaseCursor(Cursor cursor) throws SqpException {
        if (_associatedCursor != cursor) {
            return; // we don't care about old associations
        }
        _associatedCursor = null;
    }
}
