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
import io.sqp.backend.exceptions.FetchFailedException;
import io.sqp.core.ColumnMetadata;
import io.sqp.core.ErrorAction;
import io.sqp.core.ErrorType;
import io.sqp.core.exceptions.CursorProblemException;
import io.sqp.core.exceptions.SqpException;
import io.sqp.core.types.SqpValue;
import transbase.tbx.TBXCursor;
import transbase.tbx.types.helpers.QueryDescriptor;
import transbase.tbx.types.helpers.TBObject;
import transbase.tbx.types.helpers.TSpec;
import transbase.tbx.types.info.TBTypeInfo;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Stefan Burnicki
 */
public class Cursor {
    private static final int DEFAULT_TIMEOUT = 2000;
    private static final int DEFAULT_NUM_ROWS = 10; // TODO: make this intelligent by checking what the cursor has buffered
    private List<ColumnMetadata> _columnMetadata;
    private TBXCursor _tbxCursor;
    private boolean _scrollable;
    private boolean _hasMore;
    private IStatement _origin;
    private ResultConverter _resultConverter;

    public Cursor(IStatement origin, TBXCursor tbxCursor, boolean scrollable, ResultConverter resultConverter) throws SqpException {
        _tbxCursor = tbxCursor;
        _scrollable = scrollable;
        _origin = origin;
        _resultConverter = resultConverter;
        try {
            initColumnMetadata();
        } catch (SQLException e) {
            // TODO: this type of error is very unspecific
            throw new ExecutionFailedException(e);
        }
    }

    public void close() throws SqpException {
        SqpException error = null;
        try {
            _resultConverter.getLobManager().closeAll(this);
        } catch (IOException e) {
            error = new SqpException(ErrorType.CloseFailed, "Close of lobs failed!", ErrorAction.Recover, e);
        }
        try {
            _tbxCursor.close(true, false);
        } catch (SQLException e) {
            error = new SqpException(ErrorType.CloseFailed, "Close of cursor failed!", ErrorAction.Recover, e);
        }
        _origin.releaseCursor(this);
        if (error != null) {
            throw error;
        }
    }

    public List<ColumnMetadata> getColumnMetadata() {
        return _columnMetadata;
    }

    public List<List<SqpValue>> fetch(int position, int numRows, boolean forward) throws SqpException {
        if (position >= 0 && !positionCursor(position)) {
            return Collections.emptyList();
        }
        setFetchDirection(forward);
        // TODO: if we had more insight into TBXCursor, we would exactly get the amount of tuples we got from the
        // DB itself
        numRows = numRows < 0 ? DEFAULT_NUM_ROWS : numRows;
        ArrayList<List<SqpValue>> rows = new ArrayList<>(numRows);
        // _hasMore is used slightly wrong: if we catch as many rows as there are, _hasMore will be true, although
        // there are no more. But since we can't look into the future, that the way to go
        for (int i = 0; i < numRows; i++) {
            _hasMore = nextRow(forward);
            if (!_hasMore) {
                break;
            }
            rows.add(getRow());
        }
        return rows;
    }

    public boolean hasMore() {
        return _hasMore;
    }

    public boolean isScrollable() {
        return _scrollable;
    }

    private boolean positionCursor(int position) throws CursorProblemException {
        try {
            return _tbxCursor.absolute(position, DEFAULT_TIMEOUT);
        } catch (SQLException e) {
            throw new CursorProblemException("", CursorProblemException.Problem.NotPositionable, e);
        }
    }

    private void setFetchDirection(boolean forward) throws CursorProblemException {
        try {
            _tbxCursor.setFetchDirection(forward ? TBXCursor.FETCH_FORWARD : TBXCursor.FETCH_REVERSE);
        } catch (SQLException e) {
            throw new CursorProblemException("", CursorProblemException.Problem.FetchDirectionFailed, e);
        }
    }

    private boolean nextRow(boolean forward) throws FetchFailedException {
        try {
            if (forward) {
                return _tbxCursor.next(DEFAULT_TIMEOUT);
            } else {
                return _tbxCursor.previous(DEFAULT_TIMEOUT);
            }
        } catch (SQLException e) {
            throw new FetchFailedException("Failed to get the next/previous row from cursor.", e);
        }
    }

    private List<SqpValue> getRow() throws SqpException {
        int rowSize = _columnMetadata.size();
        ArrayList<SqpValue> row = new ArrayList<>(rowSize);
        for (int i = 0; i < rowSize; i++) {
            row.add(_resultConverter.valueToSqpValue(getField(i), _columnMetadata.get(i).getType(), this));
        }
        return row;
    }

    private TBObject getField(int i) throws FetchFailedException {
        try {
            return _tbxCursor.getObject(i + 1);
        } catch (SQLException e) {
            throw new FetchFailedException("Failed to get the field value for column " + (i+1), e);
        }
    }

    private void initColumnMetadata() throws SQLException, SqpException {
        QueryDescriptor qd = _tbxCursor.getQueryDescriptor();
        int numCols = qd.getFieldNo();
        _columnMetadata = new ArrayList<>(qd.getFieldNo());
        for (int i = 0; i < numCols; i++) {
            _columnMetadata.add(createColumnMetadataForField(qd, i));
        }
    }

    private ColumnMetadata createColumnMetadataForField(QueryDescriptor qd, int idx) throws SQLException, SqpException {
        // another class that starts indexing with 1. yay!
        TSpec typeSpec = qd.getFieldTspec(idx + 1);
        TBTypeInfo tbTypeInfo = TBTypeInfo.createTypeInfo(typeSpec);
        String nativeTypeName = TBTypeRepository.getTbTypeName(tbTypeInfo);
        String name = qd.getColumnName(idx + 1);
        return new ColumnMetadata(name, _resultConverter.getSqpTypeDescription(typeSpec, tbTypeInfo), nativeTypeName);
    }
}
