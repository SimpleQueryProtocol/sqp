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

import io.sqp.client.Cursor;
import io.sqp.core.ErrorType;
import io.sqp.core.types.SqpValue;
import io.sqp.client.exceptions.ColumnNotFoundException;
import io.sqp.client.exceptions.CursorBeforeDataException;
import io.sqp.core.ColumnMetadata;
import io.sqp.core.ErrorAction;
import io.sqp.core.exceptions.CursorProblemException;
import io.sqp.core.exceptions.SqpException;
import io.sqp.core.types.SqpAbstractLob;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @author Stefan Burnicki
 */
// TODO: clean up depending on support for positional fetch (remove code or fix it)
public class CursorImpl extends CloseableServerResource implements Cursor {
    private List<ColumnMetadata> _columns;
    private boolean _hasMoreData;
    private LinkedList<List<Object>> _data;
    private ListIterator<List<Object>> _iterator;
    private boolean _iteratorDirty;
    private List<Object> _currentRow;
    private int _currentRowNum;
    private Map<String, Integer> _columnIndexLookup;
    private boolean _scrollable;
    private boolean _insertingBefore;
    private HashMap<Integer, SqpValue> _currentRowDecoded;

    public CursorImpl(SqpConnectionImpl connection, String cursorId, List<ColumnMetadata> columns, boolean scrollable) {
        super(connection, cursorId, "cursor");
        _columns = columns;
        _data = new LinkedList<>();
        _currentRowNum = -1;
        _iteratorDirty = true;
        _scrollable = scrollable;
        _hasMoreData = true;
        _currentRowDecoded = new HashMap<>();
    }

    public void addDataRow(List<Object> row) {
        if (_insertingBefore) {
            _data.add(0, row);
            _currentRowNum++;
        } else {
            _data.add(row);
        }
        _iteratorDirty = true;
    }

    public void setHasMoreData(boolean hasMoreData) {
        _hasMoreData = hasMoreData;
    }

    @Override
    public boolean nextRow() throws SqpException {
        return scrollRow(true, true);
    }

    @Override
    public boolean previousRow() throws SqpException {
        return scrollRow(false, true);
    }

    @Override
    public SqpValue at(int i) throws SqpException {
        if (_currentRow == null) {
            throw new CursorBeforeDataException();
        }
        SqpValue sqpValue = _currentRowDecoded.get(i);
        if (sqpValue != null) {
            return sqpValue;
        }
        sqpValue = SqpValue.createFromJsonFormat(_columns.get(i).getType().getTypeCode(), _currentRow.get(i));
        if (sqpValue instanceof SqpAbstractLob) {
            SqpAbstractLob lob = (SqpAbstractLob) sqpValue;
            sqpValue = lob.createWithStream(new LobStream(getConnection(), lob.getId(), lob.getSize()));
        }
        _currentRowDecoded.put(i, sqpValue);
        return sqpValue;
    }

    @Override
    public SqpValue at(String name) throws SqpException {
        return at(getColumnIndex(name));
    }

    @Override
    public List<ColumnMetadata> getColumnMetadata() {
        return _columns;
    }

    private int getColumnIndex(String name) throws ColumnNotFoundException {
        if (_columnIndexLookup == null) {
            _columnIndexLookup = new HashMap<>(_columns.size());
            for (int i = 0; i < _columns.size(); i++) {
                _columnIndexLookup.put(_columns.get(i).getName(), i);
            }
        }

        Integer idx = _columnIndexLookup.get(name);
        if (idx != null) {
            return idx;
        }
        throw new ColumnNotFoundException(name);
    }

    private boolean scrollRow(boolean forward, boolean autofetch) throws SqpException {
        validateOpen();
        if (!forward && !_scrollable) {
            throw new CursorProblemException(getId(), CursorProblemException.Problem.NotScrollable);
        }
        if (_iteratorDirty) {
            // avoid to set a negative iteration start if cursor is uninitialized, without
            // data and starts with "previousRow()"
            if (!forward && _currentRowNum < 0) {
                _currentRowNum++;
            }
            int offset = forward ? 1 : 0;
            _iterator = _data.listIterator(_currentRowNum + offset);
            _iteratorDirty = false;
        }

        if (forward && _iterator.hasNext()) {
            _currentRowDecoded.clear();
            _currentRow = _iterator.next();
            _currentRowNum++;
            return true;
        } else if (!forward && _iterator.hasPrevious()) {
            _currentRowDecoded.clear();
            _currentRow = _iterator.previous();
            _currentRowNum--;
            return true;
        }
        // TODO: somehow remember with scrollable cursors if there is more data or not
        // TODO: enable support for fetching more results backwards. But we need to provide a position for this
        if (!autofetch || !forward || (!_hasMoreData && !_scrollable)) {
            return false;
        }

        // otherwise fetch more data
        fetch(forward);
        return scrollRow(forward, false);
    }

    private void fetch(boolean forward) throws SqpException {
        _insertingBefore = !forward;
        // TODO: compute and pass the position to fetch from!
        CompletableFuture<CursorImpl> future = getConnection().fetch(this, forward);
        Throwable error;
        try {
            future.get();
            return;
        } catch (InterruptedException e) {
            error = e;
        } catch (ExecutionException e) {
            error = e.getCause();
        }
        if (error instanceof SqpException) {
            throw (SqpException) error;
        }
        throw new SqpException(ErrorType.Unknown, "Error fetching more results: " + error.getMessage(),
                ErrorAction.Abort, error);
    }
}
