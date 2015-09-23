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
import org.postgresql.core.ResultCursor;
import io.sqp.backend.ResultHandler;
import io.sqp.backend.results.EndQueryResult;
import io.sqp.backend.results.QueryResult;
import io.sqp.backend.results.RowDataResult;
import io.sqp.core.ColumnMetadata;
import io.sqp.core.ErrorAction;
import io.sqp.core.ErrorType;
import io.sqp.core.TypeDescription;
import io.sqp.core.exceptions.CursorProblemException;
import io.sqp.core.exceptions.SqpException;
import io.sqp.core.types.SqpValue;

import java.io.Closeable;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Stefan Burnicki
 * This is a proxy-buffered variant. One could also create a cursor on the DBMS itself by using SQL
 */
public class Cursor implements Closeable {
    private static final int BUFFER_CLEAR_THRESHOLD = 50;
    private PGConnection _connection;
    private ResultCursor _resultCursor;
    private boolean _scrollable;
    private IStatement _originalStatement;
    private List<ColumnMetadata> _columnMetadata;
    private Field[] _fields;
    private LinkedList<byte[][]> _rowBuffer;
    private int _currentRow;

    public Cursor(PGConnection connection, IStatement originalStatement, Field[] fields, boolean scrollable) {
        _originalStatement = originalStatement;
        _connection = connection;
        _scrollable = scrollable;
        _fields = fields;
        initColumnMetadata(fields);
        _rowBuffer = new LinkedList<>();
        _currentRow = 0;
    }

    @Override
    public void close() {
        // result cursor close is non-blocking and done implicitly on another execute
        if (_resultCursor != null) {
            _resultCursor.close();
        }
        _rowBuffer.clear();
        _currentRow = 0;
    }

    public synchronized void addTuples(List tuples, ResultCursor resultCursor) {
        _resultCursor = resultCursor;
        // TODO: scrollable cursors don't clear memory in this implementation, so we should check for an OutOfMemory error
        if (!_scrollable && _currentRow > BUFFER_CLEAR_THRESHOLD) {
            _rowBuffer.subList(0, _currentRow).clear();
            _currentRow = 0;
        }
        _rowBuffer.addAll(tuples);
    }

    public List<ColumnMetadata> getColumnMetadata() {
        return _columnMetadata;
    }

    public synchronized void fetch(int position, int numRows, boolean forward, ResultHandler<QueryResult> resultHandler) {
        if ((!forward || position >= 0) && !_scrollable) {
            resultHandler.fail(new CursorProblemException("", CursorProblemException.Problem.NotScrollable));
            return;
        }
        position = position < 0 ? _currentRow : position; // -1 means current position
        numRows = numRows < 0 ? -1 : numRows; // normalize to [-1,0, ....]

        // first case: numRows == -1 and backwards means: send the whole buffer backwards from position
        if (numRows < 0 && !forward) {
            // TODO: use position. but what if position > _rowBuffer.size()?
            sendRange(_rowBuffer.size(), 0, resultHandler, true);
            return;
        }

        int bound;
        if (numRows >= 0) { // then our bound must be calculated
            int offset = forward ? numRows : -numRows;
            bound = Math.max(position + offset, 0);
        } else {
            // because we will send all the buffer. the "backwards" case is already handled above!
            bound = _rowBuffer.size();
        }

        boolean needToFetch = serverHasMoreData() && forward && (numRows < 0 || bound > _rowBuffer.size());
        // now send stuff from buffer

        sendRange(position, Math.min(bound, _rowBuffer.size()), resultHandler, !needToFetch);
        // if we still need to fetch data, fetch it and send it
        if (needToFetch) {
            int numFetch = numRows < 0 ? -1 : bound - _rowBuffer.size(); // regard what we already send from buffer
            fetchAndSend(numFetch, resultHandler);
        }
    }

    private void fetchAndSend(int numRows, ResultHandler<QueryResult> resultHandler) {
        _originalStatement.getAsyncExecutor().callAsync(() -> {
            // executed in worker thread
            try {
                FetchExecutionHook fetchHook = new FetchExecutionHook(this);
                _connection.getQueryExecutor().fetch(_resultCursor, fetchHook, numRows);
                return fetchHook.getNumNewRows();
            } catch (SQLException e) {
                throw new SqpException(ErrorType.FetchFailed,
                        "Failed to fetch more data: " + e.getMessage(), ErrorAction.Recover, e);
            }
        }, new ResultHandler<>(resultHandler::fail, numNewRows -> {
            // executed by original thread
            sendRange(_currentRow, _currentRow + numNewRows, resultHandler, true);
        }));
    }

    private void sendRange(int position, int bound, ResultHandler<QueryResult> resultHandler, boolean sendEOD) {
        int offset = Math.abs(bound - position);
        _currentRow = position;
        if (offset == 0) {
            if (sendEOD) {
                sendEndOfData(resultHandler);
            }
            return;
        }
        // TODO: think about converting data asynchronously first
        ListIterator<byte[][]> bufferIterator = _rowBuffer.listIterator(_currentRow);
        try {
            if (position < bound) {
                while (bufferIterator.hasNext() && _currentRow < bound) {
                    sendTuple(bufferIterator.next(), resultHandler);
                    _currentRow++;
                }
            } else {
                while (bufferIterator.hasPrevious() && _currentRow >= bound) {
                    sendTuple(bufferIterator.previous(), resultHandler);
                    _currentRow--;
                }
            }
            if (sendEOD) {
                sendEndOfData(resultHandler);
            }
        } catch (SqpException e) {
            resultHandler.fail(e);
        }
    }

    private void sendTuple(byte[][] tuple, ResultHandler<QueryResult> resultHandler) throws SqpException {
        int rowSize = _fields.length;
        List<SqpValue> rowData = new ArrayList<>(rowSize);
        for (int i = 0; i < rowSize; i++) {
            rowData.add(_connection.getTypeConverter().toSqpType(_fields[i], tuple[i], _columnMetadata.get(i).getType()));
        }
        resultHandler.handle(new RowDataResult(rowData));
    }

    private void sendEndOfData(ResultHandler<QueryResult> resultHandler) {
        resultHandler.handle(new EndQueryResult(_currentRow < _rowBuffer.size() || serverHasMoreData()));
    }

    private boolean serverHasMoreData() {
        return  _resultCursor != null;
    }

    private void initColumnMetadata(Field[] fields) {
        _columnMetadata = Arrays.stream(fields).map(this::createColumnMetadataFromField).collect(Collectors.toList());
    }

    private ColumnMetadata createColumnMetadataFromField(Field field) {
        String origType = TypeInfo.getTypeName(field.getOID());
        TypeConverter typeConverter = _connection.getTypeConverter();
        TypeDescription typeDescription = typeConverter.mapToTypeDescription(field);
        return new ColumnMetadata(field.getColumnLabel(), typeDescription, origType);
    }

}
