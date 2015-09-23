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

package io.sqp.backend.jdbc;

import io.sqp.core.exceptions.InvalidOperationException;
import io.sqp.core.types.SqpValue;
import io.sqp.core.exceptions.NotImplementedException;
import io.sqp.core.exceptions.SqpException;
import io.sqp.core.exceptions.UnsupportedTypeException;
import io.sqp.core.types.SqpTypeCode;

import java.sql.*;
import java.util.Arrays;
import java.util.List;

/**
 * @author Stefan Burnicki
 */
public class SmartStatement {
    final private String[] _updateCommands = {"UPDATE", "INSERT", "DELETE"}; // TODO: check/add more

    private String _sqlStatement;
    private Connection _jdbcConnection;
    private PreparedStatement _statement;
    private boolean _inUse;
    private boolean _autoClose;
    private boolean _isUpdate;
    private boolean _isClosed;
    private boolean _isScrollable;
    private boolean _useBatchExecute;

    public SmartStatement(String sql, Connection jdbcConnection, boolean isScrollable) throws SQLException {
        _autoClose = false;
        _inUse = false;
        _jdbcConnection = jdbcConnection;
        _sqlStatement = sql;
        _isUpdate = determineIfUpdate();
        _useBatchExecute = false;
        open(isScrollable);
    }

    public SmartStatement(String sql, Connection jdbcConnection) throws SQLException {
        this(sql, jdbcConnection, false);
    }

    public void close() throws SQLException {
        if (_inUse) {
            _autoClose = true;
        } else {
            doClose();
        }
    }

    public int executeUpdate() throws SQLException {
        // TODO: check for fails and send warnings!
        if (!_useBatchExecute) {
            return _statement.executeUpdate();
        }
        return Arrays.stream(_statement.executeBatch()).filter(i -> i > 0).sum();
    }

    public SmartResultSet executeQuery(boolean scrollable) throws SQLException, NotImplementedException {
        if (_inUse) {
            // clone this statement, as it can only be used once
            SmartStatement clone = new SmartStatement(_sqlStatement, _jdbcConnection, scrollable);
            clone._autoClose = true;
            return clone.executeQuery(scrollable);
        }
        // TODO: well this might not be fast, but how should be guessed it correctly?
        if (!_isScrollable && scrollable) {
            _statement.close();
            open(true);
        }
        ResultSet result = _statement.executeQuery();
        _inUse = true;
        return new SmartResultSet(result, isScrollable(), this);
    }

    public void releaseResult() throws SQLException {
        _inUse = false;
        if (_autoClose) {
            doClose();
        }
    }

    public void bindParameterBatch(List<List<SqpValue>> parameters) throws SQLException, SqpException {
        // make sure it's clea first
        _statement.clearParameters();
        _statement.clearBatch();
        _useBatchExecute = parameters.size() > 1;
        if (_useBatchExecute && !isUpdate()) {
            // TODO: support batch selects?
            throw new InvalidOperationException("Batch operations are only supported for updates");
        }
        for (List<SqpValue> parameterList : parameters) {
            bindParameterList(parameterList);
            if (_useBatchExecute) {
                _statement.addBatch();
            }
        }
    }

    public boolean isUpdate() {
        return _isUpdate;
    }

    public boolean isScrollable() {
        return _isScrollable;
    }

    private void open(boolean isScrollable) throws SQLException {
        _isScrollable = isScrollable;
        int scrollFlag = _isScrollable ? ResultSet.TYPE_SCROLL_SENSITIVE : ResultSet.TYPE_FORWARD_ONLY;
        _statement = _jdbcConnection.prepareStatement(_sqlStatement, scrollFlag, ResultSet.CONCUR_READ_ONLY);
    }

    private void doClose() throws SQLException {
        if (_isClosed) {
            return;
        }
        _isClosed = true;
        _statement.close();
    }

    private void bindParameterList(List<SqpValue> values) throws SQLException, SqpException {
        for (int i = 0; i < values.size(); i++) {
            bindParameter(i, values.get(i));
        }
    }

    private void bindParameter(int col, SqpValue value) throws SqpException, SQLException {
        SqpTypeCode type = value.getType();
        col = col + 1; // jdbc starts numbering by 1

        if (value.isNull()) {
            _statement.setNull(col, TypeInfo.getSQLFromSqpType(value.getType()));
            return;
        }

        switch (type) {
            case Boolean:
                _statement.setBoolean(col, value.asBoolean());
                break;
            case TinyInt:
                _statement.setByte(col, value.asByte());
                break;
            case SmallInt:
            case Integer:
                _statement.setInt(col, value.asInt());
                break;
            case BigInt:
                _statement.setLong(col, value.asLong());
                break;
            case Real:
                _statement.setFloat(col, value.asFloat());
                break;
            case Double:
                _statement.setDouble(col, value.asDouble());
                break;
            case Decimal:
                _statement.setBigDecimal(col, value.asBigDecimal());
                break;
            case Char:
            case VarChar:
                _statement.setString(col, value.asString());
                break;
            case Binary:
            case VarBinary:
                _statement.setBytes(col, value.asBytes());
                break;
            case Time:
                // TODO: do something about the timezone?
                _statement.setTime(col, Time.valueOf(value.asOffsetTime().toLocalTime()));
                break;
            case Date:
                _statement.setDate(col, Date.valueOf(value.asLocalDate()));
                break;
            case Timestamp:
                _statement.setTimestamp(col, Timestamp.valueOf(value.asOffsetDateTime().toLocalDateTime()));
                break;
            case Interval:
            case Xml:
            case Blob:
            case Clob:
            case Custom:
                // TODO: support them
                throw new UnsupportedTypeException("The type '" + type + "' is currently not supported.");
        }
    }

    private boolean determineIfUpdate() {
        // TODO: this method is bad since it doesn't regard escaped strings. anyway, this will do for tests
        String upperSQL = _sqlStatement.toUpperCase();
        return Arrays.stream(_updateCommands).anyMatch(s -> upperSQL.contains(s));
    }
}
