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

import io.sqp.core.TypeDescription;
import io.sqp.core.types.*;
import io.sqp.core.exceptions.NotImplementedException;
import io.sqp.core.exceptions.SqpException;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * @author Stefan Burnicki
 */
public class ResultExtractor {
    static public SqpValue extractResult(TypeDescription type, ResultSet result, int column)
            throws SQLException, SqpException {
        switch (type.getTypeCode()) {
            case Boolean:
                return new SqpBoolean(result.getBoolean(column));
            case TinyInt:
                return new SqpTinyInt(result.getByte(column));
            case SmallInt:
                return new SqpSmallInt(result.getShort(column));
            case Integer:
                return new SqpInteger(result.getInt(column));
            case BigInt:
                return new SqpBigInt(result.getLong(column));
            case Real:
                return new SqpReal(result.getFloat(column));
            case Double:
                return new SqpDouble(result.getDouble(column));
            case Decimal:
                return new SqpDecimal(result.getBigDecimal(column));
            case Char:
                return new SqpChar(result.getString(column));
            case VarChar:
                return new SqpVarChar(result.getString(column));
            case Binary:
                return new SqpBinary(result.getBytes(column));
            case VarBinary:
                return new SqpVarBinary(result.getBytes(column));
            case Time:
                LocalTime time = result.getTime(column).toLocalTime();
                return new SqpTime(time.getHour(), time.getMinute(), time.getSecond(), time.getNano());
            case Date:
                LocalDate date = result.getDate(column).toLocalDate();
                return new SqpDate(date.getYear(), date.getMonthValue(), date.getDayOfMonth());
            case Timestamp:
                LocalDateTime datetime = result.getTimestamp(column).toLocalDateTime();
                return new SqpTimestamp(datetime.getYear(), datetime.getMonthValue(), datetime.getDayOfMonth(),
                        datetime.getHour(), datetime.getMinute(), datetime.getSecond(), datetime.getNano());
            case Interval:
                // TODO: implement
                break;
            case Xml:
                // TODO: use streams?
                break;
            case Blob:
                // TODO: use streams?
                break;
            case Clob:
                // TODO: use streams?
                break;
            case Custom:
                break;
        }
        throw new NotImplementedException("Conversion to type '" + type + "' is not yet implemented.");
    }

    static public TypeDescription getStandardDataType(ResultSetMetaData metdata, int col) throws NotImplementedException, SQLException {
        int columnType = metdata.getColumnType(col);
        switch (columnType) {
            case Types.REAL:
            case Types.FLOAT:
                return SqpTypeCode.Real.asDescription();
            case Types.DOUBLE:
                return SqpTypeCode.Double.asDescription();
            case Types.VARCHAR:
            case Types.LONGNVARCHAR:
            case Types.NVARCHAR:
                return SqpTypeCode.VarChar.asDescription(metdata.getPrecision(col), 0);
            case Types.CHAR:
            case Types.NCHAR:
                return SqpTypeCode.Char.asDescription(metdata.getPrecision(col), 0);
            case Types.TINYINT:
                return SqpTypeCode.TinyInt.asDescription();
            case Types.SMALLINT:
                return SqpTypeCode.SmallInt.asDescription();
            case Types.INTEGER:
                return SqpTypeCode.Integer.asDescription();
            case Types.BIGINT:
                return SqpTypeCode.BigInt.asDescription();
            case Types.BIT:
            case Types.BINARY:
                return SqpTypeCode.Binary.asDescription(metdata.getPrecision(col), 0);
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return SqpTypeCode.VarBinary.asDescription(metdata.getPrecision(col), 0);
            case Types.DATE:
                return SqpTypeCode.Date.asDescription();
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                return SqpTypeCode.Time.asDescription(metdata.getPrecision(col), 0);
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return SqpTypeCode.Timestamp.asDescription(metdata.getPrecision(col), 0);
            case Types.BLOB:
                return SqpTypeCode.Blob.asDescription();
            case Types.CLOB:
            case Types.NCLOB:
                return SqpTypeCode.Clob.asDescription();
            case Types.NUMERIC:
            case Types.DECIMAL:
                return SqpTypeCode.Decimal.asDescription(metdata.getPrecision(col), metdata.getScale(col));
            case Types.SQLXML:
                return SqpTypeCode.Xml.asDescription();
            case Types.ARRAY:
            case Types.OTHER:
            case Types.DISTINCT:
            case Types.JAVA_OBJECT:
            default:
                // missing:  datalink, null, ref, rowid
                throw new NotImplementedException("The data type '" + columnType + "' is not yet supported.");
        }
    }
}
