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

import io.sqp.core.types.*;
import org.postgresql.core.Oid;
import org.postgresql.core.ParameterList;
import io.sqp.backend.exceptions.ParameterBindException;
import io.sqp.core.exceptions.TypeConversionException;
import io.sqp.core.exceptions.UnsupportedTypeException;
import io.sqp.core.exceptions.SqpException;
import io.sqp.core.util.TypeUtil;

import java.sql.SQLException;
import java.util.List;

/**
 * @author Stefan Burnicki
 */
public class ParameterBinder {
    private ParameterList _parameterList;
    private int _bindStringOid;
    private boolean _serverCanParseOffset;
    private boolean _serverSupportsOffsetSeconds;

    public ParameterBinder(PGConnection connection, ParameterList parameterList) {
        _parameterList = parameterList;
        _bindStringOid = connection.haveMinimumServerVersion("8.0") ? Oid.VARCHAR : Oid.UNSPECIFIED;
        _serverCanParseOffset = connection.haveMinimumServerVersion("7.4");
        _serverSupportsOffsetSeconds = connection.haveMinimumServerVersion("8.2");
    }

    public void bindParameter(int idx, SqpValue parameter) throws SqpException {
        try {
            bindParameterUnsafe(idx, parameter);
        } catch (SQLException e) {
            throw new ParameterBindException(parameter.getType(), e);
        }
    }

    private void bindParameterUnsafe(int idx, SqpValue value) throws SqpException, SQLException {
        // TODO: support for binary transport
        SqpTypeCode type = value.getType();
        // care about null values first
        if (value.isNull()) {
            // if Oid.UNSPECIFIED turns out not to work, introduce a sqp -> pg lookup in TypeInfo
            _parameterList.setNull(idx + 1, Oid.UNSPECIFIED);
            return;
        }

        switch (type) {
            case Boolean:
                bindLiteral(idx, value.asBoolean() ? "1" : "0", Oid.BOOL);
                break;

            case TinyInt:
                bindLiteral(idx, value.asString(), Oid.CHAR);
                break;

            case SmallInt:
                bindLiteral(idx, value.asString(), Oid.INT2);
                break;

            case Integer:
                bindLiteral(idx, value.asString(), Oid.INT4);
                break;

            case BigInt:
                bindLiteral(idx, value.asString(), Oid.INT8);
                break;

            case Real:
                bindLiteral(idx, value.asString(), Oid.FLOAT4);
                break;

            case Double:
                bindLiteral(idx, value.asString(), Oid.FLOAT8);
                break;

            case Decimal:
                bindLiteral(idx, value.asString(), Oid.NUMERIC);
                break;

            case Char:
                bindLiteral(idx, value.asString(), Oid.BPCHAR);
                break;

            case VarChar:
                bindLiteral(idx, value.asString(), _bindStringOid);
                break;

            case Binary:
            case VarBinary:
                bindBytes(idx, value.asBytes());
                break;

            case Time:
                bindTime(idx,(SqpTime) value);
                break;

            case Date:
                bindDate(idx, (SqpDate) value);
                break;

            case Timestamp:
                bindTimestamp(idx, (SqpTimestamp) value);
                break;

            case Custom:
                bindCustom(idx, (SqpCustom) value);
                break;

            case Blob:
                bindBlob(idx, (SqpBlob) value);
                break;

            case Clob:
                bindLiteral(idx, value.asString(), Oid.TEXT);
                break;

            case Xml:
                bindLiteral(idx, value.asString(), Oid.XML);
                break;

            case Interval:
                bindInterval(idx, (SqpInterval) value);
                break;

            default:
                throw new UnsupportedTypeException("The type '" + type + "' is currently not supported.");
        }
    }

    private void bindInterval(int idx, SqpInterval value) throws UnsupportedTypeException {
        throw new UnsupportedTypeException("Binding intervals is not yet implemented.");
    }

    private void bindBlob(int idx, SqpBlob value) throws UnsupportedTypeException {
        throw new UnsupportedTypeException("Binding blobs is not yet implemented.");
        // TODO: support LOBs. Either by just using the normal data types "bytea" and "text" for BLOBs,
        // respectively CLOBs, or by using the explicit LargeObject API.
        // For the second approach it would be needed to reimplement the LargeObjectManager of the JDBC driver
        // or to implement the BaseConnection of the JDBC driver so both Fastpath and LargeObjectManager can be used.
        // The second approach is probably easier. Then the following code can be used:
        /*
        LargeObjectManager lom = connection.getLargeObjectAPI();
        long oid = lom.createLO();
        LargeObject lob = lom.open(oid);
        OutputStream outputStream = lob.getOutputStream();
        byte[] buf = new byte[4096];
        try
        {
            long remaining;
            if (length > 0)
            {
                remaining = length;
            }
            else
            {
                remaining = Long.MAX_VALUE;
            }
            int numRead = inputStream.read(buf, 0, (length > 0 && remaining < buf.length ? (int)remaining : buf.length));
            while (numRead != -1 && remaining > 0)
            {
                remaining -= numRead;
                outputStream.write(buf, 0, numRead);
                numRead = inputStream.read(buf, 0, (length > 0 && remaining < buf.length ? (int)remaining : buf.length));
            }
        }
        */
    }

    public void bindCustom(int idx, SqpCustom custom) throws SqpException, SQLException {
        String customType = custom.getCustomTypeName();
        int oid = TypeInfo.getOidFromTypeName(customType);
        if (oid < 0) {
            throw new UnsupportedTypeException("The custom type '" + customType + "' is unknown");
        }
        switch (oid) {
            case Oid.POINT:
                bindPointFromCustom(idx, custom.getValue());
                return;
        }
        throw new UnsupportedTypeException("The custom type '" + customType + "' is currently not supported");
    }

    private void bindPointFromCustom(int idx, Object value) throws TypeConversionException, SQLException {
        try {
            List list = TypeUtil.checkAndConvert(value, List.class, "The passed value");
            if (list.size() != 2) {
                throw new IllegalArgumentException("The value needs to contain exactly 2 doubles");
            }
            Double x = TypeUtil.checkAndConvert(list.get(0), Double.class, "The first value");
            Double y = TypeUtil.checkAndConvert(list.get(1), Double.class, "The second value");
            bindLiteral(idx, "(" + x.toString() + "," + y.toString() + ")", Oid.POINT);
        } catch (IllegalArgumentException e) {
            throw new TypeConversionException("The custom value is not valid point: " + e.getMessage(), e);
        }

    }

    public void bindTimestamp(int idx, SqpTimestamp timestamp) throws SQLException {
        // org.postgresql.jdbc2.AbstractJdbc2Statement.setTimestamp tells me to use OID.UNSPECIFIED, because of
        // TimeZone quirks with set/unset timezones. I will just follow the advise instead of learning it the hard way
        // TODO: what about infinity and -infinity?
        SqpDate date = timestamp.getDate();
        SqpTime time = timestamp.getTime();
        StringBuilder sb = new StringBuilder(29 + (time.hasOffset() ? 10 :0));

        appendDate(sb, date);
        sb.append(' ');

        appendTime(sb, time);
        // in contrast to pure "Time", a time offset for timestamps seems to be supported by all server versions
        appendTimeOffset(sb, time);
        appendEra(sb, date);

        bindLiteral(idx, sb.toString(), Oid.UNSPECIFIED);
    }

    public void bindTime(int idx, SqpTime time) throws SQLException {
        // org.postgresql.jdbc2.AbstractJdbc2Statement.setTime tells me to use OID.UNSPECIFIED. I guess this has
        // to do with time zone quirks as mentioned in bindDate or bindTimestamp

        boolean useOffset = _serverCanParseOffset && time.hasOffset();
        // at least "hh:mm:ss.mmmmmm", optional TZ " +hh:mm:ss"
        StringBuilder sb = new StringBuilder(15 + (useOffset ? 10 : 0));

        appendTime(sb, time);
        if (useOffset) {
            appendTimeOffset(sb, time);
        }

        bindLiteral(idx, sb.toString(), Oid.UNSPECIFIED);
    }

    public void bindDate(int idx, SqpDate date) throws SQLException {
        // A hint in org.postgresql.jdbc2.AbstractJdbc2Statement.setDate tells me to use OID.UNSPECIFIED, or
        // it might get some timezone quirks with the server's local time zone. We certainly don't want this
        // TODO: what about infinity and -infinity?
        StringBuilder sb = new StringBuilder(13); // most commonly "xxx-xx-xx BC" (or without BC, but that's okay)
        appendDate(sb, date);
        appendEra(sb, date);

        bindLiteral(idx, sb.toString(), Oid.UNSPECIFIED);
    }

    public void bindBytes(int idx, byte[] bytes) throws SQLException, UnsupportedTypeException {
        // not that the internal parameter list takes a 1-based index
        _parameterList.setBytea(idx + 1, bytes, 0, bytes.length);
    }

    private void bindLiteral(int idx, String literal, int oid) throws SQLException {
        // ParameterList indexing starts with 1 here. Probably it's because its from the JDBC driver and some
        // JDBC spec genius wanted to fuck around with conventions
        _parameterList.setLiteralParameter(idx + 1, literal, oid);
    }

    private static void appendDate(StringBuilder sb, SqpDate date) {
        int year = Math.abs(date.getYear()); // era ("BC") gets appended separately if negative
        // use at least 4 year digits
        if (year < 10) sb.append('0');
        if (year < 100) sb.append('0');
        if (year < 1000) sb.append('0');
        sb.append(year);

        sb.append('-');
        int month = date.getMonth();
        if (month < 10) sb.append('0');
        sb.append(month);

        sb.append('-');
        int day = date.getDay();
        if (day < 10) sb.append('0');
        sb.append(day);
    }

    private static void appendTime(StringBuilder sb, SqpTime time) {
        int hour = time.getHour();
        if (hour < 10) sb.append('0');
        sb.append(hour);

        sb.append(':');
        int minute = time.getMinute();
        if (minute < 10) sb.append('0');
        sb.append(minute);

        sb.append(':');
        int second = time.getSecond();
        if (second < 10) sb.append('0');
        sb.append(second);

        char[] decimalStr = {'0', '0', '0', '0', '0', '0'};
        char[] microStr = Integer.toString(time.getNano() / 1000).toCharArray(); // PG only supports microseconds
        System.arraycopy(microStr, 0, decimalStr, decimalStr.length - microStr.length, microStr.length);
        sb.append('.');
        sb.append(decimalStr);
    }

    private void appendTimeOffset(StringBuilder sb, SqpTime time) {
        int offset = time.getOffsetSeconds();
        sb.append(offset < 0 ? '-' : '+');
        offset = Math.abs(offset);

        int hours = offset / 60 / 60;
        if (hours < 10) sb.append('0');
        sb.append(hours);

        sb.append(':');
        int mins = (offset - hours * 60 * 60) / 60;
        if (mins < 10) sb.append('0');
        sb.append(mins);

        if (_serverSupportsOffsetSeconds) {
            int secs = offset - hours * 60 * 60 - mins * 60;
            sb.append(':');
            if (secs < 10) sb.append('0');
            sb.append(secs);
        }
    }

    private static void appendEra(StringBuilder sb, SqpDate date) {
        if (date.getYear() < 0) {
            sb.append(" BC");
        }
    }
}
