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
import io.sqp.backend.exceptions.ParameterBindException;
import io.sqp.core.exceptions.NotImplementedException;
import io.sqp.core.exceptions.TypeConversionException;
import io.sqp.core.exceptions.UnsupportedTypeException;
import io.sqp.core.exceptions.SqpException;
import io.sqp.core.types.*;
import transbase.tbx.TBConst;
import transbase.tbx.TBXStoredQuery;
import transbase.tbx.types.*;
import transbase.tbx.types.helpers.CharEncodingFactory;
import transbase.tbx.types.helpers.TBObject;

import java.sql.SQLException;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * @author Stefan Burnicki
 */
public class ParameterBinder {
    private static final int[] _tbTimeFields = { TBConst.TB__MS, TBConst.TB__SS, TBConst.TB__MI, TBConst.TB__HH,
                                                 TBConst.TB__DD, TBConst.TB__MO, TBConst.TB__YY};
    private CharEncodingFactory _charEncoding;
    private TBXStoredQuery _storedQuery;

    public ParameterBinder(TBXStoredQuery storedQuery, CharEncodingFactory charEncoding) {
        _storedQuery = storedQuery;
        _charEncoding = charEncoding;
    }

    public void bindBatch(List<List<SqpValue>> parameters) throws SqpException {
        boolean useBatchUpdate = parameters.size() > 1;
        for (List<SqpValue> parameterList : parameters) {
            bindList(parameterList);
            if (!useBatchUpdate) {
                break;
            }
            try {
                _storedQuery.addBatch();
            } catch (SQLException e) {
                throw new ExecutionFailedException(e);
            }
        }
    }

    private void bindList(List<SqpValue> parameters) throws SqpException {
        for (int i = 0; i < parameters.size(); i++) {
            bindParameter(i, parameters.get(i));
        }
    }

    private void bindParameter(int i, SqpValue param) throws SqpException {
        try {
            TBObject value = createTBObject(param);
            // note that stored query uses 1-based indexing
            _storedQuery.setObject(i + 1, value);
        } catch (SQLException e) {
            throw new ExecutionFailedException(e);
        }
    }

    private TBObject createTBObject(SqpValue param) throws SqpException, SQLException {
        SqpTypeCode typeCode = param.getType();
        if (param.isNull()) {
            return new TBNulltyp(tbTypeFromSqpType(typeCode));
        }
        TBObject tbValue;
        switch (typeCode) {
            case Boolean:
                tbValue = new TBBool();
                tbValue.setBoolean(param.asBoolean());
                return tbValue;

            case TinyInt:
                tbValue = new TBTinyint();
                tbValue.setByte(param.asByte());
                return tbValue;

            case SmallInt:
                tbValue = new TBSmallint();
                tbValue.setShort(param.asShort());
                return tbValue;

            case Integer:
                tbValue = new TBInteger();
                tbValue.setInt(param.asInt());
                return tbValue;

            case BigInt:
                tbValue = new TBBigint();
                tbValue.setLong(param.asLong());
                return tbValue;

            case Real:
                // TODO: check for inf, -inf, NaN and throw
                tbValue = new TBFloat();
                tbValue.setFloat(param.asFloat());
                return tbValue;

            case Double:
                // TODO: check for inf, -inf, NaN and throw
                tbValue = new TBDouble();
                tbValue.setDouble(param.asDouble());
                return tbValue;

            case Decimal:
                tbValue = new TBNumeric();
                tbValue.setBigDecimal(param.asBigDecimal());
                return tbValue;

            case Char:
                tbValue = new TBChar(_charEncoding);
                tbValue.setString(param.asString());
                return tbValue;

            case VarChar:
                tbValue = new TBVarchar(_charEncoding);
                tbValue.setString(param.asString());
                return tbValue;

            case Binary:
            case VarBinary:
                // we use TB__BINCHAR as this should be compatible to also BITS and BITS2
                tbValue = new TBBinchar();
                tbValue.setBytes(param.asBytes());
                return tbValue;

            case Time:
                return createDateTimeFromTime(secureCast(param, SqpTime.class));

            case Date:
                return createDateTimeFromDate(secureCast(param, SqpDate.class));

            case Timestamp:
                return createDateTimeFromTimestamp(secureCast(param, SqpTimestamp.class));

            case Interval:
                return createTimespanFromInterval(secureCast(param, SqpInterval.class));

            case Custom:
                return createFromCustom(secureCast(param, SqpCustom.class));

            case Blob:
                TBBlob blob = new TBBlob(TBConst.TB__BLOB, _charEncoding);
                blob.setBinaryStream(secureCast(param, SqpBlob.class).getInputStream(), -1);
                return blob;

            case Clob:
                TBBlob clob = new TBBlob(TBConst.TB__CLOB, _charEncoding);
                clob.setBinaryStream(secureCast(param, SqpClob.class).getInputStream(), -1);
                return clob;

            case Xml:
                TBBlob xmlClob = new TBBlob(TBConst.TB__CLOB, _charEncoding);
                xmlClob.setAsciiStream(secureCast(param, SqpClob.class).getInputStream(), -1);
                xmlClob.setString(secureCast(param, SqpXml.class).asString());
                return xmlClob;
        }
        throw new ParameterBindException(typeCode, new NotImplementedException("Support for this datatype is not yet implemented"));
    }

    private int tbTypeFromSqpType(SqpTypeCode typeCode) {
        switch (typeCode) {
            case Boolean:
                return TBConst.TB__BOOL;
            case TinyInt:
                return TBConst.TB__TINYINT;
            case SmallInt:
                return TBConst.TB__SMALLINT;
            case Integer:
                return TBConst.TB__INTEGER;
            case BigInt:
                return TBConst.TB__BIGINT;
            case Real:
                return TBConst.TB__BIGINT;
            case Double:
                return TBConst.TB__DOUBLE;
            case Decimal:
                return TBConst.TB__NUMERIC;
            case Char:
                return TBConst.TB__CHAR;
            case VarChar:
                return TBConst.TB__VARCHAR;
            case Binary:
            case VarBinary:
                return TBConst.TB__BITSS2;
            case Time:
            case Date:
            case Timestamp:
                return TBConst.TB__DATETIME;
            case Interval:
                return TBConst.TB__TIMESPAN;

            case Xml:
                return TBConst.TB__CLOB;

            case Blob:
                return TBConst.TB__BLOB;

            case Clob:
                return TBConst.TB__CLOB;

            default:
                return TBConst.TB__UNDEFTYPE;
        }
    }

    private <T extends SqpValue> T secureCast(SqpValue sqpValue, Class<T> targetType) throws InternalErrorException {
        if (!targetType.isAssignableFrom(sqpValue.getClass())) {
            throw new InternalErrorException("Passed internally an " + sqpValue.getClass().getName() + " where a " +
                    targetType.getName() + " was expected");
        }
        return targetType.cast(sqpValue);
    }

    private TBDatetime createTimespanFromInterval(SqpInterval sqpInterval) {
        int[] sqpIntervalValues = {sqpInterval.getNanos() / 1000_000, sqpInterval.getSeconds(),
                sqpInterval.getMinutes(), sqpInterval.getHours(), sqpInterval.getDays(), sqpInterval.getMonths(),
                sqpInterval.getYears()};
        TBTimespan timespan = new TBTimespan();

        // we try to get the interval as small as possible, so we watch out for the actual set values
        int lowFieldIdx = getLowFieldIdx(sqpIntervalValues);
        if (lowFieldIdx < 0) { // no low field found means that the interval is simply null
            timespan.setNull();
            return timespan;
        }

        timespan.setSign(sqpInterval.getSign().equals(SqpInterval.Sign.Negative) ? TBConst.MINUSS : TBConst.PLUSS);
        int highFieldIdx = getHighFieldIdx(lowFieldIdx, sqpIntervalValues);
        timespan.setLowField(_tbTimeFields[lowFieldIdx]);
        timespan.setHighField(_tbTimeFields[highFieldIdx]);

        // set the fields
        for (int i = lowFieldIdx; i <= highFieldIdx; i++) {
            timespan.setField(_tbTimeFields[i], sqpIntervalValues[i]);
        }
        return timespan;
    }

    private TBDatetime createDateTimeFromTimestamp(SqpTimestamp sqpTimestamp) throws TypeConversionException {
        TBDatetime datetime = new TBDatetime();
        datetime.setLowField(TBConst.TB__MS);
        datetime.setHighField(TBConst.TB__YY);
        setDateTimeFromSqpTime(datetime, sqpTimestamp.getTime());
        setDateTimeFromSqpDate(datetime, sqpTimestamp.getDate());
        return datetime;
    }

    private TBDatetime createDateTimeFromTime(SqpTime sqpTime) throws TypeConversionException {
        TBDatetime datetime = new TBDatetime();
        datetime.setLowField(TBConst.TB__MS);
        datetime.setHighField(TBConst.TB__HH);
        setDateTimeFromSqpTime(datetime, sqpTime);
        return datetime;
    }

    private TBDatetime createDateTimeFromDate(SqpDate sqpDate) throws TypeConversionException {
        TBDatetime datetime = new TBDatetime();
        datetime.setLowField(TBConst.TB__DD);
        datetime.setHighField(TBConst.TB__YY);
        setDateTimeFromSqpDate(datetime, sqpDate);
        return datetime;
    }

    private void setDateTimeFromSqpTime(TBDatetime datetime, SqpTime sqpTime) throws TypeConversionException {
        // TODO: validate that behavior
        if (sqpTime.hasOffset()) {
            LocalTime utcTime = offsetTimeToUTC(sqpTime.asOffsetTime());
            setTimeFields(datetime, utcTime.getHour(), utcTime.getMinute(), utcTime.getSecond(), utcTime.getNano());
        } else {
            setTimeFields(datetime, sqpTime.getHour(), sqpTime.getMinute(), sqpTime.getSecond(), sqpTime.getNano());
        }
    }

    private void setTimeFields(TBDatetime datetime, int hours, int minutes, int seconds, int nanos) {
        datetime.setField(TBConst.TB__HH, hours);
        datetime.setField(TBConst.TB__MI, minutes);
        datetime.setField(TBConst.TB__SS, seconds);
        datetime.setField(TBConst.TB__MS, nanos / 1000_000); // we only have milliseconds resolution
    }

    private void setDateTimeFromSqpDate(TBDatetime datetime, SqpDate sqpDate) throws TypeConversionException {
        int year = sqpDate.getYear();
        if (year < 1) {
            throw new TypeConversionException("The database only accepts dates in year after 0, (no dates BC)");
        }
        datetime.setField(TBConst.TB__YY, year);
        datetime.setField(TBConst.TB__MO, sqpDate.getMonth());
        datetime.setField(TBConst.TB__DD, sqpDate.getDay());
    }

    private LocalTime offsetTimeToUTC(OffsetTime offsetTime) {
        LocalTime lt = offsetTime.toLocalTime();
        return lt.minus(offsetTime.getOffset().getTotalSeconds(), ChronoUnit.SECONDS);
    }

    private TBObject createFromCustom(SqpCustom custom) throws SqpException {
        String typeName = custom.getCustomTypeName();
        switch (TBTypeRepository.getCustomType(typeName)) {
            case DateTime:
                DateRangeSpecifier[] specifiers = TBTypeRepository.parseRangeSpecifiers(typeName);
                return createDateTimeFromCustom(specifiers[0], specifiers[1], custom.as(int[].class));
        }
        throw new UnsupportedTypeException("Custom type '" + custom.getCustomTypeName() + "' is not supported.");
    }

    private TBObject createDateTimeFromCustom(DateRangeSpecifier highField, DateRangeSpecifier lowField, int[] values) throws SqpException {
        List<DateRangeSpecifier> fields;
        try {
            fields = DateRangeSpecifier.range(highField, lowField);
        } catch (IllegalArgumentException e) {
            throw new UnsupportedTypeException("Invalid DateTime range from " + highField + " to " + lowField);
        }
        int numFields = fields.size();
        if (values.length != numFields) {
            throw new TypeConversionException("DateTime has " + values.length + " fields, but " + numFields + " are needed.");
        }
        TBDatetime datetime = new TBDatetime();
        datetime.setHighField(highField.getTbValue());
        datetime.setLowField(lowField.getTbValue());
        for (int i = 0; i < numFields; i++) {
            datetime.setField(fields.get(i).getTbValue(), values[i]);
        }
        return datetime;
    }


    private int getLowFieldIdx(int[] intervalValues) {
        for (int i = 0; i < _tbTimeFields.length; i++) {
            if (intervalValues[i] != 0) {
                return i;
            }
        }
        return -1;
    }

    private int getHighFieldIdx(int lowFieldIdx, int[] intervalValues) {
        int highFieldIdx = lowFieldIdx;
        for (int i = lowFieldIdx; i < _tbTimeFields.length; i++) {
            if (intervalValues[i] != 0) {
                highFieldIdx = i;
            }
        }
        return highFieldIdx;
    }
}
