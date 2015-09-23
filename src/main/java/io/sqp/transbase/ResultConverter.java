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

import io.sqp.core.TypeDescription;
import io.sqp.core.exceptions.TypeConversionException;
import io.sqp.core.exceptions.UnsupportedTypeException;
import io.sqp.core.exceptions.SqpException;
import io.sqp.core.types.*;
import transbase.tbx.TBConst;
import transbase.tbx.types.TBBlob;
import transbase.tbx.types.TBDatetime;
import transbase.tbx.types.TBTimespan;
import transbase.tbx.types.helpers.BlobStream;
import transbase.tbx.types.helpers.TBObject;
import transbase.tbx.types.helpers.TSpec;
import transbase.tbx.types.info.TBTypeInfo;

import java.sql.SQLException;
import java.util.List;

/**
 * @author Stefan Burnicki
 */
public class ResultConverter {
    public final static int TB_SECOND_FRACTIONS_PRECISION = 3; // transbase supports millisecond resolution
    public final static int TB_FILL_IN_YEAR = 1970; // default year if datetime doesn't contain year
    public final static int TB_FILL_IN_MONTH = 1; // default month, if datetime doesn't contain month
    public final static int TB_FILL_IN_DAY = 1; // default day, if datetime doesn't contain day

    private NativeResultConverter _nativeResultConverter;
    private LobManager _lobManager;

    public ResultConverter(LobManager lobManager) {
        _lobManager = lobManager;
        _nativeResultConverter = new NativeResultConverter();
    }

    public void addAllowedNativeTypes(List<String> allowedNativeTypes) {
        _nativeResultConverter.addAllowedTypes(allowedNativeTypes);
    }

    public LobManager getLobManager() {
        return _lobManager;
    }

    public TypeDescription getSqpTypeDescription(TSpec typeSpec, TBTypeInfo typeInfo) throws UnsupportedTypeException {
        if (_nativeResultConverter.canMapToNativeType(typeSpec)) {
            return SqpTypeCode.Custom.asDescription();
        }
        switch (typeSpec.getType()) {
            case TBConst.TB__TINYINT:
                return SqpTypeCode.TinyInt.asDescription();

            case TBConst.TB__SMALLINT:
                return SqpTypeCode.SmallInt.asDescription();

            case TBConst.TB__INTEGER:
                return SqpTypeCode.Integer.asDescription();

            case TBConst.TB__BIGINT:
                return SqpTypeCode.BigInt.asDescription();

            case TBConst.TB__NUMERIC:
                return SqpTypeCode.Decimal.asDescription(typeInfo.getPrecision(), typeInfo.getScale());

            case TBConst.TB__FLOAT:
                return SqpTypeCode.Real.asDescription();

            case TBConst.TB__DOUBLE:
                return SqpTypeCode.Double.asDescription();

            case TBConst.TB__DATETIME:
                return mapDateTimeToSqpType(typeSpec);

            case TBConst.TB__TIMESPAN:
                return SqpTypeCode.Interval.asDescription(TB_SECOND_FRACTIONS_PRECISION);

            case TBConst.TB__BOOL:
                return SqpTypeCode.Boolean.asDescription();

            case TBConst.TB__BINCHAR:
            case TBConst.TB__CHAR:
                return SqpTypeCode.Char.asDescription(typeInfo.getPrecision());

            case TBConst.TB__BITSS:
            case TBConst.TB__BITSS2:
                return SqpTypeCode.Binary.asDescription(typeInfo.getPrecision());


            case TBConst.TB__FILEREF:
            case TBConst.TB__VARCHAR:
                return SqpTypeCode.VarChar.asDescription(typeInfo.getPrecision());

            case TBConst.TB__BLOB:
                return SqpTypeCode.Blob.asDescription();

            case TBConst.TB__CLOB:
                return SqpTypeCode.Clob.asDescription();

            case TBConst.TB__NULLTYP: // I don't understand if this happens at all. This doesn't make sense
            case TBConst.TB__MONEY: // No support in TB itself, yet
            case TBConst.TB__UNDEFTYPE: // This is just a special-case value, not an actual data type
            default: // TB's datatypes aren't extensible. We don't need to cast to string by default. If we're here, this is erroneous anyway.
                break;
        }
        throw new UnsupportedTypeException("The transbase type '" + typeInfo.getTypeName() + "' is not supported.");
    }

    public SqpValue valueToSqpValue(TBObject value, TypeDescription targetType, Cursor cursor) throws SqpException {
        try {
            return valueToSqp(value, targetType, cursor);
        } catch (SQLException e) {
            throw new TypeConversionException("Could not convert value of type " + value.getClass() + " to " + targetType, e);
        }
    }

    private SqpValue valueToSqp(TBObject value, TypeDescription targetTypeDescription, Cursor cursor) throws SQLException, SqpException {
        // we already send the target types via cursor description. So we rely on the capability to cast the actual
        // value to the desired type
        SqpTypeCode targetType = targetTypeDescription.getTypeCode(); // maybe we need precision and scale some other day
        if (value.isNull()) {
            return new SqpNull(targetType);
        }

        switch (targetType) {
            case Boolean:
                return new SqpBoolean(value.getBoolean());

            case TinyInt:
                return new SqpTinyInt(value.getByte());

            case SmallInt:
                return new SqpSmallInt(value.getShort());

            case Integer:
                return new SqpInteger(value.getInt());

            case BigInt:
                return new SqpBigInt(value.getLong());

            case Real:
                return new SqpReal(value.getFloat());

            case Double:
                return new SqpDouble(value.getDouble());

            case Decimal:
                return new SqpDecimal(value.getBigDecimal());

            case Char:
                return new SqpChar(value.getString());

            case VarChar:
                return new SqpVarChar(value.getString());

            case Binary:
                return new SqpBinary(value.getBytes());

            case VarBinary:
                // Note: there is no tb type that gets mapped to var bit. We implement this anyway, because why not.
                return new SqpVarBinary(value.getBytes());

            case Time:
                return parseTimeFromDateTime(secureCast(value, TBDatetime.class));

            case Date:
                return parseDateFromDateTime(secureCast(value, TBDatetime.class));

            case Timestamp:
                return parseTimestampFromDateTime(secureCast(value, TBDatetime.class));

            case Interval:
                return parseIntervalFromTimespan(secureCast(value, TBTimespan.class));

            case Custom:
                return _nativeResultConverter.mapNative(value);

            case Blob:
                TBBlob tbBlob = secureCast(value, TBBlob.class);
                return new SqpBlob(registerLob(tbBlob, cursor), tbBlob.length());

            case Clob:
                TBBlob tbClob = secureCast(value, TBBlob.class);
                return new SqpClob(registerLob(tbClob, cursor), tbClob.length());

            case Xml:
                // this shouldn't happen, because TB doesn't support XML
                return new SqpXml(value.getString());
        }
        throw new TypeConversionException("The value " + value.getString() + " could not be converted to " + targetType);
    }

    private String registerLob(TBBlob tbBlob, Cursor cursor) throws SQLException {
        BlobStream blobStream = tbBlob.getBlobStream();
        boolean isClob = tbBlob.getTBType() == TBConst.TB__CLOB;
        return _lobManager.registerLob(blobStream, cursor, isClob);
    }

    private static TypeDescription mapDateTimeToSqpType(TSpec typeSpec) {
        // first check for a date (or subrange)
        if (typeSpec.getLowf() >= TBConst.TB__DD && typeSpec.getHighf() <= TBConst.TB__YY) {
            return SqpTypeCode.Date.asDescription();
        // otherwise check for a time (or subrange)
        } else if (typeSpec.getHighf() <= TBConst.TB__HH && typeSpec.getLowf() >= TBConst.TB__MS) {
            return SqpTypeCode.Time.asDescription(TB_SECOND_FRACTIONS_PRECISION);
        }
        // otherwise we just take a timestamp with all fields
        return SqpTypeCode.Timestamp.asDescription(TB_SECOND_FRACTIONS_PRECISION);
    }

    private static <T extends TBObject> T secureCast(TBObject value, Class<T> targetClass) throws TypeConversionException {
        if (targetClass.isAssignableFrom(value.getClass())) {
            return targetClass.cast(value);
        }
        throw new TypeConversionException("Value is not of expected type " + targetClass.getName() +
                ", but of type " + value.getClass().getName());
    }

    private static SqpTime parseTimeFromDateTime(TBDatetime datetime) {
        int hour = datetime.getField(TBConst.TB__HH);
        int minute = datetime.getField(TBConst.TB__MI);
        int second = datetime.getField(TBConst.TB__SS);
        int milli = datetime.getField(TBConst.TB__MS);
        return new SqpTime(hour, minute, second, milli * 1000_000); // * 1000_000 : milli to nano
    }

    private static SqpDate parseDateFromDateTime(TBDatetime datetime) {
        int year = datetime.getField(TBConst.TB__YY);
        int month = datetime.getField(TBConst.TB__MO);
        int day = datetime.getField(TBConst.TB__DD);
        year = year == 0 ? TB_FILL_IN_YEAR : year;
        month = month == 0 ? TB_FILL_IN_MONTH : month;
        day = day == 0 ? TB_FILL_IN_DAY : day;
        return new SqpDate(year, month, day);
    }

    private static SqpTimestamp parseTimestampFromDateTime(TBDatetime datetime) {
        SqpDate date = parseDateFromDateTime(datetime);
        SqpTime time = parseTimeFromDateTime(datetime);
        return new SqpTimestamp(date, time);
    }

    private static SqpInterval parseIntervalFromTimespan(TBTimespan timespan) {
        int years = timespan.getField(TBConst.TB__YY);
        int months = timespan.getField(TBConst.TB__MO);
        int days = timespan.getField(TBConst.TB__DD);
        int hours = timespan.getField(TBConst.TB__HH);
        int minutes = timespan.getField(TBConst.TB__MI);
        int seconds = timespan.getField(TBConst.TB__SS);
        int millis = timespan.getField(TBConst.TB__MS);
        SqpInterval.Sign sign = timespan.getSign() == TBConst.MINUSS ? SqpInterval.Sign.Negative : SqpInterval.Sign.Positive;
        return new SqpInterval(sign, years, months, days, hours, minutes, seconds, millis *1000_000); // *1000_000 for milli to nano
    }
}
