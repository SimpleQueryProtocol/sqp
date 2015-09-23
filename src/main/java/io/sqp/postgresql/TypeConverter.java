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
import org.postgresql.core.Encoding;
import org.postgresql.core.Field;
import org.postgresql.core.Oid;
import org.postgresql.util.PGbytea;
import io.sqp.core.TypeDescription;
import io.sqp.core.exceptions.TypeConversionException;
import io.sqp.core.exceptions.SqpException;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * @author Stefan Burnicki
 * In contrast to org.postgresql.jdbc2.AbstractJdbc2ResultSet where I got inspiration for this,
 * the conversion methods are less tolerant. That is because the field type is known, so we can highly
 * assume that the value is actually matching the type that was announced. I.e. parseInt2AsShort assumes that it's only
 * used for short values. It won't be able to parse a money field as short.
 */
public class TypeConverter {
    private Encoding _encoding;
    private TimeUtils _timeUtils;
    private Locale _serverLocale;
    private List<Integer> _allowedNativeTypes;


    public TypeConverter(PGConnection connection, Locale serverLocale) throws SqpException {
        _serverLocale = serverLocale;
        _encoding = connection.getProtocolConnection().getEncoding();
        _allowedNativeTypes = new ArrayList<>();
        _timeUtils = new TimeUtils(connection);
    }

    public void addAllowedNativeTypes(List<String> allowedNativeTypes) {
        // TODO: we could send warnings for types we couldn't resolve
        _allowedNativeTypes.addAll(allowedNativeTypes.stream()
                .mapToInt(TypeInfo::getOidFromTypeName)
                .filter(i -> i >= 0)
                .boxed().collect(Collectors.toList()));
    }

    public TypeDescription mapToTypeDescription(Field field) {
        int oid = field.getOID();
        int typemod = field.getMod();
        int precision = TypeInfo.getPrecision(oid, typemod);
        int scale = TypeInfo.getScale(oid, typemod);
        SqpTypeCode typeCode = TypeInfo.getSupportedSqpTypeCode(oid);
        // TODO: this is still ugly. somehow merge this class with type info and put the decision at the right place
        if (typeCode == SqpTypeCode.Custom && !_allowedNativeTypes.contains(oid)) {
            typeCode = SqpTypeCode.VarChar;
        }
        return typeCode.asDescription(precision, scale);
    }

    public SqpValue toSqpType(Field field, byte[] value, TypeDescription typeDescription) throws SqpException {
        // currently type description is just to internally check that we are not accidentally convert to a different
        // type. This assures we actually send data as we announced it in the cursor description
        // Note that we assume that the source of field/value is trusted, i.e. that the value really fits the
        // field's oid.

        SqpTypeCode mapping = typeDescription.getTypeCode();

        // first handle null
        if (value == null) {
            return new SqpNull(mapping);
        }
        int oid = field.getOID();
        // TODO: enable support for some binary types
        if (isBinary(field)) {
            throw new TypeConversionException("No support for binary fields, yet.", null);
        }
        switch (oid) {
            case Oid.INT2:
                assert mapping == SqpTypeCode.SmallInt;
                return new SqpSmallInt(parseInt2AsShort(value));

            case Oid.INT4:
                assert mapping == SqpTypeCode.Integer;
                return new SqpInteger(parseInt4AsInt(value));

            case Oid.INT8:
                assert mapping == SqpTypeCode.BigInt;
                return new SqpBigInt(parseInt8AsLong(value));

            case Oid.NUMERIC:
                assert mapping == SqpTypeCode.Decimal;
                return new SqpDecimal(parseNumericAsDecimal(value));

            case Oid.MONEY:
                assert mapping == SqpTypeCode.Decimal;
                return new SqpDecimal(parseMoneyAsDecimal(value));

            case Oid.FLOAT4:
                assert mapping == SqpTypeCode.Real;
                return new SqpReal(parseFloat4AsFloat(value));

            case Oid.FLOAT8:
                assert mapping == SqpTypeCode.Double;
                return new SqpDouble(parseFloat8AsDouble(value));

            case Oid.BOOL:
                assert mapping == SqpTypeCode.Boolean;
                return new SqpBoolean(parseBoolAsBoolean(value));

            case Oid.DATE:
                assert mapping == SqpTypeCode.Date;
                return parseDateAsSqpDate(value);

            case Oid.TIME:
            case Oid.TIMETZ:
                assert mapping == SqpTypeCode.Time;
                return parseTimeAsSqpTime(value);

            case Oid.TIMESTAMP:
            case Oid.TIMESTAMPTZ:
                assert mapping == SqpTypeCode.Timestamp;
                return parseTimestampAsSqpTimestamp(value);

            case Oid.NAME:
            case Oid.TEXT:
            case Oid.VARCHAR:
                assert mapping == SqpTypeCode.VarChar;
                return new SqpVarChar(parseString(value));

            case Oid.OID:
                assert mapping == SqpTypeCode.Blob;
                // TODO: find out the size in pass it as second arg
                return new SqpBlob(Long.toString(parseInt8AsLong(value)), -1);

            case Oid.BPCHAR:
                assert mapping == SqpTypeCode.Char;
                return new SqpChar(parseString(value));

            case Oid.BIT:
                assert mapping == SqpTypeCode.Binary;
                return new SqpBinary(value);

            case Oid.VOID:
                break;

            case Oid.INTERVAL:
                break;

            case Oid.CHAR:
                // This is not char(N), this is "char" a single byte type.
                assert mapping == SqpTypeCode.TinyInt;
                return new SqpTinyInt(parseCharAsByte(value));

            case Oid.VARBIT:
            case Oid.BYTEA:
                assert mapping == SqpTypeCode.VarBinary;
                return new SqpVarBinary(parseBytes(value));

            case Oid.UUID:
                break;

            case Oid.POINT:
                if (mapping == SqpTypeCode.Custom) {
                    return new SqpCustom(parsePointAsDoubleArray(value));
                }
                // otherwise default mapping

            // TODO: support for more data types
            case Oid.XML:
            case Oid.BOX:
                // fall-through
            // TODO: support for arrays
            case Oid.INT2_ARRAY:
            case Oid.INT4_ARRAY:
            case Oid.INT8_ARRAY:
            case Oid.TEXT_ARRAY:
            case Oid.NUMERIC_ARRAY:
            case Oid.FLOAT4_ARRAY:
            case Oid.FLOAT8_ARRAY:
            case Oid.BOOL_ARRAY:
            case Oid.DATE_ARRAY:
            case Oid.TIME_ARRAY:
            case Oid.TIMETZ_ARRAY:
            case Oid.TIMESTAMP_ARRAY:
            case Oid.TIMESTAMPTZ_ARRAY:
            case Oid.BYTEA_ARRAY:
            case Oid.VARCHAR_ARRAY:
            case Oid.OID_ARRAY:
            case Oid.BPCHAR_ARRAY:
            case Oid.MONEY_ARRAY:
            case Oid.NAME_ARRAY:
            case Oid.BIT_ARRAY:
            case Oid.INTERVAL_ARRAY:
            case Oid.CHAR_ARRAY:
            case Oid.VARBIT_ARRAY:
            case Oid.UUID_ARRAY:
            case Oid.XML_ARRAY:
                // fall-through
                break;
        }
        assert mapping == SqpTypeCode.VarChar;
        // TODO: check for binary mode and throw error...
        return new SqpVarChar(parseString(value));
    }

    public boolean isBinary(Field field) {
        return field.getFormat() == Field.BINARY_FORMAT;
    }

    private String parseString(byte[] value) throws SqpException {
        try {
            return _encoding.decode(value);
        } catch (IOException e) {
            // taken from JDBC driver
            String error =  "Invalid character data was found.  This is most likely caused by stored data containing " +
                            "characters that are invalid for the character set the database was created in.  The " +
                            "most common example of this is storing 8bit data in a SQL_ASCII database.";
            throw new TypeConversionException(error);
        }
    }


    private byte[] parseBytes(byte[] value) throws TypeConversionException {
        // TODO: really implement binary communication
        try {
            return PGbytea.toBytes(value);
        } catch (SQLException e) {
            throw new TypeConversionException("Invalid type from DBMS. Could not convert to bytes[]");
        }
    }

    private short parseInt2AsShort(byte[] value) throws SqpException {
        String strval = parseString(value).trim();
        try {
            return Short.parseShort(strval);
        } catch (NumberFormatException e) {
            throw new TypeConversionException("Invalid type from DBMS. Could not convert value '" + strval + "' to short.");
        }
    }

    private int parseInt4AsInt(byte[] value) throws SqpException {
        String strval = parseString(value).trim();
        try {
            return Integer.parseInt(strval);
        } catch (NumberFormatException e) {
            throw new TypeConversionException("Invalid type from DBMS. Could not convert value '" + strval + "' to int.");
        }
    }

    private long parseInt8AsLong(byte[] value) throws SqpException {
        String strval = parseString(value).trim();
        try {
            return Long.parseLong(strval);
        } catch (NumberFormatException e) {
            throw new TypeConversionException("Invalid type from DBMS. Could not convert value '" + strval + "' to long.");
        }
    }

    private float parseFloat4AsFloat(byte[] value) throws SqpException {
        String strval = parseString(value).trim();
        try {
            return Float.parseFloat(strval);
        } catch (NumberFormatException e) {
            throw new TypeConversionException("Invalid type from DBMS. Could not convert value '" + strval + "' to float.");
        }
    }

    private double parseFloat8AsDouble(byte[] value) throws SqpException {
        String strval = parseString(value).trim();
        try {
            return Double.parseDouble(strval);
        } catch (NumberFormatException e) {
            throw new TypeConversionException("Invalid type from DBMS. Could not convert value '" + strval + "' to double.");
        }
    }

    private boolean parseBoolAsBoolean(byte[] value) throws SqpException {
        String strval = parseString(value).trim();
        if (strval.equalsIgnoreCase("t") || strval.equalsIgnoreCase("true") || strval.equals("1"))
            return true;

        if (strval.equalsIgnoreCase("f") || strval.equalsIgnoreCase("false") || strval.equals("0"))
            return false;
        throw new TypeConversionException("Invalid type from DBMS. Expected a boolean as string, got value '" + strval + "'.");
    }

    private BigDecimal parseNumericAsDecimal(byte[] value) throws SqpException {
        String strval = parseString(value);
        try {
            return new BigDecimal(strval);
        } catch (NumberFormatException e) {
            throw new TypeConversionException("Invalid type from DBMS. Could not convert value '" + strval + "' to Decimal.");
        }
    }

    private BigDecimal parseMoneyAsDecimal(byte[] value) throws SqpException {
        String strval = parseString(value);
        //strval = trimPotentialCurrencies(strval);
        try {
            DecimalFormat format = (DecimalFormat) NumberFormat.getCurrencyInstance(_serverLocale);
            format.setParseBigDecimal(true);
            return (BigDecimal) format.parseObject(strval);
        } catch (ParseException | NumberFormatException e) {
            throw new TypeConversionException("Cannot map DBMS money value '" + strval + "' to Decimal.");
        }
    }

    private byte parseCharAsByte(byte[] value) throws SqpException {
        String strval = parseString(value).trim();
        try {
            // from the JDBC method it looks like this can be empty somehow
            return strval.isEmpty() ? 0 : Byte.parseByte(strval);
        } catch (NumberFormatException e) {
            throw new TypeConversionException("Invalid type from DBMS. Could not convert value '" + strval + "' to byte.");
        }
    }

    private double[] parsePointAsDoubleArray(byte[] value) throws SqpException {
        String strval = parseString(value);
        String numberList = strval;
        if (numberList.startsWith("(")) {
            numberList = strval.substring(1, strval.length() - 1); // remove () from value
        }
        String[] parts = numberList.split(",", 2);
        if (parts.length < 2) {
            throw new TypeConversionException("Invalid point value from DBMS. Not enough values in '" + strval + "'");
        }
        try {
            return new double[] {Double.parseDouble(parts[0]), Double.parseDouble(parts[1])};
        } catch (NumberFormatException | NullPointerException e) {
            throw new TypeConversionException("Invalid point value from DBMS. The point '" + strval + "' doesn't contain valid doubles");
        }
    }

    private SqpDate parseDateAsSqpDate(byte[] value) throws SqpException {
        String strval = parseString(value);
        return _timeUtils.parseSqpDate(strval);
    }

    private SqpTime parseTimeAsSqpTime(byte[] value) throws SqpException {
        String strval = parseString(value);
        return _timeUtils.parseSqpTime(strval);
    }

    private SqpTimestamp parseTimestampAsSqpTimestamp(byte[] value) throws SqpException {
        String strval = parseString(value);
        return _timeUtils.parseSqpTimestamp(strval);
    }


}
