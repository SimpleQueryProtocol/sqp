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

import org.postgresql.core.Oid;
import io.sqp.core.types.SqpTypeCode;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Stefan Burnicki
 * Static information about PostgreSQL types and mapping to sqp types.
 * Mostly taken from the org.postgresql.jdbc2.TypeInfoCache
 * The class cannot be used directly as it depends on JDBC and blocking database calls
 */
public class TypeInfo {
    private static final String TYPE_NAME_PREFIX = "pg_";
    private static final Object _typeInfo[][] = {
            {Oid.INT2, "int2", SqpTypeCode.SmallInt, Oid.INT2_ARRAY},
            {Oid.INT4, "int4", SqpTypeCode.Integer, Oid.INT4_ARRAY},
            {Oid.OID, "oid", SqpTypeCode.Blob, Oid.OID_ARRAY},
            {Oid.INT8, "int8", SqpTypeCode.BigInt, Oid.INT8_ARRAY},
            {Oid.MONEY, "money", SqpTypeCode.Decimal, Oid.MONEY_ARRAY},
            {Oid.NUMERIC, "numeric", SqpTypeCode.Decimal, Oid.NUMERIC_ARRAY},
            {Oid.FLOAT4, "float4", SqpTypeCode.Real, Oid.FLOAT4_ARRAY},
            {Oid.FLOAT8, "float8", SqpTypeCode.Double, Oid.FLOAT8_ARRAY},
            {Oid.CHAR, "char", SqpTypeCode.TinyInt, Oid.CHAR_ARRAY},
            {Oid.BPCHAR, "bpchar", SqpTypeCode.Char, Oid.BPCHAR_ARRAY},
            {Oid.VARCHAR, "varchar", SqpTypeCode.VarChar, Oid.VARCHAR_ARRAY},
            {Oid.TEXT, "text", SqpTypeCode.VarChar, Oid.TEXT_ARRAY},
            {Oid.NAME, "name", SqpTypeCode.VarChar, Oid.NAME_ARRAY},
            {Oid.BYTEA, "bytea", SqpTypeCode.VarBinary, Oid.BYTEA_ARRAY},
            {Oid.BOOL, "bool", SqpTypeCode.Boolean, Oid.BOOL_ARRAY},
            {Oid.BIT, "bit", SqpTypeCode.Binary, Oid.BIT_ARRAY},
            {Oid.DATE, "date", SqpTypeCode.Date, Oid.DATE_ARRAY},
            {Oid.TIME, "time", SqpTypeCode.Time, Oid.TIME_ARRAY},
            {Oid.TIMETZ, "timetz", SqpTypeCode.Time, Oid.TIMETZ_ARRAY},
            {Oid.TIMESTAMP, "timestamp", SqpTypeCode.Timestamp, Oid.TIMESTAMP_ARRAY},
            {Oid.TIMESTAMPTZ, "timestamptz", SqpTypeCode.Timestamp, Oid.TIMESTAMPTZ_ARRAY},
            {Oid.INTERVAL, "interval", SqpTypeCode.Interval, Oid.INTERVAL_ARRAY},
            {Oid.POINT, "point", SqpTypeCode.Custom, null},
            // TODO: support more datatypes
    };

    private static final Map<Integer, String> _typeNameLookup;
    private static final Map<Integer, SqpTypeCode> _sqpTypeCodeLookup;
    private static final Map<String, Integer> _oidLookup;

    static {
        _typeNameLookup = new HashMap<>(_typeInfo.length);
        _sqpTypeCodeLookup = new HashMap<>(_typeInfo.length);
        _oidLookup = new HashMap<>(_typeInfo.length); // reverse of typeNameLookup

        for (Object[] stdType : _typeInfo) {
            _typeNameLookup.put((Integer) stdType[0], (String) stdType[1]);
            _sqpTypeCodeLookup.put((Integer) stdType[0], (SqpTypeCode) stdType[2]);
            _oidLookup.put((String) stdType[1], (Integer) stdType[0]);
        }
    }

    private TypeInfo() {
        // prevent intention
    }

    /**
     * Gets the PostgreSQL type name for the requested type.
     * ("OID" + oid) if the type is unknown
     * @param oid The PostgreSQL oid of the type
     * @return The name
     */
    public static String getInternalTypeName(int oid) {
        // TODO: get all type names from server
        String typeName = _typeNameLookup.get(oid);
        return typeName != null ? typeName : "oid" + oid;
    }

    /**
     * The type name with 'pg_' prefix for unique identification
     * @param oid The PostgreSQL oid
     * @return The prefixed name
     */
    public static String getTypeName(int oid) {
        return TYPE_NAME_PREFIX+ getInternalTypeName(oid);
    }

    public static int getOidFromTypeName(String typeName) {
        typeName = typeName.toLowerCase();
        if (!typeName.startsWith(TYPE_NAME_PREFIX)) {
            return -1;
        }
        typeName = typeName.substring(TYPE_NAME_PREFIX.length());
        // check that it actually exists
        return getOidFromInternalTypeName(typeName);
    }

    public static int getOidFromInternalTypeName(String internalType) {
        if (internalType.startsWith("oid")) {
            return Integer.parseInt(internalType.substring(3));
        }
        Integer oid = _oidLookup.get(internalType);
        return oid == null ? -1 : oid;
    }

    public static SqpTypeCode getSupportedSqpTypeCode(int oid) {
        SqpTypeCode typeCode = _sqpTypeCodeLookup.get(oid);
        // if no mapping exists we map it as VarChar!
        if (typeCode == null) {
            // TODO: send a warning here?
            return SqpTypeCode.VarChar;
        } else {
            return typeCode;
        }
    }

    public static int getPrecision(int oid, int typemod) {
        // taken from org.postgresql.jdbc2.TypeInfoCache.getPrecision
        // modified to exclude JDBC specific stuff

        // mostly precision/scale are default by exact datatype mappings (e.g. int2 to SmallInt)
        // however, some need extra restrictions, e.g. Time is limited to max 6 fractions in PG
        switch (oid) {
            case Oid.CHAR: // this is a single character in PG
                return 1;

            case Oid.NUMERIC:
                if (typemod == -1) {
                    return 0;
                }
                return ((typemod-4) & 0xFFFF0000) >> 16;

            case Oid.BPCHAR:
            case Oid.VARCHAR:
                if (typemod == -1) {
                    return -1;
                }
                return typemod - 4;

            case Oid.TIME:
            case Oid.TIMETZ:
            case Oid.TIMESTAMP:
            case Oid.TIMESTAMPTZ:
                if (typemod == -1) {
                    return 6; // PG default precision for time
                } else if (typemod == 1) {
                    return 2; // quirk in PG
                } else {
                    return typemod;
                }

            case Oid.INTERVAL:
                if (typemod == -1) {
                    return 6;
                }
                return typemod & 0xFFFF;

            case Oid.BIT:
                return typemod;

            case Oid.VARBIT:
                return typemod;

            default:
                return -1; // "default"
        }
    }

    public static int getScale(int oid, int typemod) {
        // taken from org.postgresql.jdbc2.TypeInfoCache.getPrecision
        // modified to exclude JDBC specific stuff
        switch (oid) {
            case Oid.NUMERIC:
                if (typemod == -1) {
                    return 0;
                }
                return (typemod - 4) & 0xFFFF;

            // TODO why is scale same as precision for the following types?
            case Oid.FLOAT4:
                return 8;

            case Oid.FLOAT8:
                return 17;

            case Oid.TIME:
            case Oid.TIMETZ:
            case Oid.TIMESTAMP:
            case Oid.TIMESTAMPTZ:
                if (typemod == -1) {
                    return 6;
                }
                return typemod;

            case Oid.INTERVAL:
                if (typemod == -1)
                    return 6;
                return typemod & 0xFFFF;

            default:
                return -1; //"default"
        }
    }
}
