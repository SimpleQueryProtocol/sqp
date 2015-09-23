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

import io.sqp.core.types.SqpTypeCode;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Stefan Burnicki
 */
public class TypeInfo {
    private static final Map<SqpTypeCode, Integer> _sqpToSQLTypeLookup;
    static {
        _sqpToSQLTypeLookup = new HashMap<>();
        _sqpToSQLTypeLookup.put(SqpTypeCode.Boolean, Types.BOOLEAN);
        _sqpToSQLTypeLookup.put(SqpTypeCode.TinyInt, Types.TINYINT);
        _sqpToSQLTypeLookup.put(SqpTypeCode.SmallInt, Types.SMALLINT);
        _sqpToSQLTypeLookup.put(SqpTypeCode.Integer, Types.INTEGER);
        _sqpToSQLTypeLookup.put(SqpTypeCode.BigInt, Types.BIGINT);
        _sqpToSQLTypeLookup.put(SqpTypeCode.Real, Types.REAL);
        _sqpToSQLTypeLookup.put(SqpTypeCode.Double, Types.DOUBLE);
        _sqpToSQLTypeLookup.put(SqpTypeCode.Decimal, Types.DECIMAL);
        _sqpToSQLTypeLookup.put(SqpTypeCode.Char, Types.CHAR);
        _sqpToSQLTypeLookup.put(SqpTypeCode.VarChar, Types.VARCHAR);
        _sqpToSQLTypeLookup.put(SqpTypeCode.Binary, Types.BINARY);
        _sqpToSQLTypeLookup.put(SqpTypeCode.VarBinary, Types.VARBINARY);
        _sqpToSQLTypeLookup.put(SqpTypeCode.Time, Types.TIME);
        _sqpToSQLTypeLookup.put(SqpTypeCode.Date, Types.DATE);
        _sqpToSQLTypeLookup.put(SqpTypeCode.Timestamp, Types.TIMESTAMP);
        _sqpToSQLTypeLookup.put(SqpTypeCode.Unknown, Types.NULL);
        _sqpToSQLTypeLookup.put(SqpTypeCode.Xml, Types.SQLXML);
        _sqpToSQLTypeLookup.put(SqpTypeCode.Blob, Types.LONGVARBINARY);
        _sqpToSQLTypeLookup.put(SqpTypeCode.Clob, Types.LONGVARCHAR);
    }

    public static int getSQLFromSqpType(SqpTypeCode typeCode) {
        Integer type = _sqpToSQLTypeLookup.get(typeCode);
        // TODO: throw error instead of returning Types.OTHER?
        return type == null ? Types.OTHER : type;
    }
}
