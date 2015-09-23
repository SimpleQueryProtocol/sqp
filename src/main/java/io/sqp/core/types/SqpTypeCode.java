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

package io.sqp.core.types;

import io.sqp.core.TypeDescription;

/**
 * Type Codes are the "names" of SQP types and used to identify SQP values without using reflection, so they can be used
 * in serialization. They contain minimal meta information.
 *
 * For each type code there is an associated class that can be used to represent a value of that type.
 * Also, a schema is associated to all types, that provides a semantic and syntactic description of the type and
 * can be used for introspection and validation.
 * @author Stefan Burnicki
 */
// TODO: write and add missing schemas
public enum SqpTypeCode {
    /**
     * Boolean, a truth value.
     * @see SqpBoolean
     */
    Boolean(SqpBoolean.class, "schemas/Boolean.json", 1),

    /**
     * TinyInt, a 1 byte signed integer.
     * @see SqpTinyInt
     */
    TinyInt(SqpTinyInt.class, "schemas/TinyInt.json", 1),

    /**
     * SmallInt, a 2 byte signed integer.
     * @see SqpSmallInt
     */
    SmallInt(SqpSmallInt.class, "schemas/SmallInt.json", 2),

    /**
     * Integer, a 4 byte signed integer.
     * @see SqpInteger
     */
    Integer(SqpInteger.class, "schemas/Integer.json", 4),

    /**
     * BigInt, a 8 byte signed integer.
     * @see SqpBigInt
     */
    BigInt(SqpBigInt.class, "schemas/BigInt.json", 8),

    // TODO: verify precision/scale
    /**
     * Real, a 4 byte IEEE 754 floating point number.
     * @see SqpReal
     */
    Real(SqpReal.class, "schemas/Real.json", 6, 6),

    /**
     * Double, a 8 byte IEEE 754 floating point number.
     * @see SqpDouble
     */
    Double(SqpDouble.class, "schemas/Double.json", 15, 15),

    /**
     * Decimal, an exact decimal number with arbitrary precision.
     * @see SqpDecimal
     */
    Decimal(SqpDecimal.class, "schemas/Decimal.json"),

    /**
     * Char, a fixed-length character sequence.
     * @see SqpChar
     */
    Char(SqpChar.class, "schemas/Char.json"),

    /**
     * VarChar, or character varying, a variable-length character sequence.
     * @see SqpVarChar
     */
    VarChar(SqpVarChar.class, "schemas/VarChar.json"),

    /**
     * Binary, a fixed-length bit sequence. Usually constrained to length dividable by 8 (bytes)
     * @see SqpBinary
     */
    Binary(SqpBinary.class, "schemas/Binary.json", 1),

    /**
     * VarBinary, or bit varying, a variable-length bit sequence. Usually constrained to length dividable by 8 (bytes).
     * @see SqpVarBinary
     */
    VarBinary(SqpVarBinary.class, "schemas/VarBinary.json"),

    /**
     * Time, a calendar time at any day with nanosecond precision with optional offset (e.g. for time zones).
     * @see SqpTime
     */
    Time(SqpTime.class, "schemas/Time.json", 9),

    /**
     * Date, a calendar day without time.
     * @see SqpDate
     */
    Date(SqpDate.class, "schemas/Date.json", 0),

    /**
     * Timestamp, a specific point in time with nanosecond precision and optional offset (e.g. for time zones).
     * In practice, this is a combination of Date and Time.
     * @see SqpTimestamp
     */
    Timestamp(SqpTimestamp.class, "schemas/DateTime.json", 9),

    /**
     * Interval, a signed time interval with nanosecond precision.
     * @see SqpInterval
     */
    Interval(SqpInterval.class, "schemas/Interval.json", 9),

    /**
     * A custom data type which is not standard.
     * @see SqpCustom
     */
    Custom(SqpCustom.class, "schemas/Custom.json"),

    /**
     * Xml data in form of a string.
     * @see SqpXml
     */
    Xml(SqpXml.class, "schemas/Xml.json"),

    /**
     * A Binary Large OBject (BLOB), so any huge binary data.
     * @see SqpBlob
     */
    Blob(SqpBlob.class, "schemas/Blob.json"),

    /**
     * A Character Large OBject (CLOB), so any huge text.
     * @see SqpClob
     */
    Clob(SqpClob.class, "schemas/Clob.json"),

    /**
     * An unknown type, which may be used when binding parameters.
     */
    Unknown(null, "");

    Class<? extends SqpValue> _typeClass;
    String _schemaResourceName;
    int _defaultPrecision;
    int _defaultScale;

    SqpTypeCode(Class<? extends SqpValue> clazz, String schemaResourceName, int defPrecision, int defScale) {
        _schemaResourceName = schemaResourceName;
        _typeClass = clazz;
        _defaultPrecision = defPrecision;
        _defaultScale = defScale;
    }

    SqpTypeCode(Class<? extends SqpValue> clazz, String schemaResourceName, int defPrecision) {
        this(clazz, schemaResourceName, defPrecision, 0);
    }

    SqpTypeCode(Class<? extends SqpValue> clazz, String schemaResourceName) {
        this(clazz, schemaResourceName, 0);
    }

    /**
     * Gets the types default precsion
     * @return The dafault precison
     */
    public int getDefaultPrecision() {
        return _defaultPrecision;
    }

    /**
     * Gets the types maximum precision
     * @return Max precision
     */
    public int getMaxPrecision() {
        return _defaultPrecision; // TODO: use a separate value for this?
    }

    /**
     * Gets the types default scale
     * @return Default scale
     */
    public int getDefaultScale() {
        return _defaultScale;
    }

    /**
     * Gets the types max scale
     * @return Max Scale
     */
    public int getMaxScale() {
        return _defaultScale; // TODO: use a separate value for this?
    }

    /**
     * Gets the class that represents a value of this type
     * @return The representing class
     */
    public Class<? extends SqpValue> getTypeClass() {
        return _typeClass;
    }

    /**
     * Constructs a {@link TypeDescription} with default precision and scale.
     * @return A TypeDescription with this type
     */
    public TypeDescription asDescription() {
        return asDescription(-1, -1);
    }

    /**
     * Constructs a {@link TypeDescription} with custom precision and default scale.
     * @return A TypeDescription with this type
     */
    public TypeDescription asDescription(int precision) {
        return asDescription(precision, -1);
    }

    /**
     * Constructs a {@link TypeDescription} with custom precision and scale.
     * @return A TypeDescription with this type
     */
    public  TypeDescription asDescription(int precision, int scale) {
        precision = precision < 0 ? _defaultPrecision : precision;
        scale = scale < 0 ? _defaultScale : scale;
        return new TypeDescription(this, precision, scale);
    }

    /**
     * Returns the resource name of the associated JSON schema file that describes the type syntactically and semantically
     * when being serialized for use on the wire.
     * @return The schema's resource name
     */
    public String getSchemaResourceName() {
        return _schemaResourceName;
    }
}
