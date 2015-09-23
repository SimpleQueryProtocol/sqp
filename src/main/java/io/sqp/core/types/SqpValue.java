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

import io.sqp.core.MessageEncoder;
import io.sqp.core.jackson.JacksonMessageEncoder;
import io.sqp.client.impl.InternalException;
import io.sqp.core.exceptions.SqpException;
import io.sqp.core.exceptions.TypeConversionException;
import io.sqp.core.util.TypeUtil;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.OffsetTime;

/**
 * Base class of all SQP-typed values.
 * Each subclass is a SQP standard type which can be instantiate to represents a specific value, e.g. a date, a decimal
 * number, a binary object, etc. This base class is especially useful as it defines a variety of casting operations
 * that the concrete types may override, so they can be easily used in a JVM language.
 * Each instance has an associated type code, which is some kind of meta description of the type and can be used to
 * identify the value's type without using reflection.
 *
 * @author Stefan Burnicki
 * @see SqpTypeCode
 */
public abstract class SqpValue {
    final private SqpTypeCode _type;

    /**
     * Constructor to be used by derived classes.
     * @param type The type code of the represented value.
     */
    protected SqpValue(SqpTypeCode type) {
        _type = type;
    }

    /**
     * Returns the type code of the represented value.
     * @return The value's type code
     */
    public SqpTypeCode getType() {
        return _type;
    }

    /**
     * Returns an object that represents the value in a JSON-compatible format to be used for serialization.
     * JSON-like format might be primitive values, but could also be a mixture of key-value structures and tuples.
     * Key-Value structures might be maps or objects with getters, while tuples might be arrays or lists.
     * <p>
     * This function will be used to serialize the value with a {@link MessageEncoder}, which is probably
     * a {@link JacksonMessageEncoder}. The returned type should not contain type information, but
     * should only represent the value itself, as type information is always sent separately to reduce overhead.
     * <p>
     * The factory method {@link #createFromJsonFormat(SqpTypeCode, Object)} can be used to reconstruct the value from
     * the JSON-compatible format.
     *
     * @return The value in a JSON-compatible format
     * @see #createFromJsonFormat(SqpTypeCode, Object)
     */
    abstract public Object getJsonFormatValue();

    /**
     * Tries to get the object value by using {@link #asObject()} and coverts the object value then to the target
     * type by using {@link TypeUtil#checkAndConvert}. Note that this won't work with all values, as for example
     * a {@link SqpDate} cannot be represented as an object.
     * <p>
     * However, this method will especially useful with {@link SqpCustom} values. Not only it's easy to convert some
     * tuple to an array for example (while doing auto type checks), but also as this method utilizes
     * Jacksons ObjectMapper for conversion. The ObjectMapper is even capable of mapping some JSON-compatible values
     * to "real" objects by using setters or a special constructor.
     *
     * @param targetClass The class object to convert the value to
     * @param <T> The type to convert to
     * @return The converted value
     * @throws TypeConversionException If conversion was not possible
     * @see TypeUtil#checkAndConvert(Object, Class, String)
     * @see SqpCustom
     */
    public <T> T as(Class<T> targetClass) throws TypeConversionException {
        Object objectVal = asObject();
        if (objectVal == null) {
            return null;
        }
        try {
            return TypeUtil.checkAndConvert(objectVal, targetClass, getType() + " Value");
        } catch (IllegalArgumentException e) {
            throw new TypeConversionException(e.getMessage(), e);
        }
    }

    /**
     * Converts the value to a generic object, if possible.
     * @return The value as an object
     * @throws TypeConversionException If the type doesn't support conversion to Object or the conversion failed
     * @see SqpCustom
     */
    public Object asObject() throws TypeConversionException {
        throw new TypeConversionException(getType(), Object.class);
    }

    /**
     * Converts the value to a String, if possible.
     * @return The value as String
     * @throws TypeConversionException If the type doesn't support conversion to String or the conversion failed
     * @see SqpChar
     * @see SqpVarChar
     * @see SqpXml
     */
    public String asString() throws TypeConversionException {
        throw new TypeConversionException(getType(), String.class);
    }

    /**
     * Converts the value to a boolean value, if possible.
     * @return The value as boolean
     * @throws TypeConversionException If the type doesn't support conversion to boolean or the conversion failed
     * @see SqpBoolean
     */
    public boolean asBoolean() throws TypeConversionException {
        throw new TypeConversionException(getType(), Boolean.class);
    }

    /**
     * Converts the value to a single byte, if possible.
     * @return The value as byte
     * @throws TypeConversionException If the type doesn't support conversion to byte or the conversion failed
     * @see SqpTinyInt
     */
    public byte asByte() throws TypeConversionException {
        throw new TypeConversionException(getType(), Byte.class);
    }

    /**
     * Converts the value to a short, if possible.
     * @return The value as short
     * @throws TypeConversionException If the type doesn't support conversion to short or the conversion failed
     * @see SqpSmallInt
     */
    public short asShort() throws TypeConversionException {
        throw new TypeConversionException(getType(), Short.class);
    }

    /**
     * Converts the value to a int, if possible.
     * @return The value as int
     * @throws TypeConversionException If the type doesn't support conversion to int or the conversion failed
     * @see SqpInteger
     */
    public int asInt() throws TypeConversionException {
        throw new TypeConversionException(getType(), Integer.class);
    }

    /**
     * Converts the value to a long, if possible.
     * @return The value as long
     * @throws TypeConversionException If the type doesn't support conversion to long or the conversion failed
     * @see SqpBigInt
     */
    public long asLong() throws TypeConversionException {
        throw new TypeConversionException(getType(), Long.class);
    }

    /**
     * Converts the value to a float, if possible.
     * @return The value as float
     * @throws TypeConversionException If the type doesn't support conversion to float or the conversion failed
     * @see SqpReal
     */
    public float asFloat() throws TypeConversionException {
        throw new TypeConversionException(getType(), Float.class);
    }

    /**
     * Converts the value to a double, if possible.
     * @return The value as double
     * @throws TypeConversionException If the type doesn't support conversion to double or the conversion failed
     * @see SqpDouble
     * @see SqpReal
     */
    public double asDouble() throws TypeConversionException {
        throw new TypeConversionException(getType(), Double.class);
    }

    /**
     * Converts the value to a BigDecimal, if possible.
     * @return The value as BigDecimal
     * @throws TypeConversionException If the type doesn't support conversion to BigDecimal or the conversion failed
     * @see SqpDecimal
     */
    public BigDecimal asBigDecimal() throws TypeConversionException {
        throw new TypeConversionException(getType(), BigDecimal.class);
    }

    /**
     * Converts the value to a byte array, if possible.
     * @return The value as byte array
     * @throws TypeConversionException If the type doesn't support conversion to byte array or the conversion failed
     * @see SqpBinary
     * @see SqpVarBinary
     * @see SqpAbstractLob
     * @see SqpBlob
     * @see SqpClob
     */
    public byte[] asBytes() throws TypeConversionException {
        throw new TypeConversionException(getType(), byte[].class);
    }

    /**
     * Converts the value to a LocalDate, if possible.
     * @return The value as LocalDate
     * @throws TypeConversionException If the type doesn't support conversion to LocalDate or the conversion failed
     * @see SqpDate
     * @see SqpTimestamp
     */
    public LocalDate asLocalDate() throws TypeConversionException {
        throw new TypeConversionException(getType(), LocalDate.class);
    }

    /**
     * Converts the value to a OffsetTime, if possible. The offset might be simply zero, if not set.
     * @return The value as OffsetTime
     * @throws TypeConversionException If the type doesn't support conversion to OffsetTime or the conversion failed
     * @see SqpTime
     * @see SqpTimestamp
     */
    public OffsetTime asOffsetTime() throws TypeConversionException {
        throw new TypeConversionException(getType(), OffsetTime.class);
    }

    /**
     * Converts the value to a OffsetDateTime, if possible. The offset might be simply zero, if not set.
     * @return The value as OffsetDateTime
     * @throws TypeConversionException If the type doesn't support conversion to OffsetDateTime or the conversion failed
     * @see SqpTimestamp
     */
    public OffsetDateTime asOffsetDateTime() throws TypeConversionException {
        throw new TypeConversionException(getType(), OffsetDateTime.class);
    }

    /**
     * Converts the value to a Duration, if possible.
     * @return The value as Duration
     * @throws TypeConversionException If the type doesn't support conversion to Duration or the conversion failed
     * @see SqpInterval
     */
    public Duration asDuration() throws TypeConversionException {
        throw new TypeConversionException(getType(), Duration.class);
    }

    /**
     * Can be used to test if the value represents a NULL value.
     * This should only be the case for {@link SqpNull}, but it can be used generically to test it, without the need
     * to check the value for its type first.
     *
     * @return True if the value is NULL, otherwise false.
     * @see SqpNull
     */
    public boolean isNull() {
        return false;
    }

    /**
     * Creates a new SqpValue with the given typ code from the provided value in JSON-compatible format.
     * All derived classes implement a static fromJsonFormatValue function. This function finds the
     * class associated to the type code, and calls the classes fromJsonFormatValue function to instantiate the correct
     * object from the provided JSON-compatible value.
     * <p>
     * Basically, this class is the counter part to {@link #getJsonFormatValue()}. So while {@link #getJsonFormatValue()}
     * can be used to serialize the value, this function can be used to reconstruct the value from deserialized data again.
     *
     * @param typeCode The type code of the value to construct
     * @param jsonFormatValue The JSON-compatible value
     * @return The created SQP value
     * @throws SqpException When creation of the desired value failed
     * @see #getJsonFormatValue()
     * @see SqpTypeCode
     */
    public static SqpValue createFromJsonFormat(SqpTypeCode typeCode, Object jsonFormatValue) throws SqpException {
        // NOTE: if reflection turns out to be too slow, we should use a switch-case here

        // null value is a special value
        if (jsonFormatValue == null) {
            return new SqpNull(typeCode);
        }

        if (typeCode == null || typeCode == SqpTypeCode.Unknown) {
            throw new TypeConversionException("Cannot instantiate a type unknown type code");
        }

        Class<? extends SqpValue> typeClass = typeCode.getTypeClass();
        try {
            Method factoryMethod = typeClass.getMethod("fromJsonFormatValue", Object.class);
            if (!SqpValue.class.isAssignableFrom(factoryMethod.getReturnType()) ||
                !Modifier.isStatic(factoryMethod.getModifiers())) {
                throw new NoSuchMethodException("No factory method 'static SqpValue fromJsonFormatValue(Object)' found");
            }
            return (SqpValue) factoryMethod.invoke(null, jsonFormatValue);
        } catch (ReflectiveOperationException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IllegalArgumentException) {
                throw new TypeConversionException(typeCode, cause);
            }
            throw new InternalException("Could not create SqpValue " + typeClass + " from JSON data. "
                    + "This is likely to be an implementation problem.", e);
        }
    }
}
