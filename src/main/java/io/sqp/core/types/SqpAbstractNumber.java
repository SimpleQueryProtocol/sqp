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

import io.sqp.core.exceptions.TypeConversionException;
import io.sqp.core.util.TypeUtil;

import java.math.BigDecimal;

/**
 * Abstract base class for all number types.
 * @author Stefan Burnicki
 * @see SqpTinyInt
 * @see SqpSmallInt
 * @see SqpInteger
 * @see SqpBigInt
 * @see SqpDecimal
 * @see SqpDouble
 * @see SqpReal
 */
public abstract class SqpAbstractNumber<T extends Number> extends SqpValue {
    private T _value;

    /**
     * Constructor to be used by the derived types.
     * @param typeCode The type code of the actual type
     * @param value The numeric value
     */
    protected SqpAbstractNumber(SqpTypeCode typeCode, T value) {
        super(typeCode);
        _value = value;
    }

    /**
     * To internally get the value associated with this object
     * @return The associated numeric value
     */
    protected T getValue() {
        return _value;
    }

    /*
     * The following functions should be overridden by subtypes, to prevent unnecessary checks and conversion.
     * E.g. the SqpShort can simply return the value itself for all integer conversions, as the JVM will do the rest.
     * Note that the type checks perform range checks, so a cast from Long to int is possible, if the value is in range.
     */

    @Override
    public byte asByte() throws TypeConversionException {
        return convert(Byte.class);
    }

    @Override
    public short asShort() throws TypeConversionException {
        return convert(Short.class);
    }

    @Override
    public int asInt() throws TypeConversionException {
        return convert(Integer.class);
    }

    @Override
    public long asLong() throws TypeConversionException {
        return convert(Long.class);
    }

    @Override
    public float asFloat() throws TypeConversionException {
        return convert(Float.class);
    }

    @Override
    public double asDouble() throws TypeConversionException {
        return convert(Double.class);
    }

    @Override
    public BigDecimal asBigDecimal() throws TypeConversionException {
        return convert(BigDecimal.class);
    }

    private <U> U convert(Class<U> targetClass) throws TypeConversionException {
        try {
            // this auto-checks the range
            return TypeUtil.checkAndConvert(getValue(), targetClass, getType() + " Value");
        } catch (IllegalArgumentException e) {
            throw new TypeConversionException(e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * This will be the numeric value itself.
     */
    @Override
    public Object getJsonFormatValue() {
        return _value;
    }

    /**
     * Simple conversion of the numeric value to a String.
     * @return The String representing the numeric value.
     */
    @Override
    public String asString() {
        return _value.toString();
    }
}
