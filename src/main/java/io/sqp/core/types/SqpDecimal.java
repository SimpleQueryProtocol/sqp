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

import java.math.BigDecimal;

/**
 * The "Decimal" SQP type, a decimal number with arbitrary, but exact, precision and scale.
 * @author Stefan Burnicki
 * @see SqpDouble
 * @see SqpReal
 */
public class SqpDecimal extends SqpAbstractNumber<BigDecimal> {

    /**
     * Constructs the object from a String. The string is allowed to use an exponent in the scientific notation.
     * @param value The string representing the decimal
     */
    public SqpDecimal(String value) throws TypeConversionException {
        this(convertToBigDecimal(value));
    }

    /**
     * Constructs the value from a BigDecimal
     * @param value The decimal value
     */
    public SqpDecimal(BigDecimal value) {
        super(SqpTypeCode.Decimal, value);
    }

    /**
     * {@inheritDoc}
     * <p>
     * The concrete format will simply be a string in scientific notation (with "E" exponent).
     * @return The decimal as a string with scientific notation
     */
    @Override
    public Object getJsonFormatValue() {
        return getValue().toString();
    }

    /**
     * Returns the value as BigDecimal
     * @return The value as BigDecimal
     */
    @Override
    public BigDecimal asBigDecimal() throws TypeConversionException {
        return getValue();
    }

    /**
     * Constructs the object from a deserialized value, which should simply be a String with the scientific notation.
     * @param jsonFormatObject The value which will be converted to String
     * @return The created object
     * @throws TypeConversionException If the value, converted to String, is not a valid decimal representation
     */
    public static SqpDecimal fromJsonFormatValue(Object jsonFormatObject) throws TypeConversionException {
        return new SqpDecimal(jsonFormatObject.toString());
    }

    private static BigDecimal convertToBigDecimal(String value) throws TypeConversionException {
        try {
            return new BigDecimal(value);
        } catch (NumberFormatException e) {
            throw new TypeConversionException("Invalid decimal representation: " + e.getMessage(), e);
        }
    }

}
