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

import io.sqp.core.util.TypeUtil;

/**
 * The SQP "Double" type, a 8 byte IEEE 754 floating point number
 * @author Stefan Burnicki
 * @see SqpReal
 * @see SqpDecimal
 */
public class SqpDouble extends SqpAbstractNumber<Double> {

    /**
     * Constructs the object by wrapping a double
     * @param value The double to wrap
     */
    public SqpDouble(double value) {
        super(SqpTypeCode.Double, value);
    }

    /**
     * Returns the double wrapped by this instace
     * @return The double value
     */
    @Override
    public double asDouble() {
        return getValue();
    }

    /**
     * Creates the object from a Double-compatible value or by parsing a String.
     * @param value The value that should be converted to Double
     * @return The new instance
     * @throws IllegalArgumentException If the value couldn't be parsed or converted to Double
     */
    public static SqpDouble fromJsonFormatValue(Object value) throws IllegalArgumentException {
        if (value instanceof String) {
            return new SqpDouble(Double.parseDouble((String) value));
        }
        return new SqpDouble(TypeUtil.checkAndConvert(value, Double.class, "The Double value"));
    }
}
