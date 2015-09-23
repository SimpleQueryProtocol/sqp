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
 * The "BigInt" SQP type, a 8 byte signed integer.
 * @author Stefan Burnicki
 */
public class SqpBigInt extends SqpAbstractNumber<Long> {

    /**
     * Constructs the object by wrapping a long value
     * @param value The value to wrap
     */
    public SqpBigInt(long value) {
        super(SqpTypeCode.BigInt, value);
    }

    /**
     * Simply returns the underlying long value
     * @return The value
     */
    @Override
    public long asLong() {
        return getValue();
    }


    /**
     * Factory method used for deserialization.
     * Simply tries to cast or convert the value to a Long.
     * @param value The value to convert
     * @return The created object
     * @throws  IllegalArgumentException If the value couldn't be converted to a Long
     * @see TypeUtil#checkAndConvert(Object, Class, String)
     */
    public static SqpBigInt fromJsonFormatValue(Object value) throws IllegalArgumentException {
        return new SqpBigInt(TypeUtil.checkAndConvert(value, Long.class, "The BigInt value"));
    }
}
