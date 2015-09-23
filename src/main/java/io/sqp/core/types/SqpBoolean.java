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
 * The "Boolean" SQP type,  a simple boolean (truth) value
 * @author Stefan Burnicki
 */
public class SqpBoolean extends SqpValue {
    private boolean _value;

    /**
     * Creates the object by wrapping the value
     * @param value The truth value
     */
    public SqpBoolean(boolean value) {
        super(SqpTypeCode.Boolean);
        _value = value;
    }

    /**
     * Creates the object from deserialized data, which should be convertible to Boolean.
     * It uses the {@link TypeUtil#checkAndConvert(Object, Class, String)} method to do this.
     * @param value The deserialized value, e.g. a boolean
     * @return The created object
     * @throws IllegalArgumentException If the value couldn't be converted to Boolean.
     * @see TypeUtil#checkAndConvert(Object, Class, String)
     */
    public static SqpBoolean fromJsonFormatValue(Object value) throws IllegalArgumentException {
        return new SqpBoolean(TypeUtil.checkAndConvert(value, Boolean.class, "The JSON format value"));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns the boolean value itself.
     */
    @Override
    public Boolean getJsonFormatValue() {
        return _value;
    }
}
