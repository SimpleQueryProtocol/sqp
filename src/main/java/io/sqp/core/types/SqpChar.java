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
 * The "Char" SQP type,  a fixed number of characters.
 * It only differs from {@link SqpVarChar} and {@link SqpXml} by its semantic means.
 * @see SqpVarChar
 * @see SqpXml
 * @author Stefan Burnicki
 */
public class SqpChar extends SqpAbstractText {

    /**
     * Constructs the object from deserialized data, which should simply be convertible to text
     * @param value The deserialized data
     * @return The created object
     */
    public static SqpChar fromJsonFormatValue(Object value) {
        return new SqpChar(TypeUtil.checkAndConvert(value, String.class, "The value"));
    }

    /**
     * Constructs the object by wrapping a String
     * @param value The characters to be represented
     */
    public SqpChar(String value) {
        super(SqpTypeCode.Char, value);
    }
}
