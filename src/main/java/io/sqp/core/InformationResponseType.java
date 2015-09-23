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

package io.sqp.core;

import io.sqp.core.messages.InformationRequestMessage;
import io.sqp.core.messages.InformationResponseMessage;

/**
 * Defines the value type of an information response message.
 * @author Stefan Burnicki
 * @see InformationResponseMessage
 * @see InformationRequestMessage
 */
public enum InformationResponseType {
    /**
     * The response value is text (a String).
     */
    Text(String.class),

    /**
     * The response value is a boolean/truth value.
     */
    Boolean(Boolean.class),

    /**
     * The response value is a JSON Schema (as String).
     */
    Schema(String.class),

    /**
     * The response value is an Integer.
     */
    Integer(Integer.class),

    /**
     * The response value is unknown.
     */
    Unknown(Object.class),

    /**
     * The response value is an array of strings.
     */
    TextArray(String[].class);

    private Class<?> _underlyingType;

    InformationResponseType(Class<?> underlyingType) {
        _underlyingType = underlyingType;
    }

    /**
     * Checks if a response type is compatible to a Java type, so it can get casted to it.
     * For example "Text" and "Schema" are both compatible to String.
     * @param javaType The java type the response type might be compatible to
     * @return Truth if the a value of the response type can be assigned to the Java type, false otherwise.
     */
    public boolean isCompatibleTo(Class<?> javaType) {
        return javaType.isAssignableFrom(_underlyingType);
    }
}
