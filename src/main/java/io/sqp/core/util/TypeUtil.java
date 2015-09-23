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

package io.sqp.core.util;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Utility that are Java type realted.
 */
public class TypeUtil {
    private static ObjectMapper _objectMapper = new ObjectMapper();
    private TypeUtil() {}

    /**
     * Checks a value to be not null and tries to map it to a given class.
     * The method first checks the value to be not-null and then if it's assignable to the desired class.
     * If it's not directly assignable (e.g. a int is not assignable to a Double), Jackson's object mapper
     * is used to convert the value, as it is able to map any classes if they are somehow compatible.
     * Therefore this method can also be used to map a {@literal List<Integer>} to an {@literal int[]}.
     * This method also does range checks, e.g. it fails if you try to convert 2^30 to a Short.
     * @param value The value to be checked and mapped.
     * @param clazz The class the value should be mapped to
     * @param what A brief description of the message that is used in a potential error message. E.g. "the identifier"
     * @param <T> The desired object type
     * @return The converted, non-null object.
     * @throws IllegalArgumentException If the value is null or cannot be converted to the desired type
     */
    public static <T> T checkAndConvert(Object value, Class<T> clazz, String what) {
        if (value == null) {
            throw new IllegalArgumentException(what + " is null.");
        }
        if (clazz.isAssignableFrom(value.getClass())) {
            return clazz.cast(value);
        }
        try {
            return _objectMapper.convertValue(value, clazz);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(what + " could not be mapped to type " + clazz.getName() + ": " + e.getMessage(), e);
        }
    }
}
