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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.sqp.core.types.SqpDecimal;
import io.sqp.core.types.SqpTypeCode;

/**
 * Represents a type information with a specific precision and scale.
 * Types, like {@link SqpDecimal} might have specific precision and scale when used as an attribute
 * in a database.
 * This class can be used for encoding/decoding via Jackson.
 * @see ColumnMetadata
 */
public class TypeDescription {
    SqpTypeCode _typeCode;
    int _precision;
    int _scale;

    /**
     * Creates a type description.
     * @param typeCode The type code
     * @param precision The precision
     * @param scale The scale
     */
    public TypeDescription(SqpTypeCode typeCode, int precision, int scale) {
        _typeCode = typeCode;
        _precision = precision;
        _scale = scale;
    }

    /**
     * Returns the type code of the represented SQP type
     * @return The type code
     */
    @JsonProperty("type")
    public SqpTypeCode getTypeCode() {
        return _typeCode;
    }

    /**
     * Return the associated precision
     * @return Associated precision
     */
    public int getPrecision() {
        return _precision;
    }

    /**
     * Returns the associated scale
     * @return Associated scale
     */
    public int getScale() {
        return _scale;
    }
}
