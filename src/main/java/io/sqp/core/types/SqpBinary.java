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

import java.util.Base64;

/**
 * The "Binary" SQP type, a fixed number of bytes.
 * The difference to {@link SqpVarBinary} is only a semantic difference (fixed number instead of variable number of bytes).
 * @author Stefan Burnicki
 * @see SqpVarBinary
 */
public class SqpBinary extends SqpValue {
    private byte[] _value;

    /**
     * Creates the object by wrapping a byte array.
     * @param value The byte array to wrap
     */
    public SqpBinary(byte[] value) {
        super(SqpTypeCode.Binary);
        // TODO: make length configurable
        _value = value;
    }

    /**
     * Returns the wrapped byte array
     * @return The wrapped byte array
     */
    @Override
    public byte[] asBytes() {
        return _value;
    }

    /**
     * {@inheritDoc}
     * <p>
     * The value will be the byte array itself.
     */
    @Override
    public byte[] getJsonFormatValue() {
        return _value;
    }

    /**
     * Creates the object from deserialized data, which is either a base64 string, or a byte[]
     * @param value The base64 string or byte[]
     * @return The created object
     */
    public static SqpBinary fromJsonFormatValue(Object value) {
        if (value instanceof byte[]) {
            return new SqpBinary((byte[]) value);
        }
        String encoded = TypeUtil.checkAndConvert(value, String.class, "Base64 encoded value");
        return new SqpBinary(Base64.getDecoder().decode(encoded));
    }
}
