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

/**
 * The "Custom" SQP type, which is used for any non-standard data.
 * SQP allows to define custom data types and send native database types. To fit them into the standard data model,
 * this type is used to provide information about the data type and hold the custom object.
 * <p>
 * When being serialized and deserialized to/from JSON-like data, only the value gets send to reduce the overhead.
 * As a consequence, the custom value should JSON-serializable, i.e. a Map, a object with getters, a list, array, a
 * primitive value, or a combination of them.
 * The name of the custom value's type, which can also be represented by this type, always needs to be send separately.
 *
 * @author Stefan Burnicki
 */
public class SqpCustom extends SqpValue {
    private Object _value;
    private String _customTypeName;

    /**
     * Constructs a custom value wrapper without knowing the name of the custom type.
     * @param value The custom value, which might be anything
     */
    public SqpCustom(Object value) {
        this(value, "");
    }

    /**
     * Constructs a custom value wrapper with an associated name of the custom type.
     * @param value The custom value, which might be anything
     * @param customTypeName The name of the custom type
     */
    public SqpCustom(Object value, String customTypeName) {
        super(SqpTypeCode.Custom);
        _value = value;
        _customTypeName = customTypeName;
    }

    /**
     * Gets the name of the custom type, if specified when the object was constructed.
     * @return The name of the wrapped value's type or an empty string, if none is specified
     */
    public String getCustomTypeName() {
        return _customTypeName;
    }

    /**
     * Gets the custom value wrapped by this instance as passed in the constructor.
     * @return The wrapped custom value
     */
    public Object getValue() {
        return _value;
    }

    /**
     * Simply passes the jsonFormatValue to the constructor as the custom value.
     * @param jsonFormatValue The custom value
     * @return The created object
     */
    public static SqpCustom fromJsonFormatValue(Object jsonFormatValue) {
        return new SqpCustom(jsonFormatValue);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Simply returns the custom value, which therefore must be a JSON-serializable structure.
     */
    @Override
    public Object getJsonFormatValue() {
        return _value;
    }

    /**
     * Returns the custom value wrapped by this instance.
     * @return The custom value
     */
    @Override
    public Object asObject() {
        return _value;
    }
}
