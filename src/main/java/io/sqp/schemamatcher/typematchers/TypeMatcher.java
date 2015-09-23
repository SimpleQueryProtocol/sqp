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

package io.sqp.schemamatcher.typematchers;

import com.fasterxml.jackson.databind.JsonNode;
import io.sqp.schemamatcher.InvalidMatchingSchemaException;
import io.sqp.schemamatcher.InvalidSchemaException;
import io.sqp.schemamatcher.Matcher;
import io.sqp.schemamatcher.JsonType;

/**
 * @author Stefan Burnicki
 */
public abstract class TypeMatcher implements Matcher {
    private JsonType _type;

    protected TypeMatcher(JsonType type) {
        _type = type;
    }

    @Override
    final public boolean isCompatibleTo(JsonNode other) {
        JsonNode typeField = other.get("type");
        if (typeField == null) {
            throw new InvalidMatchingSchemaException("Type field is not set. Expected type '" + _type + "'.");
        }
        return _type.matches(typeField.asText()) && isTypeCompatibleTo(other);
    }

    abstract protected boolean isTypeCompatibleTo(JsonNode other);

    public static TypeMatcher createForSchema(JsonNode schema) {
        JsonNode typeField = schema.get("type");
        if (typeField == null) {
            throw new InvalidSchemaException("The schema doesn't contain a type field");
        }
        JsonType type = JsonType.parse(typeField.asText());
        switch (type) {
            case Integer:
            case Number:
                return new NumberTypeMatcher(type, schema);
            case String:
                return new StringTypeMatcher(schema);
            case Object:
                return new ObjectTypeMatcher(schema);
            case Array:
                return new ArrayTypeMatcher(schema);
            default:
                return new TrivialTypeMatcher(type);
        }
    }
}
