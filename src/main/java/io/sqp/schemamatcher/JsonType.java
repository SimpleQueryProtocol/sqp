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

package io.sqp.schemamatcher;

/**
 * @author Stefan Burnicki
 */
public enum JsonType {
    Boolean("boolean"),
    Number("number"),
    Integer("integer", Number),
    String("string"),
    Object("object"),
    Array("array"),
    Null("null");

    private String _identifier;
    private JsonType _compatibleType;

    JsonType(String identifier) {
        this(identifier, null);
    }

    JsonType(String identifier, JsonType compatibleType) {
        _identifier = identifier;
        _compatibleType = compatibleType;
    }

    public boolean matches(String otherIdentifier) {
        if (_identifier.equals(otherIdentifier.toLowerCase())) {
            return true;
        } else if (_compatibleType != null) {
            return _compatibleType.matches(otherIdentifier);
        }
        return false;
    }

    public static JsonType parse(String identifier) {
        for (JsonType type : JsonType.values()) {
            if (type.matches(identifier)) {
                return type;
            }
        }
        throw new InvalidSchemaException("Invalid type '" + identifier + "'");
    }
}
