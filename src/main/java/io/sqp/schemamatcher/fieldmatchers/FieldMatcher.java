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

package io.sqp.schemamatcher.fieldmatchers;

import com.fasterxml.jackson.databind.JsonNode;
import io.sqp.schemamatcher.Matcher;

import java.util.Arrays;

/**
 * @author Stefan Burnicki
 */
public abstract class FieldMatcher implements Matcher {
    private String _fieldName;
    private JsonNode _field;
    private JsonNode _otherSchema;

    public FieldMatcher(String fieldName, JsonNode schema) {
        _field = schema.get(fieldName);
        _fieldName = fieldName;
    }

    public static boolean allMatch(FieldMatcher[] fieldMatchers, JsonNode other) {
        return Arrays.stream(fieldMatchers).allMatch(m -> m.isCompatibleTo(other));
    }

    protected JsonNode currentlyMatchingSchema() {
        return _otherSchema;
    }

    @Override
    final public boolean isCompatibleTo(JsonNode otherSchema) {
        _otherSchema = otherSchema;
        JsonNode otherField = otherSchema.get(_fieldName);
        if (otherField == null) {
            return true;
        }
        return _field != null && otherFieldIsLessRestrictive(_field, otherField);
    }

    abstract protected boolean otherFieldIsLessRestrictive(JsonNode field, JsonNode otherField);
}
