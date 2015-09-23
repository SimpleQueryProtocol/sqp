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
import io.sqp.schemamatcher.fieldmatchers.ExclusiveMaxFieldMatcher;
import io.sqp.schemamatcher.fieldmatchers.ExclusiveMinFieldMatcher;
import io.sqp.schemamatcher.JsonType;
import io.sqp.schemamatcher.fieldmatchers.FieldMatcher;
import io.sqp.schemamatcher.fieldmatchers.MultipleOfMatcher;

/**
 * @author Stefan Burnicki
 */
public class NumberTypeMatcher extends NonTrivialTypeMatcher {
    private static final String MULTIPLE_OF = "multipleOf";
    private static final String MAXIMUM = "maximum";
    private static final String MINIMUM = "minimum";
    private static final String EXCLUSIVE_MAXIMUM = "exclusiveMaximum";
    private static final String EXCLUSIVE_MINIMUM = "exclusiveMinimum";

    private FieldMatcher[] _fieldMatchers;

    public NumberTypeMatcher(JsonType type, JsonNode schema) {
        super(type, schema);
        _fieldMatchers = new FieldMatcher[]{
                new MultipleOfMatcher(MULTIPLE_OF, schema),
                new ExclusiveMaxFieldMatcher(MAXIMUM, EXCLUSIVE_MAXIMUM, schema),
                new ExclusiveMinFieldMatcher(MINIMUM, EXCLUSIVE_MINIMUM, schema)
        };
    }

    @Override
    protected boolean checkFields(JsonNode other) {
        return FieldMatcher.allMatch(_fieldMatchers, other);
    }

}
