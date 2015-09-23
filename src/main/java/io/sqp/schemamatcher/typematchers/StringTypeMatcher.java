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
import io.sqp.schemamatcher.JsonType;
import io.sqp.schemamatcher.fieldmatchers.FieldMatcher;
import io.sqp.schemamatcher.fieldmatchers.MaxFieldMatcher;
import io.sqp.schemamatcher.fieldmatchers.MinFieldMatcher;
import io.sqp.schemamatcher.fieldmatchers.StringFieldMatcher;

/**
 * @author Stefan Burnicki
 */
public class StringTypeMatcher extends NonTrivialTypeMatcher {
    private static final String MAX_LENGTH = "maxLength";
    private static final String MIN_LENGTH = "minLength";
    private static final String PATTERN = "pattern";

    private FieldMatcher[] _fieldMatchers;

    public StringTypeMatcher(JsonNode schema) {
        super(JsonType.String, schema);
        _fieldMatchers = new FieldMatcher[]{
                new MaxFieldMatcher(MAX_LENGTH, schema),
                new MinFieldMatcher(MIN_LENGTH, schema),
                new StringFieldMatcher(PATTERN, schema)
        };
    }

    @Override
    protected boolean checkFields(JsonNode other) {
        return FieldMatcher.allMatch(_fieldMatchers, other);
    }
}
