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
import io.sqp.schemamatcher.fieldmatchers.StringFieldMatcher;

/**
 * @author Stefan Burnicki
 */
public abstract class NonTrivialTypeMatcher extends TypeMatcher {
    private final static String FORMAT = "format";

    private FieldMatcher _formatFieldMatcher;

    public NonTrivialTypeMatcher(JsonType type, JsonNode schema) {
        super(type);
        _formatFieldMatcher = new StringFieldMatcher(FORMAT, schema);
    }

    @Override
    final protected boolean isTypeCompatibleTo(JsonNode other) {
        return checkCommonFields(other) && checkFields(other);
    }

    private boolean checkCommonFields(JsonNode other) {
        return _formatFieldMatcher == null ||
                _formatFieldMatcher.isCompatibleTo(other);
    }

    abstract protected boolean checkFields(JsonNode node);

}
