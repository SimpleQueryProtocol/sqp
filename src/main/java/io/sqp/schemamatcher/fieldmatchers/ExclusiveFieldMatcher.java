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
import io.sqp.schemamatcher.NumberUtils;

/**
 * @author Stefan Burnicki
 */
public abstract class ExclusiveFieldMatcher extends NumberFieldMatcher {
    private String _exclusiveFieldName;
    private boolean _exclusive;

    public ExclusiveFieldMatcher(String fieldName, String exclusiveFieldName, JsonNode schema) {
        super(fieldName, schema);
        _exclusiveFieldName = exclusiveFieldName;
        JsonNode exclusiveField = schema.get(_exclusiveFieldName);
        _exclusive = exclusiveField != null && exclusiveField.asBoolean();
    }

    @Override
    final protected boolean otherDoubleIsLessRestrictive(Double fieldValue, Double otherValue) {
        if (NumberUtils.roughlyEqual(fieldValue, otherValue)) {
            return checkExclusive();
        }
        return checkLessRestrictive(fieldValue, otherValue);
    }

    protected abstract boolean checkLessRestrictive(Double fieldValue, Double otherValue);

    private boolean checkExclusive() {
        if (_exclusive) {
            return true;
        }
        JsonNode otherExclusiveField = currentlyMatchingSchema().get(_exclusiveFieldName);
        return otherExclusiveField == null || !otherExclusiveField.asBoolean();
    }
}
