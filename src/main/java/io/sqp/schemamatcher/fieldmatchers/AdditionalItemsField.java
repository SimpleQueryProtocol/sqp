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
import io.sqp.schemamatcher.SchemaMatcher;

/**
 * @author Stefan Burnicki
 */
public class AdditionalItemsField {
    private JsonNode _field;
    private boolean _allowsAnyMore;
    private boolean _isSchema;
    private boolean _deniesEverything;
    private SchemaMatcher _schemaMatcher;

    public AdditionalItemsField(JsonNode field) {
        _field = field;
        _isSchema = field != null && field.isObject();
        _allowsAnyMore = field == null || field.asBoolean(false);
        _deniesEverything = field != null && !field.asBoolean(true);
    }

    public JsonNode getSchema() {
        return _field;
    }

    public boolean allowsEverything() {
        return _allowsAnyMore;
    }

    public boolean deniesEverything() { return _deniesEverything; }

    public boolean isSchema() {
        return _isSchema;
    }

    public boolean isCompatibleTo(JsonNode schema) {
        if (schema == null) {
            return true;
        }
        if (!isSchema()) {
            return !allowsEverything(); // inverted as "anything" is not compatible to a schema
        }
        if (_schemaMatcher == null) {
            _schemaMatcher = new SchemaMatcher(_field);
        }
        return _schemaMatcher.isCompatibleTo(schema);
    }

    public boolean isCompatibleTo(AdditionalItemsField otherAdditional) {
        if (deniesEverything() || otherAdditional.allowsEverything()) {
            return true;
        }
        // the other schema has restrictions. So fail if we don't or the other is full-restrictive
        if (allowsEverything() || otherAdditional.deniesEverything()) {
            return false;
        }
        // otherwise both fields are schemas, so we check compatibility
        return isCompatibleTo(otherAdditional.getSchema());
    }
}
