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
import io.sqp.schemamatcher.fieldmatchers.*;
import io.sqp.schemamatcher.JsonType;

/**
 * @author Stefan Burnicki
 */
public class ArrayTypeMatcher extends NonTrivialTypeMatcher {
    private static final String INVALID_ITEMS_FIELD = "'items' field is neither an object, nor an array";
    private static final String MAX_ITEMS = "maxItems";
    private static final String MIN_ITEMS = "minItems";
    private static final String UNIQUE_ITEMS = "uniqueItems";
    private static final String ITEMS = "items";
    private static final String ADDITIONAL_ITEMS = "additionalItems";

    private FieldMatcher[] _fieldMatchers;
    private ItemsFieldMatcher _itemsFieldMatcher;

    public ArrayTypeMatcher(JsonNode schema) {
        super(JsonType.Array, schema);
        _fieldMatchers = new FieldMatcher[] {
            new MaxFieldMatcher(MAX_ITEMS, schema),
            new MinFieldMatcher(MIN_ITEMS, schema),
            new BooleanFieldMatcher(UNIQUE_ITEMS, schema)
        };
        JsonNode itemsField = schema.get(ITEMS);
        if (itemsField == null) {
            // never is compatible. Will only be used if other field isn't also null
            _itemsFieldMatcher = new NegativeItemsFieldMatcher();
        } else if (itemsField.isArray()) {
            AdditionalItemsField additionalItemsField = new AdditionalItemsField(schema.get(ADDITIONAL_ITEMS));
            _itemsFieldMatcher = new ItemsSchemaArrayMatcher(itemsField, additionalItemsField);
        } else if (itemsField.isObject()) {
            JsonNode maxItemsField = schema.get(MAX_ITEMS);
            int maxItems = maxItemsField == null ? -1 : maxItemsField.asInt();
            _itemsFieldMatcher = new ItemsSchemaMatcher(itemsField, maxItems);
        }
    }

    @Override
    protected boolean checkFields(JsonNode other) {
        return FieldMatcher.allMatch(_fieldMatchers, other) && checkItems(other);
    }

    private boolean checkItems(JsonNode other) {
        JsonNode otherItems = other.get(ITEMS);
        if (otherItems == null) { // everything is permitted
            return true;
        }
        if (otherItems.isObject()) {
            return _itemsFieldMatcher.isCompatibleToSchema(otherItems);
        } else if (otherItems.isArray()) {
            AdditionalItemsField additional = new AdditionalItemsField(other.get(ADDITIONAL_ITEMS));
            return _itemsFieldMatcher.isCompatibleToSchemaArray(otherItems, additional);
        } else {
            throw new InvalidMatchingSchemaException(INVALID_ITEMS_FIELD);
        }
    }

    class NegativeItemsFieldMatcher implements ItemsFieldMatcher {
        @Override
        public boolean isCompatibleToSchema(JsonNode otherSchema) {
            return false;
        }
        @Override
        public boolean isCompatibleToSchemaArray(JsonNode schemaArray, AdditionalItemsField otherAdditional) {
            return false;
        }
    }

}
