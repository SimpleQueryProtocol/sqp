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

import java.util.Arrays;

/**
 * @author Stefan Burnicki
 */
public class ItemsSchemaArrayMatcher implements ItemsFieldMatcher {
    private SchemaMatcher[] _schemaMatchers;
    private AdditionalItemsField _additionalField;

    public ItemsSchemaArrayMatcher(JsonNode schemaArray, AdditionalItemsField additionalField) {
        int numSchemas = schemaArray.size();
        _schemaMatchers = new SchemaMatcher[numSchemas];
        for (int i = 0; i < numSchemas; i++) {
            _schemaMatchers[i] = new SchemaMatcher(schemaArray.get(i));
        }
        _additionalField = additionalField;
    }

    public boolean isCompatibleToSchema(JsonNode otherSchema) {
        return Arrays.stream(_schemaMatchers).allMatch(sm -> sm.isCompatibleTo(otherSchema)) &&
               _additionalField.isCompatibleTo(otherSchema);
    }

    public boolean isCompatibleToSchemaArray(JsonNode schemaArray, AdditionalItemsField otherAdditional) {
        int numItems = _schemaMatchers.length;
        int otherNumItems = schemaArray.size();
        for (int i = 0; i < Math.min(numItems, otherNumItems); i++) {
            if (!_schemaMatchers[i].isCompatibleTo(schemaArray.get(i))) {
                return false;
            }
        }

        if (otherNumItems >= numItems) {
            return checkAdditionalItemsAreCompatible(schemaArray, otherAdditional);
        } else {
            return checkRemainingItemsAreCompatible(otherAdditional, otherNumItems);
        }
    }

    private boolean checkRemainingItemsAreCompatible(AdditionalItemsField otherAdditional, int numChecked) {
        // this schema defined more items than the matching schema. We need to check if the matching schema's additional
        // item field can handle them
        if (otherAdditional.allowsEverything()) {
            return true;
        } else if (otherAdditional.deniesEverything()) {
            return false;
        }
        for (int i = numChecked; i < _schemaMatchers.length; i++) {
            if (!_schemaMatchers[i].isCompatibleTo(otherAdditional.getSchema())) {
                return false;
            }
        }
        return _additionalField.isCompatibleTo(otherAdditional.getSchema());
    }

    private boolean checkAdditionalItemsAreCompatible(JsonNode schemaArray, AdditionalItemsField otherAdditional) {
        // the matching schema defined more items than this schema. we need to check if we have an additional items
        // field defined, and if it is compatible with the matching schema
        for (int i = _schemaMatchers.length; i < schemaArray.size(); i++) {
            if (!_additionalField.isCompatibleTo(schemaArray.get(i))) {
                return false;
            }
        }
        // now just check if the additionalFields are compatible to each other
        return _additionalField.isCompatibleTo(otherAdditional);
    }
}
