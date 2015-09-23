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
public class ItemsSchemaMatcher implements ItemsFieldMatcher {
    private SchemaMatcher _schemaMatcher;
    private int _maxItems;

    public ItemsSchemaMatcher(JsonNode schema, int maxItems) {
        _schemaMatcher = new SchemaMatcher(schema);
        _maxItems = maxItems;
    }

    @Override
    public boolean isCompatibleToSchema(JsonNode otherSchema) {
        return _schemaMatcher.isCompatibleTo(otherSchema);
    }

    @Override
    public boolean isCompatibleToSchemaArray(JsonNode schemaArray, AdditionalItemsField additional) {
        int arraySize = schemaArray.size();
        int checkMax = _maxItems < 0 ? arraySize : Math.min(_maxItems, arraySize);
        for (int i = 0; i < checkMax; i++) {
            if (!_schemaMatcher.isCompatibleTo(schemaArray.get(i))) {
                return false;
            }
        }

        // if the maximum is below or equal to the schemas we checked, or additionals aren't restricted this is just fine.
        if (additional.allowsEverything() || (_maxItems >= 0 && _maxItems <= arraySize)) {
            return true;
        }

        // otherwise it doesn't allow any or is a schema that needs to be compatible
        return additional.isSchema() && _schemaMatcher.isCompatibleTo(additional.getSchema());
    }
}
