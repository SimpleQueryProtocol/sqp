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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.sqp.schemamatcher.typematchers.TypeMatcher;

import java.io.IOException;

/**
 * @author Stefan Burnicki
 */
public class SchemaMatcher implements Matcher {
    private ObjectMapper _mapper;
    private TypeMatcher _matcher;

    public SchemaMatcher(String schema) throws IOException {
        _mapper = new ObjectMapper();
        init(_mapper.readTree(schema));
    }

    public SchemaMatcher(JsonNode schema) {
        _mapper = new ObjectMapper();
        init(schema);
    }

    private void init(JsonNode schema) {
        if (schema == null) {
            throw new InvalidSchemaException("The schema is null.");
        }
        if (!schema.isObject()) {
            throw new InvalidSchemaException("The schema is not an object");
        }
        _matcher = TypeMatcher.createForSchema(schema);
    }

    public boolean isCompatibleTo(String schema) {
        try {
            return isCompatibleTo(_mapper.readTree(schema));
        } catch (IOException e) {
            throw new InvalidMatchingSchemaException(e.getMessage());
        }
    }

    @Override
    public boolean isCompatibleTo(JsonNode node) {
        if (node == null) {
            throw new InvalidMatchingSchemaException("The schema to match is empty.");
        }
        return _matcher.isCompatibleTo(node);
    }
}
