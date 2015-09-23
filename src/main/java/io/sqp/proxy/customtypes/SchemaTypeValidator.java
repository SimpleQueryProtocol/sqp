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

package io.sqp.proxy.customtypes;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import io.sqp.proxy.exceptions.TypeMappingNotPossibleException;
import io.sqp.proxy.exceptions.ValidationFailedException;

/**
 * @author Stefan Burnicki
 */
public class SchemaTypeValidator implements TypeValidator {
    private static ObjectMapper _objectMapper = new ObjectMapper();
    private static JsonSchemaFactory _schemaFactory;
    private JsonSchema _schema;

    private SchemaTypeValidator(JsonSchema schema) {
        _schema = schema;
    }

    @Override
    public void validate(Object value) throws ValidationFailedException {
        JsonNode valueNode = _objectMapper.valueToTree(value);
        try {
            ProcessingReport report = _schema.validate(valueNode);
            if (!report.isSuccess()) {
                throw new ValidationFailedException("Validation of data failed: " + report.toString());
            }
        } catch (ProcessingException e) {
            throw new ValidationFailedException("Validation of data failed: " + e.getMessage());
        }
    }

    public static SchemaTypeValidator create(JsonNode schema) throws TypeMappingNotPossibleException {
        if (_schemaFactory == null) {
            _schemaFactory = JsonSchemaFactory.byDefault();
        }
        try {
            return new SchemaTypeValidator(_schemaFactory.getJsonSchema(schema));
        } catch (ProcessingException e) {
            throw new TypeMappingNotPossibleException("Cannot instantiate a validator with schema: " + e.getShortMessage());
        }
    }
}
