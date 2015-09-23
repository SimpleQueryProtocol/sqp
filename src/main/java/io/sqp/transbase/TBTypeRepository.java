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

package io.sqp.transbase;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.sqp.backend.TypeRepository;
import io.sqp.core.exceptions.BackendErrorException;
import transbase.tbx.TBConst;
import transbase.tbx.types.info.TBTypeInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Stefan Burnicki
 */
public class TBTypeRepository implements TypeRepository {
    private final static String JSON_SCHEMA = "http://json-schema.org/draft-04/schema#";
    private final static String DATETIME_SCHEMA_ID = "http://sqp.org/transbase-backend/datetime";
    private final static String TB_TYPE_PREFIX = "tb_";
    private final static String TB_DATETIME_NAME = "DATETIME";
    private Map<String, String> _cachedSchemas;
    private ObjectMapper _objectMapper;
    private List<String> _nativeTypes;

    public TBTypeRepository() {
        _cachedSchemas = new HashMap<>();
        _objectMapper = new ObjectMapper();
        initNativeTypesList();
    }

    private void initNativeTypesList() {
        DateRangeSpecifier[] specifiers = DateRangeSpecifier.values();
        int numSpecifiers = specifiers.length;
        _nativeTypes = new ArrayList<>(numSpecifiers * (numSpecifiers + 1) / 2); // triangular number
        for (int i = 0; i < numSpecifiers; i++) {
            for (int j = i; j < numSpecifiers; j++) {
                _nativeTypes.add(TB_TYPE_PREFIX + TB_DATETIME_NAME + createRangeSpecifier(specifiers[i], specifiers[j]));
            }
        }
    }

    @Override
    public List<String> getNativeTypes() {
        return _nativeTypes;
    }

    @Override
    public String getSchema(String typename) {
        switch (getCustomType(typename)) {
            case DateTime:
                return getDateTimeSchema(typename);
        }
        // currently no other custom types are supported
        return null;
    }

    public static CustomType getCustomType(String typename) {
        int prefixLength = TB_TYPE_PREFIX.length();
        if (!typename.substring(0, prefixLength).toLowerCase().equals(TB_TYPE_PREFIX)) {
            return CustomType.Unknown;
        }
        typename = typename.substring(prefixLength);
        if (typename.toUpperCase().startsWith(TB_DATETIME_NAME)) {
            return CustomType.DateTime;
        }
        return CustomType.Unknown;
    }

    public static String getTbTypeName(TBTypeInfo typeInfo) throws BackendErrorException {
        String origTypeName = typeInfo.getTypeName();
        if (typeInfo.getTypeCode() == TBConst.TB__DATETIME) {
            try {
                origTypeName += createRangeSpecifier(DateRangeSpecifier.fromTbValue(typeInfo.getHighField()),
                                                     DateRangeSpecifier.fromTbValue(typeInfo.getLowField()));
            } catch (IllegalArgumentException e) {
                throw new BackendErrorException("Invalid date range specifier from DBMS.", e);
            }
        }
        return TBTypeRepository.TB_TYPE_PREFIX + origTypeName;
    }

    public static DateRangeSpecifier[] parseRangeSpecifiers(String specifier) {
        if (specifier.toLowerCase().startsWith(TB_TYPE_PREFIX)) {
            specifier = specifier.substring(TB_TYPE_PREFIX.length());
        }
        if (specifier.toUpperCase().startsWith(TB_DATETIME_NAME)) {
            specifier = specifier.substring(TB_DATETIME_NAME.length());
        }
        if (!specifier.startsWith("[") || !specifier.endsWith("]")) {
            throw new IllegalArgumentException("Range specifier doesn't star with '[' and ends with ']'");
        }
        String[] specifiers = specifier.substring(1, specifier.length() - 1).split(":", 2);
        if (specifiers.length < 2) {
            throw new IllegalArgumentException("Range specifier doesn't contain two valid fields");
        }
        DateRangeSpecifier highField = DateRangeSpecifier.fromSpecifier(specifiers[0]);
        DateRangeSpecifier lowField = DateRangeSpecifier.fromSpecifier(specifiers[1]);
        return new DateRangeSpecifier[] {highField, lowField};
    }

    private String getDateTimeSchema(String specifier) {
        String cached = _cachedSchemas.get(specifier);
        if (cached != null) {
            return cached;
        }
        try {
            DateRangeSpecifier[] specifiers = parseRangeSpecifiers(specifier);
            cached = createDateTimeSchema(specifiers[0], specifiers[1]);
        } catch(IllegalArgumentException e) {
            return null; // TODO: send a warning?
        }
        _cachedSchemas.put(specifier, cached);
        return cached;
    }

    private String createDateTimeSchema(DateRangeSpecifier highField, DateRangeSpecifier lowField) {
        List<DateRangeSpecifier> fields = DateRangeSpecifier.range(highField, lowField);
        ObjectNode schema = createDateTimeSchemaSkeleton(highField, lowField);
        ArrayNode items = _objectMapper.createArrayNode();
        fields.forEach(f -> items.add(createTypeFromSpecifier(f)));
        schema.set("items", items);
        schema.put("minItems", fields.size());
        schema.put("additionalItems", false);
        return schema.toString();
    }

    private ObjectNode createTypeFromSpecifier(DateRangeSpecifier specifier) {
        return _objectMapper.createObjectNode()
                .put("type", "integer")
                .put("minimum", specifier.getMinValue())
                .put("maximum", specifier.getMaxValue())
                .put("id", specifier.getSpecifier())
                .put("title", specifier.toString());
    }

    private ObjectNode createDateTimeSchemaSkeleton(DateRangeSpecifier highField, DateRangeSpecifier lowField) {
        return _objectMapper.createObjectNode()
                .put("$schema", JSON_SCHEMA)
                .put("id", DATETIME_SCHEMA_ID)
                .put("type", "array")
                .put("title", TB_DATETIME_NAME + createRangeSpecifier(highField, lowField))
                .put("description", "Transbase " + TB_DATETIME_NAME + " value restricted to the range from " +
                        highField.toString() + " to " + lowField.toString());
    }

    private static String createRangeSpecifier(DateRangeSpecifier highField, DateRangeSpecifier lowField) {
        return "[" + highField.getSpecifier() + ":" + lowField.getSpecifier() + "]";
    }
}
