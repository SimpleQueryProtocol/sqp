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

package io.sqp.proxy;

import io.sqp.core.exceptions.TypeConversionException;
import io.sqp.core.types.*;
import io.sqp.proxy.customtypes.CustomType;
import io.sqp.proxy.customtypes.CustomTypeMapper;
import io.sqp.proxy.customtypes.TypeValidator;
import io.sqp.proxy.exceptions.ValidationFailedException;
import io.sqp.core.exceptions.SqpException;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Stefan Burnicki
 */
public class ParameterDecoder {
    private List<SqpTypeCode> _types;
    private CustomType[] _customTypes;
    private CustomTypeMapper _customTypeMapper;
    private Map<String, InputStream> _lobs;

    public ParameterDecoder(CustomTypeMapper customTypeMapper, List<SqpTypeCode> types, List<String> customTypes, Map<String, InputStream> lobs) throws TypeConversionException {
        _types = types;
        _customTypeMapper = customTypeMapper;
        _lobs = lobs;
        initCustomTypes(customTypes);
    }

    private void initCustomTypes(List<String> customTypes) throws TypeConversionException {
        if (customTypes.size() < 1) {
            _customTypes = null;
            return;
        }
        _customTypes = new CustomType[_types.size()];
        int numCustomTypes = customTypes.size();
        int numSetCustomTypes = 0;
        for (int i = 0; i < _types.size(); i++) {
            if (_types.get(i) != SqpTypeCode.Custom) {
                continue;
            }
            if (numSetCustomTypes >= numCustomTypes) {
                throw new TypeConversionException("There are less customTypes defined than used in parameterTypes!");
            }
            _customTypes[i] = initCustomType(customTypes.get(numSetCustomTypes));
            numSetCustomTypes++;
        }
        if (numSetCustomTypes < numCustomTypes) {
            throw new TypeConversionException("There are more customTypes defined than used in parameterTypes.");
        }
    }

    private CustomType initCustomType(String name) {
        CustomType mappedType = _customTypeMapper.getMapping(name);
        if (mappedType != null) {
            return mappedType;
        }
        // otherwise we assume that this type is a native backend type
        return new CustomType(name, TypeValidator.AlwaysValidate);
    }

    public List<List<SqpValue>> decodeParameterListBatch(List<List<Object>> valueListBatch) throws SqpException {
        List<List<SqpValue>> decodedBatch = new ArrayList<>(valueListBatch.size());
        for (List<Object> valueList : valueListBatch) {
            decodedBatch.add(decodeParameterList(valueList));
        }
        return decodedBatch;
    }

    public List<SqpValue> decodeParameterList(List<Object> values) throws SqpException {
        ArrayList<SqpValue> decodedValues = new ArrayList<>(_types.size());
        int numValues = values.size();
        for (int i = 0; i < _types.size(); i++) {
            Object value = i < numValues ? values.get(i) : null;
            decodedValues.add(decodeParameter(i, value));
        }
        return decodedValues;
    }

    private SqpValue decodeParameter(int i, Object value) throws SqpException {
        SqpTypeCode type = _types.get(i);
        if (type == SqpTypeCode.Custom) {
            return decodeCustomType(_customTypes[i], value);
        } else if (value == null) {
            return new SqpNull(type);
        }
        SqpValue sqpValue = SqpValue.createFromJsonFormat(type, value);
        if (sqpValue instanceof SqpAbstractLob) {
            sqpValue = associateLobStream((SqpAbstractLob) sqpValue);
        }
        return sqpValue;
    }

    private SqpAbstractLob associateLobStream(SqpAbstractLob sqpLob) throws TypeConversionException {
        String id = sqpLob.getId();
        InputStream stream = _lobs.get(id);
        if (stream == null) {
            throw new TypeConversionException("There is no data for " + sqpLob.getType() + " with id " + id);
        }
        return sqpLob.createWithStream(stream);
    }

    private SqpCustom decodeCustomType(CustomType customType, Object value) throws ValidationFailedException {
        customType.validate(value);
        return new SqpCustom(value, customType.getName());
    }
}
