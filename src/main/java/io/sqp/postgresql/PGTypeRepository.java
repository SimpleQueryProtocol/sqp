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

package io.sqp.postgresql;

import io.sqp.backend.TypeRepository;
import io.sqp.core.exceptions.SqpException;
import io.sqp.core.types.SqpTypeCode;
import io.sqp.proxy.util.ResourceUtil;

import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Stefan Burnicki
 */
public class PGTypeRepository implements TypeRepository {
    private Logger _logger;
    private List<String> _nativeTypes;

    public PGTypeRepository(Logger logger) {
        _logger = logger;
        // TODO: get this information from somewhere
        _nativeTypes = Collections.singletonList("pg_point");
    }

    // TODO: support caching
    @Override
    public String getSchema(String typename) {
        int oid = TypeInfo.getOidFromTypeName(typename);
        if (oid < 0) {
            return null;
        }
        SqpTypeCode typeCode = TypeInfo.getSupportedSqpTypeCode(oid);
        // we only support schemas for types that may be mapped as a custom type
        if (typeCode != SqpTypeCode.Custom) {
            return null;
        }
        String internalName = TypeInfo.getInternalTypeName(oid);
        String resourceName = "postgres-backend/schemas/" + internalName + ".json";
        try {
            return ResourceUtil.readResourceAsString(resourceName, _logger);
        } catch (SqpException e) {
            _logger.log(Level.WARNING, "Error loading schema resource for type " + internalName);
            return null;
        }
    }

    @Override
    public List<String> getNativeTypes() {
        return _nativeTypes;
    }
}
