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

import io.sqp.backend.results.InformationRequestResult;
import io.sqp.core.ErrorType;
import io.sqp.core.InformationSubject;
import io.sqp.backend.TypeRepository;
import io.sqp.core.ErrorAction;
import io.sqp.core.InformationResponseType;
import io.sqp.core.exceptions.SqpException;
import io.sqp.core.types.SqpTypeCode;
import io.sqp.proxy.util.ResourceUtil;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Stefan Burnicki
 */
public class InformationProvider {
    Logger _logger;
    TypeRepository _backendTypeRepo;

    public InformationProvider(Logger logger, TypeRepository backendTypeRepo) {
        _logger = logger;
        _backendTypeRepo = backendTypeRepo;
    }

    public InformationRequestResult get(InformationSubject subject, String detail) throws SqpException {
        // NOTE: we would have the chance to block specific information requests here
        switch (subject) {
            case SupportsBinaryProtocol:
                return new InformationRequestResult(InformationResponseType.Boolean, true);

            case TypeSchema:
                return getTypeSchema(detail);

            case SupportedNativeTypes:
                String[] nativeTypes = _backendTypeRepo.getNativeTypes().stream().toArray(String[]::new);
                return new InformationRequestResult(InformationResponseType.TextArray, nativeTypes);
        }
        return InformationRequestResult.DELEGATE;
    }

    public InformationRequestResult getDefaults(InformationSubject subject, String detail) throws SqpException {
        switch (subject) {
            case MaxPrecision:
                try {
                    SqpTypeCode typeCode = SqpTypeCode.valueOf(detail);
                    return new InformationRequestResult(InformationResponseType.Integer, typeCode.getMaxPrecision());
                } catch (IllegalArgumentException e) {
                    break;
                }

            case MaxScale:
                try {
                    SqpTypeCode typeCode = SqpTypeCode.valueOf(detail);
                    return new InformationRequestResult(InformationResponseType.Integer, typeCode.getMaxScale());
                } catch (IllegalArgumentException e) {
                    break;
                }

        }
        return InformationRequestResult.UNKNOWN;
    }

    private InformationRequestResult getTypeSchema(String typename) throws SqpException {
        if (typename == null || typename.length() < 1) {
            throw new SqpException(ErrorType.InvalidArgument,
                    "'detail' must be set to a type name when requesting a type schema", ErrorAction.Recover);
        }
        try {
            String schemaResource = SqpTypeCode.valueOf(typename).getSchemaResourceName();
            String schema = ResourceUtil.readResourceAsString(schemaResource, _logger);
           return new InformationRequestResult(InformationResponseType.Schema, schema);
        } catch (IllegalArgumentException e) {
            // This means we cannot find the type code. It's a not standard type, so we delegate this to the backend
            _logger.log(Level.INFO, "Cannot find type '" + typename + "'. Checking backend's type repository.");
        }

        // now check the backend type repo
        String schema = _backendTypeRepo.getSchema(typename);
        if (schema != null) {
            return new InformationRequestResult(InformationResponseType.Schema, schema);
        }
        return InformationRequestResult.UNKNOWN;
    }

}
