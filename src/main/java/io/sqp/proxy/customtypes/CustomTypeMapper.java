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
import io.sqp.backend.*;
import io.sqp.proxy.exceptions.TypeMappingNotPossibleException;
import io.sqp.schemamatcher.InvalidMatchingSchemaException;
import io.sqp.schemamatcher.InvalidSchemaException;
import io.sqp.schemamatcher.SchemaMatcher;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * @author Stefan Burnicki
 */
public class CustomTypeMapper {
    Map<String, CustomType> _registeredMappings;
    TypeRepository _backendTypeRepo;
    Logger _logger;
    private BackendConnection _backendConnection;

    public CustomTypeMapper(Logger logger, TypeRepository backendTypeRepository) {
        _logger = logger;
        _backendTypeRepo = backendTypeRepository;
        _registeredMappings = new HashMap<>();
    }

    public CustomType getMapping(String name) {
        return _registeredMappings.get(name.toLowerCase());
    }

    public void registerMapping(String name, JsonNode schema, List<String> keywords, ResultHandler<String> resultHandler) throws TypeMappingNotPossibleException {
        name = name.toLowerCase();
        checkMappingName(name);
        keywords = keywords.stream().map(String::toLowerCase).collect(Collectors.toList());
        SchemaMatcher schemaMatcher;
        schemaMatcher = new SchemaMatcher(schema);
        // TODO: support standard type repository and throw an error if it fits a standard type?
        String match = findMappingInRepo(schemaMatcher, keywords, _backendTypeRepo, "backend repository");
        if (match == null) {
            throw new TypeMappingNotPossibleException("No native type matches the provided schema.");
        }
        registerWorkingMapping(name, match, schema, resultHandler);
    }

    private void checkMappingName(String name) {
        if (_registeredMappings.containsKey(name)) {
            // TODO: send a warning? now we just override it
        }
    }

    private void registerWorkingMapping(String name, String nativeType, JsonNode schema, ResultHandler<String> resultHandler) throws TypeMappingNotPossibleException {
        _registeredMappings.put(name, new CustomType(nativeType, SchemaTypeValidator.create(schema)));
        if (_backendConnection == null) {
            throw new TypeMappingNotPossibleException("No connection to the backend");
        }
        // allow that type to be returned from the backend
        FeatureSetting<String[]> feature = new FeatureSetting<>(FeatureSetting.Feature.AllowNativeTypes, new String[]{nativeType});
        _backendConnection.setFeatures(Collections.singletonList(feature),
                new SuccessHandler(resultHandler::fail, () -> resultHandler.handle(nativeType)));
    }

    private String findMappingInRepo(SchemaMatcher matcher, List<String> keywords, TypeRepository repository, String repoName) throws TypeMappingNotPossibleException {
        ArrayList<String> keywordTypes = new ArrayList<>();
        ArrayList<String> otherTypes = new ArrayList<>();
        repository.getNativeTypes().forEach(type -> (typeMatchesAnyKeyword(type, keywords) ? keywordTypes : otherTypes).add(type));

        String match;
        if (keywordTypes.size() > 0) {
            match = findMapping(matcher, repository, keywordTypes);
            if (match != null) {
                return match;
            } else {
                _logger.log(Level.INFO, "Didn't find a native type matching the schema with keywords: '" +
                        String.join(", ", keywords) + "' in " + repoName);
            }
        }
        match = findMapping(matcher, repository, otherTypes);
        if (match == null) {
            _logger.log(Level.INFO, "Didn't find any native type matching the schema in " + repoName);
        }
        return match;
    }

    private String findMapping(SchemaMatcher matcher, TypeRepository repository, ArrayList<String> types) throws TypeMappingNotPossibleException {
        for (String type : types) {
            String typeSchema = repository.getSchema(type);
            if (typeSchema == null) {
                _logger.log(Level.WARNING, "Backend told to support type '" + type + "', but doesn't provide a schema.");
                continue;
            }
            try {
                if (matcher.isCompatibleTo(typeSchema)) {
                    return type;
                }
            } catch (InvalidSchemaException e) {
                throw new TypeMappingNotPossibleException("The provided schema seems to be invalid: " + e.getMessage());
            } catch (InvalidMatchingSchemaException e) {
                _logger.log(Level.WARNING, "The schema of type '" + type + "' seems to be invalid: " + e.getMessage());
            }
        }
        return null;
    }

    private static boolean typeMatchesAnyKeyword(String type, List<String> keywords) {
        return keywords.stream().anyMatch(type::contains);
    }

    public void setBackendConnection(BackendConnection backendConnection) {
        _backendConnection = backendConnection;
    }
}
