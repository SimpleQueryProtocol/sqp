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

package io.sqp.proxy.vertx;

import io.sqp.backend.AsyncExecutor;
import io.sqp.backend.Configuration;
import io.sqp.proxy.BackendConnectionPool;
import io.sqp.proxy.exceptions.ServerErrorException;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.sqp.backend.Backend;

import javax.naming.ConfigurationException;
import java.util.Map;
import java.util.logging.Level;

/**
 * @author Stefan Burnicki
 */
public class VertxBackendConnectionPool extends BackendConnectionPool {
    private Class<? extends Backend> _backendClass;
    private Map<String, Object> _backendSpecificConfiguration;
    private Vertx _vertx;
    private JsonArray _backendConfigurations;

    public VertxBackendConnectionPool(Vertx vertx, int poolSize, JsonArray backendConfigurations) {
        super(poolSize);
        _vertx = vertx;
        _backendConfigurations = backendConfigurations;
    }

    @Override
    protected void doInit() throws ServerErrorException {
        try {
            loadConfigurations(_backendConfigurations);
        } catch (ConfigurationException e) {
            throw new ServerErrorException("Configuration failed:" + e.getMessage(), e);
        }
    }

    @Override
    public AsyncExecutor getAsyncExecutor() {
        return new VertxAsyncExecutor(_vertx);
    }

    @Override
    public Class<? extends Backend> getBackendClass() {
        return _backendClass;
    }

    @Override
    public Configuration getBackendSpecificConfiguration() {
        return new Configuration(_backendSpecificConfiguration);
    }

    private void loadConfigurations(JsonArray backendConfs) throws ConfigurationException {
        // first validate the configuration object
        if (backendConfs == null || backendConfs.isEmpty()) {
            throw new ConfigurationException("No backend was configured.");
        }
        if (backendConfs.size() > 1) {
            logger.log(Level.WARNING, "Only one backend configuration is supported at the moment. The rest is ignored.");
        }
        JsonObject firstConf = backendConfs.getJsonObject(0);
        if (firstConf == null) {
            throwInvalidConfiguration("It's not a valid JSON object.");
        }

        // Every backend needs a specific 'config' configuration map
        JsonObject backendSpecificConf = firstConf.getJsonObject("config");
        if (backendSpecificConf == null) {
            throwInvalidConfiguration("Backend specific configuration is missing.");
        }
        _backendSpecificConfiguration = backendSpecificConf.getMap();

        // The 'type' defines the subclass of BackendConnection that is the heart of the backend
        String backendType = firstConf.getString("type");
        if (backendType == null) {
            throwInvalidConfiguration("No type specified.");
        }
        // Get the class from the class loader and verify it's a subclass of BackendConnection
        Class<?> uncastedBackendClass = null;
        try {
            uncastedBackendClass = Class.forName(backendType);
        } catch (ClassNotFoundException e) {
            throwInvalidConfiguration("Class '" + backendType + "' specified as 'type' was not found.");
        }
        try {
            _backendClass = uncastedBackendClass.asSubclass(Backend.class);
        } catch (ClassCastException e) {
            throwInvalidConfiguration("Class '" + backendType + "' is not a valid Backend implementation.");
        }
    }

    private void throwInvalidConfiguration(String reason) throws ConfigurationException {
        throw new ConfigurationException("The backend configuration is invalid: " + reason);
    }
}
