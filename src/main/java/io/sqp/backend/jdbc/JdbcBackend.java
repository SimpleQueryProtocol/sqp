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

package io.sqp.backend.jdbc;

import io.sqp.backend.BackendConnection;
import io.sqp.backend.Configuration;
import io.sqp.backend.AsyncExecutor;
import io.sqp.backend.Backend;

import javax.naming.ConfigurationException;
import java.util.logging.Logger;

/**
 * @author Stefan Burnicki
 */
public class JdbcBackend implements Backend {
    private String _jdbcDriver;
    private String _jdbcUrl;
    private String _username;
    private String _password;
    private Logger _logger;

    @Override
    public void init(Configuration configuration, AsyncExecutor asyncExecutor) throws ConfigurationException {
        loadConfig(configuration);
        loadJdbcDriver();
        _logger = Logger.getGlobal();
    }

    @Override
    public BackendConnection createConnection() {
        return new JdbcConnection(_logger, _jdbcUrl, _username, _password);
    }


    private void loadJdbcDriver() throws ConfigurationException {
        try {
            Class.forName(_jdbcDriver);
        } catch (ClassNotFoundException e) {
            throw new ConfigurationException("The configured JDBC driver '" + _jdbcDriver + "' could not be loaded:"
                    + e.getMessage());
        }
    }

    private void loadConfig(Configuration configuration) throws ConfigurationException {
        _jdbcDriver = configuration.getString("jdbcDriver");
        _jdbcUrl = configuration.getString("jdbcUrl");
        _username = configuration.getString("username");
        _password = configuration.getString("password", true);

        // make sure the JDBC URL ends with a slash so we can easily append the database
        if (!_jdbcUrl.endsWith("/")) {
            _jdbcUrl += "/";
        }
    }
}
