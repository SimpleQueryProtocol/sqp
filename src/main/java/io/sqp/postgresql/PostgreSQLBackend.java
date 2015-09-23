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

import io.sqp.backend.*;

import javax.naming.ConfigurationException;
import java.util.logging.Logger;

/**
 * @author Stefan Burnicki
 */
public class PostgreSQLBackend implements Backend {
    private PGConfiguration _configuration;
    private AsyncExecutor _asyncExecutor;
    private Logger _logger;
    private PGTypeRepository _typeRepository;

    @Override
    public void init(Configuration configuration, AsyncExecutor asyncExecutor) throws ConfigurationException {
        _logger = Logger.getGlobal();
        _asyncExecutor = asyncExecutor;
        _configuration = PGConfiguration.load(configuration, _logger);
        _typeRepository = new PGTypeRepository(_logger);
    }

    @Override
    public BackendConnection createConnection() {
        return new PGConnection(_configuration, _asyncExecutor);
    }

    @Override
    public TypeRepository getTypeRepository() {
        return _typeRepository;
    }
}
