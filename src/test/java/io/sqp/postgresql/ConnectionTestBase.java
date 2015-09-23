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

import io.vertx.core.Vertx;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import io.sqp.backend.AsyncExecutor;
import io.sqp.backend.Configuration;
import io.sqp.proxy.vertx.VertxAsyncExecutor;
import io.sqp.testhelpers.AsyncTestBase;
import io.sqp.testhelpers.TestUtils;

import javax.naming.ConfigurationException;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Logger;

/**
 * @author Stefan Burnicki
 */
// TODO: currently unused!
public class ConnectionTestBase extends AsyncTestBase {
    private static Logger _logger = Logger.getGlobal();
    private static PGConfiguration _pgConfiguration;
    private static AsyncExecutor _asyncExecutor;

    protected PGConnection connection;

    @BeforeSuite
    public void initConfig() throws IOException, ConfigurationException {
        Map<String, Object> configMap = TestUtils.getBackendConfiguration("native-postgres").getJsonArray("backends")
                                                 .getJsonObject(0).getJsonObject("config").getMap();
        _pgConfiguration = PGConfiguration.load(new Configuration(configMap), _logger);
        _asyncExecutor = new VertxAsyncExecutor(Vertx.vertx());
    }

    @BeforeMethod
    public void initConnection() throws IOException {
        connection = new PGConnection(_pgConfiguration, _asyncExecutor);
        waitFor(1);
        connection.connect("proxytest", null, completionHandler());
        await();
    }

}
