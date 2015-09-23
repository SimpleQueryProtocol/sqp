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

import io.vertx.core.Vertx;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import io.sqp.backend.Configuration;
import io.sqp.backend.SuccessHandler;
import io.sqp.testhelpers.AsyncTestBase;
import io.sqp.proxy.vertx.VertxAsyncExecutor;

import java.util.HashMap;
import java.util.logging.Logger;

/**
 * @author Stefan Burnicki
 */
public class ConnectionTest extends AsyncTestBase {
    private Configuration _config = new Configuration(new HashMap<>())
        .set("username", "tbadmin")
        .set("password", "")
        .set("host", "localhost")
        .set("kernelPort", 2024);
    private Connection _connection;

    @BeforeMethod
    public void CreateConnection() throws Exception {
        _connection = new Connection(Logger.getGlobal(),TBConfiguration.load(_config),
                new TBNativeSQLFactory(), new VertxAsyncExecutor(Vertx.vertx()));
    }

    @Test
    public void CanConnectToDatabase() throws Exception {
        _connection.connect("proxytest", v -> {}, new SuccessHandler(f -> fail(), this::complete));
        await();
    }
}
