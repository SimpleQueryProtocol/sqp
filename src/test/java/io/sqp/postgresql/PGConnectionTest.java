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
import org.testng.annotations.Test;
import io.sqp.backend.Configuration;
import io.sqp.backend.SuccessHandler;
import io.sqp.backend.exceptions.DatabaseConnectionException;
import io.sqp.proxy.vertx.VertxAsyncExecutor;
import io.sqp.testhelpers.AsyncTestBase;

import java.util.HashMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

/**
 * @author Stefan Burnicki
 */
public class PGConnectionTest extends AsyncTestBase {
    private Configuration _config = new Configuration(new HashMap<>())
            .set("username", "proxyuser")
            .set("password", "proxypw")
            .set("host", "localhost")
            .set("port", 5432);
    private PGConnection _connection;

    @BeforeMethod
    public void CreateConnection() throws Exception {
        _connection = new PGConnection(PGConfiguration.load(_config, null), new VertxAsyncExecutor(Vertx.vertx()));
    }

    @Test
    public void CanConnectToDatabase() throws Exception {
        _connection.connect("proxytest", v -> {}, new SuccessHandler(f -> fail(), this::complete));
        await();
    }

    @Test
    public void ConnectWithWrongDatabaseNameThrows() throws Exception {
        _connection.connect("notExisting", v -> {}, new SuccessHandler(
            error -> {
                assertThat(error, instanceOf(DatabaseConnectionException.class));
                complete();
            },
            () -> fail("Connect seemed to work anyway!")
        ));
        await();
    }

    @Test
    public void ConnectWithWrongPortThrows() throws Exception {
        PGConnection connection = new PGConnection(PGConfiguration.load(_config.set("port", 1), null),
                new VertxAsyncExecutor(Vertx.vertx()));
        connection.connect("notExisting", v -> {}, new SuccessHandler(
            error -> {
                assertThat(error, instanceOf(DatabaseConnectionException.class));
                complete();
            },
            () -> fail("Connect seemed to work anyway!")
        ));
        await();
    }
}
