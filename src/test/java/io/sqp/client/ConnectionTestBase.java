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

package io.sqp.client;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Parameters;
import io.sqp.client.testhelpers.ProxyStarter;

import java.util.concurrent.CompletableFuture;

/**
 * @author Stefan Burnicki
 */
public class ConnectionTestBase {
    protected final static String TEST_TABLE = "weather";
    protected final static String TEST_DATABASE = "proxytest";
    private static  ProxyStarter _proxyStarter;
    protected static int proxyPort;
    protected static  String proxyUrl;
    protected static  String dbmsName;

    @BeforeSuite(alwaysRun = true)
    @Parameters("backendConfiguration")
    public void StartProxy(String backendConfiguration) throws Throwable {
        _proxyStarter = new ProxyStarter(backendConfiguration);
        _proxyStarter.start();
        proxyPort = _proxyStarter.getPort();
        proxyUrl = _proxyStarter.getUrl();
        dbmsName = _proxyStarter.getDbmsName();
    }

    @AfterSuite(alwaysRun = true)
    public void StopProxy() {
        if (_proxyStarter != null) {
            _proxyStarter.stop();
        }
    }

    protected CompletableFuture<SqpConnection> defaultConnect(SqpConnection conn) {
        return conn.connect("localhost", proxyPort, proxyUrl, TEST_DATABASE);
    }

    protected CompletableFuture<QueryResult> clearTestTable(SqpConnection conn) {
        return conn.execute("DELETE FROM " + TEST_TABLE);
    }

    protected CompletableFuture<QueryResult> insertData(SqpConnection conn) {
        String[] values = new String[] {
                "('TestCity', -12, 30, 0.123456789, '1015-07-12')",
                "('FooCity', 0, 21, 1.3, '1900-01-03')",
                "('Stuttgart', -400, -2, 3242342.11111111111111, '3156-12-31')",
                "('T\u00dcbingen', 40, 2232, 0.0, '0001-02-04')"
        };
        String beginStmt = "INSERT INTO " + TEST_TABLE + " (city, temp_lo, temp_hi, prob, date) VALUES ";
        CompletableFuture<QueryResult> future = conn.execute(beginStmt + values[0]);
        for (int i = 1; i < values.length; i++) {
            final int curIdx = i;
            future = future.thenCompose(v->conn.execute(beginStmt + values[curIdx]));
        }
        return future;
    }
}
