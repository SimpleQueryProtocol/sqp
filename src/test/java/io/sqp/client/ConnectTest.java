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

import io.sqp.client.exceptions.ConnectionException;
import io.sqp.client.exceptions.ErrorResponseException;
import io.sqp.core.ErrorType;
import org.hamcrest.core.Is;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.testng.FileAssert.fail;

/**
 * @author Stefan Burnicki
 */
public class ConnectTest extends ConnectionTestBase{
    @Test
    public void CanConnectAndClose() throws Exception {
        try (SqpConnection connection = SqpConnection.create()){
            CompletableFuture<SqpConnection> future = defaultConnect(connection);
            // wait for the future
            future.join();
            assertThat(connection.isConnected(), is(true));
            connection.close();
            assertThat(connection.isConnected(), is(false));
        }
    }

    @Test
    public void CanConnectAndAutoClose() throws Exception {
        SqpConnection openedConn;
        try (SqpConnection connection = SqpConnection.create()){
            openedConn = connection;
            CompletableFuture<SqpConnection> future = defaultConnect(connection);
            // wait for the future
            future.join();
            assertThat(connection.isConnected(), is(true));
        }
        assertThat(openedConn.isConnected(), is(false));
    }

    @Test
    public void ConnectWithWrongParametersFails() throws Throwable {
        try (SqpConnection connection = SqpConnection.create()){
            CompletableFuture<SqpConnection> future = connection.connect("localhost", proxyPort, "/foo", TEST_DATABASE);
            // wait for the future
            future.join();
            fail(); // exception should be thrown before
        } catch (CompletionException e) {
            assertThat(e.getCause(), instanceOf(ConnectionException.class));
        }
    }

    @Test
    public void ConnectWithWrongDatabaseFails() throws Exception {
        try (SqpConnection connection = SqpConnection.create()){
            CompletableFuture<SqpConnection> future = connection.connect("localhost", proxyPort, proxyUrl, "fooDB");
            // wait for the future
            future.join();
            fail(); // exception should be thrown before
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            assertThat(cause, instanceOf(ErrorResponseException.class));
            ErrorResponseException errorResp = (ErrorResponseException) cause;
            assertThat(errorResp.getErrorType(), Is.is(ErrorType.DatabaseConnectionFailed));
        }
    }
}
