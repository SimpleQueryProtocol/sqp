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

import io.sqp.backend.BackendConnection;
import io.sqp.backend.ResultHandler;
import io.sqp.proxy.testhelpers.DummyBackendConnection;
import io.sqp.proxy.testhelpers.DummyBackendConnectionPool;
import org.hamcrest.MatcherAssert;
import org.hamcrest.core.Is;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.lang.reflect.Field;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.fail;

/**
 * @author Stefan Burnicki
 */
public class BackendConnectionPoolTest {

    public BackendConnectionPool createPool(int poolSize) throws Exception {
        BackendConnectionPool pool = Mockito.spy(new DummyBackendConnectionPool(poolSize));
        pool.init();
        return pool;
    }

    private void setMaxPoolSize(int num) throws Exception {
        BackendConnectionPool pool = createPool(1);
        Field poolSizeField = pool.getClass().getSuperclass().getDeclaredField("_poolSize");
        poolSizeField.setAccessible(true);
        poolSizeField.set(pool, num);
    }

    @Test
    public void canCreateConnection() throws Exception {
        BackendConnectionPool pool = createPool(1);
        // no active connections, yet
        assertThat(pool.getNumActiveConnections(), is(0));
        long id = pool.createConnection("test", s -> {}, new ResultHandler<>(fail -> fail(), connection -> {
            // the new connection should be connected to "test" and not be closed
            MatcherAssert.assertThat(connection, Is.is(new DummyBackendConnection(true, false, "test")));
        }));
        assertThat(id, is(greaterThanOrEqualTo(0l)));
        // connection was created directly
        assertThat(pool.getNumActiveConnections(), is(1));
    }

    @Test
    public void canCloseConnection() throws Exception {
        BackendConnectionPool pool = createPool(1);
        final BackendConnection[] connections = new BackendConnection[1];
        long id = pool.createConnection("test", s -> {}, new ResultHandler<>(f -> fail(),
                connection -> connections[0] = connection));
        assertThat(pool.getNumActiveConnections(), is(1));
        assertThat(connections[0], is(new DummyBackendConnection(true, false, "test"))); // connected, not closed

        pool.closeConnection(id);
        assertThat(pool.getNumActiveConnections(), is(0));
        assertThat(connections[0], is(new DummyBackendConnection(true, true, "test"))); // connected, closed
    }

    @Test
    public void canQueueConnection() throws Exception {
        BackendConnectionPool pool = createPool(1);
        BackendConnection dummyConnection = new DummyBackendConnection(true, false, "test");
        final BackendConnection[] connections = new DummyBackendConnection[2];

        // first connect happens directly
        long id = pool.createConnection("test", s -> {}, new ResultHandler<>(f -> fail(),
                connection -> connections[0] = connection));
        assertThat(id, is(greaterThanOrEqualTo(0l)));
        assertThat(pool.getNumActiveConnections(), is(1));
        assertThat(pool.getNumQueuedConnections(), is(0));
        assertThat(connections, is(arrayContaining(dummyConnection, null)));

        // second connect returns true, but queues the connectHandler as the pool limit is 1
        long nextId = pool.createConnection("test2", s -> {}, new ResultHandler<>(f -> fail(),
                connection -> connections[1] = connection));
        assertThat(nextId, is(id + 1));
        assertThat(pool.getNumActiveConnections(), is(1));
        assertThat(pool.getNumQueuedConnections(), is(1));
        assertThat(connections, is(arrayContaining(dummyConnection, null)));

        // we close the first connection now, which should also create the second one
        pool.closeConnection(id);
        // now the first dummyConnection should be closed while the second should exist and be connected
        assertThat(connections, is(arrayContaining(
                new DummyBackendConnection(true, true, "test"), // connected, closed, db: test
                new DummyBackendConnection(true, false, "test2") // new connection, not closed
        )));
        assertThat(pool.getNumActiveConnections(), is(1));
    }

    @Test
    public void closingQueuedConnectionRemovesItFromQueue() throws Exception {
        BackendConnectionPool pool = createPool(1);
        final BackendConnection[] connections = new DummyBackendConnection[2];

        // first connect
        long id = pool.createConnection("test", s -> {}, new ResultHandler<>(f -> fail(),
                connection -> connections[0] = connection));
        assertThat(id, is(greaterThanOrEqualTo(0l)));
        assertThat(pool.getNumActiveConnections(), is(1));
        assertThat(pool.getNumQueuedConnections(), is(0));

        // second queue
        long nextId = pool.createConnection("test2", s -> {}, new ResultHandler<>(f -> fail(),
                connection -> connections[1] = connection));
        assertThat(nextId, is(id + 1));
        assertThat(pool.getNumActiveConnections(), is(1));
        assertThat(pool.getNumQueuedConnections(), is(1));

        // we close the second connection now, which is only queued, not active
        pool.closeConnection(nextId);
        // now the first dummyConnection should still be opened while the second should not exist. que should be empty
        assertThat(connections, is(arrayContaining(new DummyBackendConnection(true, false, "test"), null)));
        assertThat(pool.getNumActiveConnections(), is(1));
        assertThat(pool.getNumQueuedConnections(), is(0));
    }

    @Test
    public void failIfQueueFull() throws Exception {
        BackendConnectionPool pool = createPool(1);
        final BackendConnection[] connections = new BackendConnection[3]; // just in case
        long id = pool.createConnection("test", s -> {}, new ResultHandler<>(f -> fail(),
                connection -> connections[0] = connection));
        assertThat(id, is(greaterThanOrEqualTo(0l)));
        long nextId = pool.createConnection("test2", s -> {}, new ResultHandler<>(f -> fail(),
                connection -> connections[1] = connection));
        assertThat(nextId, is(id + 1));
        assertThat(pool.getNumActiveConnections(), is(1));

        long failId = pool.createConnection("test3", s -> {}, new ResultHandler<>(f -> fail(),
                connection -> connections[2] = connection));
        assertThat(failId, is(-1l)); // rejected

        // make sure it's really not queued by closing and checking the others
        assertThat(pool.getNumActiveConnections(), is(1));

        assertThat(connections, is(arrayContaining(
                new DummyBackendConnection(true, false, "test"),
                null,
                null
        )));
        pool.closeConnection(id);

        // second connection from queue is opened
        assertThat(pool.getNumActiveConnections(), is(1));
        assertThat(connections, is(arrayContaining(
                new DummyBackendConnection(true, true, "test"),
                new DummyBackendConnection(true, false, "test2"),
                null
        )));
        pool.closeConnection(nextId);

        // connectHandler not called, no third connection opened, just two closed so it was really not queued
        assertThat(pool.getNumActiveConnections(), is(0));
        assertThat(connections, is(arrayContaining(
                new DummyBackendConnection(true, true, "test"),
                new DummyBackendConnection(true, true, "test2"),
                null
        )));
        assertThat(pool.getNumActiveConnections(), is(0));
    }

    @Test
    public void canOpenMultipleConnections() throws Exception {
        BackendConnectionPool pool = createPool(2);
        final BackendConnection[] connections = new DummyBackendConnection[2];

        // first connect happens directly
        pool.createConnection("test", s -> {
        }, new ResultHandler<>(f -> fail(),
                connection -> connections[0] = connection));
        long nextId = pool.createConnection("test2", s -> {}, new ResultHandler<>(f -> fail(),
                connection -> connections[1] = connection));
        assertThat(pool.getNumActiveConnections(), is(2));
        assertThat(connections, is(arrayContaining(
                new DummyBackendConnection(true, false, "test"),
                new DummyBackendConnection(true, false, "test2")
        )));

        // close the second connection to show that the correct one is closed
        pool.closeConnection(nextId);
        assertThat(pool.getNumActiveConnections(), is(1));
        assertThat(connections, is(arrayContaining(
                new DummyBackendConnection(true, false, "test"),
                new DummyBackendConnection(true, true, "test2") // now closed
        )));
    }
}
