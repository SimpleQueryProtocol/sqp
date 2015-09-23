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

import io.sqp.backend.*;
import io.sqp.proxy.exceptions.ServerErrorException;
import io.sqp.core.exceptions.SqpException;

import javax.naming.ConfigurationException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Stefan Burnicki
 */
// TODO: think of reusing old connections
public abstract class BackendConnectionPool {
    final private int _poolSize;

    private Queue<QueuedConnection> _connectionQueue;
    private Map<Long, BackendConnection> _activeConnections;
    private long _currentConnectionId = 0;
    private Backend _backend;


    final protected Logger logger;

    protected BackendConnectionPool(int poolSize) {
        _poolSize = poolSize;
        logger = Logger.getGlobal();
        _connectionQueue = new LinkedList<>();
        _activeConnections = new HashMap<>(_poolSize);
    }

    public final void init() throws ServerErrorException {
        doInit();
        _backend = createBackend();
    }

    public Backend getBackend() {
        return _backend;
    }

    /**
     * Asynchronously get a connection from the connection pool
     **/
    public long createConnection(String dbName, Consumer<String> disconnectHandler, ResultHandler<BackendConnection> connectHandler) {
        long newConnectionId = _currentConnectionId;
        _currentConnectionId++;
        // either create it directly
        if (_activeConnections.size() < _poolSize) {
            instantiateConnection(newConnectionId, dbName, disconnectHandler, connectHandler);
            return newConnectionId;
        }
        // or queue it
        // TODO: think about a special parameter for the queue size
        if (_connectionQueue.size() < _poolSize) {
            _connectionQueue.add(new QueuedConnection(newConnectionId, dbName, disconnectHandler, connectHandler));
            logger.log(Level.INFO, "Queued BackendConnection creation");
            return newConnectionId;
        }

        // otherwise we are overloaded, so discard it, decrease connection id again, because we don't need it
        _currentConnectionId--;
        logger.log(Level.WARNING, "The connection pool and queue are full. Discarding a connection.");
        return -1;
    }

    public void closeConnection(long id) {
        if (id < 0) {
            return;
        }
        BackendConnection connection = _activeConnections.getOrDefault(id, null);
        // not in if, because it's optional anyway and would remove null values, if somehow present
        _activeConnections.remove(id);
        if (connection != null) {
            connection.close();
            logger.log(Level.INFO, "Closed BackendConnection: " + connection + " with ID: " + id);
        }
        // clean the queue
        _connectionQueue.removeIf(q -> q.id == id);

        // now there is one less active connection. check if we have a queue to invoke new connections
        if (_connectionQueue.size() > 0 && _activeConnections.size() < _poolSize) {
            logger.log(Level.INFO, "Taking a new connection request from the queue");
            QueuedConnection queued = _connectionQueue.poll();
            instantiateConnection(queued.id, queued.dbName, queued.disconnectHandler, queued.connectHandler);
        }
    }

    public int getNumActiveConnections() {
        return _activeConnections.size();
    }

    public int getNumQueuedConnections() {
        return _connectionQueue.size();
    }

    private void instantiateConnection(long connectionId, String dbName,
                                       Consumer<String> disconnectHandler,
                                       ResultHandler<BackendConnection> connectHandler) {


        try {
            BackendConnection backendConnection = newBackendConnection();
            _activeConnections.put(connectionId, backendConnection);
            // actually connect, call connectHandler if connect is successful
            backendConnection.connect(dbName, disconnectHandler,
                    new SuccessHandler(connectHandler::fail, () -> connectHandler.handle(backendConnection)));
        } catch (SqpException e) {
            connectHandler.fail(e);
        }
    }

    private BackendConnection newBackendConnection() throws ServerErrorException {
        // instantiate the backend connection class
        try {
            return _backend.createConnection();
        } catch (Exception e) {
            throw new ServerErrorException("The backend failed to create a connection.", e);
        }
    }

    private Backend createBackend() throws ServerErrorException {
        Configuration config = getBackendSpecificConfiguration();
        Class<? extends Backend> backendClass = getBackendClass();
        try {
            Constructor<? extends Backend> constructor = backendClass.getConstructor();
            Backend backend = constructor.newInstance();
            logger.log(Level.INFO, "Created BackendConnection: " + backend);
            backend.init(config, getAsyncExecutor());
            return backend;
        } catch (ConfigurationException e) {
            throw new ServerErrorException("Server error: " + e.getMessage(), e);
        } catch (Exception e) {
            // any reflectional error
            Throwable cause = e;
            // if the constructor failed, we will directly get the cause
            if (e instanceof InvocationTargetException) {
                cause = e.getCause();
            }
            throw new ServerErrorException("Failed to create the backend. This is likely to be server problem.", cause);
        }
    }

    abstract protected void doInit() throws ServerErrorException;

    abstract public AsyncExecutor getAsyncExecutor();

    abstract public Class<? extends Backend> getBackendClass();

    abstract public Configuration getBackendSpecificConfiguration();

    private class QueuedConnection {
        final public long id;
        final public String dbName;
        final public ResultHandler<BackendConnection> connectHandler;
        final public Consumer<String> disconnectHandler;

        public QueuedConnection(long id, String dbName, Consumer<String> disconnectHandler, ResultHandler<BackendConnection> connectHandler) {
            this.id = id;
            this.dbName = dbName;
            this.connectHandler = connectHandler;
            this.disconnectHandler = disconnectHandler;
        }
    }
}
