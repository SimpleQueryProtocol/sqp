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

import io.sqp.proxy.exceptions.ServerErrorException;
import io.sqp.proxy.vertx.VertxClientConnection;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.sqp.proxy.vertx.VertxBackendConnectionPool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Stefan Burnicki
 */
public class ServerVerticle extends AbstractVerticle {
    public static final String DEFAULT_PATH = "/";
    public static final int DEFAULT_PORT = 8080;
    public static final int DEFAULT_POOL_SIZE = 30;
    private ExecutorService _executorService;
    private Logger _logger;
    private boolean _started;
    private Throwable _startingError;

    public ServerVerticle() {
        _logger = Logger.getGlobal();
    }

    @Override
    public void start() {
        // TODO: set subprotocols

        JsonObject config = config();

        String path = config.getString("path", DEFAULT_PATH);
        int port = config.getInteger("port", DEFAULT_PORT);
        int poolSize = config.getInteger("connectionPoolSize", DEFAULT_POOL_SIZE);
        JsonArray backendConfs = config.getJsonArray("backends");
        _executorService = Executors.newFixedThreadPool(10); // TODO: set this reasonably

        // Initialize the backend connection pool
        BackendConnectionPool connectionPool = new VertxBackendConnectionPool(vertx, poolSize, backendConfs);

        try {
            connectionPool.init();
        } catch (ServerErrorException e) {
            _logger.log(Level.SEVERE, "Failed to create the connection pool", e);
            throw new RuntimeException(e.getMessage(), e.getCause());
        }

        HttpServerOptions options = new HttpServerOptions();
        int maxFrameSize = options.getMaxWebsocketFrameSize();


        // Create the actual server
        HttpServer server = vertx.createHttpServer(options);

        // For each incoming websocket connection: create a client connection object
        server.websocketHandler(socket -> {
            if (!socket.path().equals(path)) {
                socket.reject();
                return;
            }
            // TODO: check sub protocols
            new VertxClientConnection(_executorService, socket, connectionPool, maxFrameSize);
        });
        // start to listen
        server.listen(port, result -> {
            if (result.succeeded()) {
                _logger.log(Level.INFO, "Listening on port " + port + " and path '" + path + "'...");
                setStarted(true);
            } else {
                _logger.log(Level.SEVERE, "Failed to listen", result.cause());
                setStartingError(result.cause());
            }
        });
    }

    public synchronized Throwable getStartingError() {
        return _startingError;
    }

    public synchronized boolean isStarted() {
        return _started;
    }

    private synchronized void setStartingError(Throwable startingError) {
        _startingError = startingError;
    }

    private synchronized void setStarted(boolean started) {
        _started = started;
    }
}
