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

package io.sqp.client.testhelpers;

import io.sqp.testhelpers.TestUtils;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.sqp.proxy.ServerVerticle;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author Stefan Burnicki
 */
public class ProxyStarter {
    private int _timeout;
    private int _port;
    private String _url;
    private String _deploymentId;
    private Vertx _vertx;
    private String _dbmsName;
    private JsonObject _config;

    public ProxyStarter(String backendConfiguration) throws IOException {
        this(backendConfiguration, ServerVerticle.DEFAULT_PORT, ServerVerticle.DEFAULT_PATH, 3000);
    }

    public ProxyStarter(String backendConfiguration, int port, String url, int timeout) throws IOException {
        _timeout = timeout;
        _port = port;
        _url = url;
        _vertx = Vertx.vertx();
        _config = TestUtils.getBackendConfiguration(backendConfiguration, url, port);
        _dbmsName = _config.getString("dbmsName");
    }

    public int getPort() {
        return _port;
    }

    public String getUrl() {
        return _url;
    }

    public void start() throws Throwable {
        if (_deploymentId != null) {
            throw new RuntimeException("Vertx is already deployed");
        }

        BlockingQueue<Object> blockingQueue = new ArrayBlockingQueue<>(1);
        DeploymentOptions options = new DeploymentOptions().setConfig(_config);
        ServerVerticle verticle = new ServerVerticle();
        _vertx.deployVerticle(verticle, options, r -> {
            try {
                blockingQueue.put(r.failed() ? r.cause() : r.result());
            } catch (InterruptedException e) {
                throw new RuntimeException("Failed starting the server", e);
            }
        });

        // wait until we started the server
        Object result;
        try {
            result = blockingQueue.poll(_timeout, TimeUnit.MILLISECONDS);
            long timeoutExpiredMs = System.currentTimeMillis() + _timeout;
            while (!verticle.isStarted() && System.currentTimeMillis() < timeoutExpiredMs) {
                Thread.sleep(100);
            }
            if (!verticle.isStarted()) {
                Throwable cause = verticle.getStartingError();
                String reason = cause == null ? "" : cause.getMessage();
                throw new RuntimeException("Verticle didn't start http server in time: " + reason);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed starting the server", e);
        }
        if (result instanceof String) {
            _deploymentId = (String) result;
        } else {
            throw ((Throwable) result);
        }
    }

    public String getDbmsName() {
        return _dbmsName;
    }

    public void stop() {
        if (_deploymentId == null) {
            return;
        }
        BlockingQueue<Boolean> blockingQueue = new ArrayBlockingQueue<>(1);
        _vertx.undeploy(_deploymentId, res -> {
            if (res.succeeded()) {
                try {
                    blockingQueue.put(true);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Failed to stop the verticle");
                }
            } else {
                throw new RuntimeException("Failed to undeploy the verticle: " + res.cause().getMessage());
            }
        });
        try {
            blockingQueue.poll(_timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed to stop the verticle");
        }
        _deploymentId = null;
    }
}
