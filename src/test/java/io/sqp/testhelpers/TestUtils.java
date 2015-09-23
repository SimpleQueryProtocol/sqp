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

package io.sqp.testhelpers;

import io.vertx.core.json.JsonObject;
import io.sqp.proxy.ServerVerticle;

import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;

/**
 * @author Stefan Burnicki
 */
public class TestUtils {
    public static final int PROXY_DEFAULT_PORT = ServerVerticle.DEFAULT_PORT;
    public static final String PROXY_DEFAULT_URL = ServerVerticle.DEFAULT_PATH;

    public static JsonObject getBackendConfiguration(String backendConfigName) throws IOException {
        return getBackendConfiguration(backendConfigName, PROXY_DEFAULT_URL, PROXY_DEFAULT_PORT);
    }

    public static JsonObject getBackendConfiguration(String backendConfigName, String url, int port) throws IOException {
        JsonObject configurations = new JsonObject(TestUtils.readResourceAsString("backendConfigurations.json"));
        JsonObject backendConfig = configurations.getJsonObject(backendConfigName);
        if (backendConfig == null) {
            throw new IOException("Configuration file does not contain a section for '" + backendConfigName + "'");
        }
        backendConfig.put("url", url);
        backendConfig.put("port", port);
        return backendConfig;
    }

    public static String readResourceAsString(String resourceName) throws IOException {
        try (InputStream resourceStream = ClassLoader.getSystemClassLoader().getResourceAsStream(resourceName)) {
            // TODO: this uses a "Scanner trick, since \A means "beginning of input boundary". Also see here:
            // http://stackoverflow.com/questions/309424/read-convert-an-inputstream-to-a-string
            try (Scanner s = new java.util.Scanner(resourceStream).useDelimiter("\\A")) {
                return s.hasNext() ? s.next() : "";
            }
        } catch (IOException e) {
            throw new IOException("Failed to open the resource stream for " + resourceName);
        }
    }
}
