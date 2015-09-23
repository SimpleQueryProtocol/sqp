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

import io.sqp.backend.Configuration;
import transbase.tbx.TBURL;

import javax.naming.ConfigurationException;

/**
 * @author Stefan Burnicki
 */
public class TBConfiguration {
    private String _username;
    private String _password;
    private String _tbURlStart;

    private TBConfiguration() {}

    public String getUsername() {
        return _username;
    }

    public String getPassword() {
        return _password;
    }

    public TBURL getTBUrlForDatabase(String database) {
        return new TBURL(_tbURlStart + database);
    }

    public static TBConfiguration load(Configuration config) throws ConfigurationException {
        TBConfiguration tbconfig = new TBConfiguration();
        tbconfig.setUsername(config.getString("username"));
        tbconfig.setPassword(config.getString("password", true));
        tbconfig.setTbURlStart(buildTBUrlStart(config));
        return tbconfig;
    }

    private void setUsername(String username) {
        _username = username;
    }

    private void setPassword(String password) {
        _password = password;
    }

    private void setTbURlStart(String tbURlStart) {
        _tbURlStart = tbURlStart;
    }

    private static String buildTBUrlStart(Configuration config) throws ConfigurationException {
        // the jdbc url schema type is even represented in the TBX lower level.
        // We need to specify the URL like this or we won't get a connection
        StringBuilder sb = new StringBuilder("jdbc:transbase:");
        if (config.hasKey("pipe") && config.getBoolean("pipe")) {
            sb.append("pipe///");
        } else {
            sb.append("//");
            createNetworkUrlPart(config, sb);
        }
        return sb.toString();
    }

    private static void createNetworkUrlPart(Configuration config, StringBuilder sb) throws ConfigurationException {
        sb.append(config.getString("host"));
        if (config.hasKey("kernelPort")) {
            sb.append(":");
            sb.append(config.getInt("kernelPort"));
        }
        if (config.hasKey("serverPort")) {
            sb.append(":");
            sb.append(config.getInt("serverPort"));
        }
        sb.append("/");
    }
}
