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

import org.postgresql.util.HostSpec;
import io.sqp.backend.Configuration;

import javax.naming.ConfigurationException;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Stefan Burnicki
 */
public class PGConfiguration {
    private String _username;
    private String _password;
    private HostSpec _hostSpec;
    private Locale _serverLocale;

    public PGConfiguration() {}

    public String getUsername() {
        return _username;
    }

    public void setUsername(String username) {
        _username = username;
    }

    public String getPassword() {
        return _password;
    }

    public void setPassword(String password) {
        _password = password;
    }

    public HostSpec getHostSpec() {
        return _hostSpec;
    }

    public void setHostSpec(HostSpec hostSpec) {
        _hostSpec = hostSpec;
    }

    public Locale getServerLocale() {
        return _serverLocale;
    }

    public void setServerLocale(Locale serverLocale) {
        _serverLocale = serverLocale;
    }

    public static PGConfiguration load(Configuration config, Logger logger) throws ConfigurationException {
        PGConfiguration pgConfig = new PGConfiguration();
        pgConfig.setUsername(config.getString("username"));
        pgConfig.setPassword(config.getString("password", true));
        pgConfig.setHostSpec(new HostSpec(config.getString("host"), config.getInt("port")));
        if (config.hasKey("serverCountry") && config.hasKey("serverLanguage")) {
            pgConfig.setServerLocale(new Locale(config.getString("serverLanguage"), config.getString("serverCountry")));
        } else {
            pgConfig.setServerLocale(Locale.getDefault());
            if (logger != null) {
                logger.log(Level.WARNING, "serverLanguage and/or serverCountry are not set. Using default locale.");
            }
        }
        return pgConfig;
    }
}
