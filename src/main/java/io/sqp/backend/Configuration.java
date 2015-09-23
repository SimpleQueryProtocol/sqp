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

package io.sqp.backend;

import javax.naming.ConfigurationException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Stefan Burnicki
 */
public class Configuration {
    private Map<String, Object> _configMap;

    public Configuration() {
        this(new HashMap<>());
    }

    public Configuration(Map<String, Object> configMap) {
        _configMap = configMap;
    }

    public boolean hasKey(String key) {
        return _configMap.containsKey(key);
    }

    public String getString(String key) throws ConfigurationException {
        return getString(key, false);
    }

    public String getString(String key, boolean allowEmpty) throws ConfigurationException {
        Object value = _configMap.get(key);
        if (value == null) {
            throwInvalidConfiguration("Required value '" + key + "' is not set.");
        }
        if (!(value instanceof String)) {
            throwInvalidConfiguration("Required value '" + key + "' is not a string, but '" + value.getClass().getName() + "'");
        }
        String strval = (String) value;
        if (!allowEmpty && strval.isEmpty()) {
            throwInvalidConfiguration("Required value '" + key + "' is empty.");
        }
        return strval;
    }

    public int getInt(String key) throws ConfigurationException {
        Object value = _configMap.get(key);
        if (value == null) {
            throwInvalidConfiguration("Required value '" + key + "' is not set.");
        }
        if (!(value instanceof Integer)) {
            throwInvalidConfiguration("Required value '" + key + "' is not an integer, but '" + value.getClass().getName() + "'");
        }
        return (int) value;
    }

    public boolean getBoolean(String key) throws ConfigurationException {
        Object value = _configMap.get(key);
        if (value == null) {
            throwInvalidConfiguration("Required value '" + key + "' is not set.");
        }
        if (!(value instanceof Boolean)) {
            throwInvalidConfiguration("Required value '" + key + "' is not an integer, but '" + value.getClass().getName() + "'");
        }
        return (boolean) value;
    }

    public Configuration set(String key, int value) {
        _configMap.put(key, value);
        return this;
    }

    public Configuration set(String key, String value) {
        _configMap.put(key, value);
        return this;
    }

    private void throwInvalidConfiguration(String reason) throws ConfigurationException {
        throw new ConfigurationException("The configuration is invalid: " + reason);
    }
}
