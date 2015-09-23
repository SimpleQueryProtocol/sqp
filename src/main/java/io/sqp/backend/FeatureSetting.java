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

import io.sqp.core.exceptions.BackendErrorException;

/**
 * @author Stefan Burnicki
 */
public class FeatureSetting<T> {
    public enum Feature {
        AutoCommit,
        CompressedDecimal,
        AllowNativeTypes
    }

    private Feature _feature;
    private T _value;

    public FeatureSetting(Feature feature, T value) {
        _feature = feature;
        _value = value;
    }

    public Feature getFeature() {
        return _feature;
    }

    public <U> U getValue(Class<U> targetClass) throws BackendErrorException {
        if (_value == null) {
            return null;
        }
        if (!targetClass.isAssignableFrom(_value.getClass())) {
            throw new BackendErrorException("The feature " + _feature + " was requested as a " + targetClass.getName() +
                    ", but is actualy a " + _value.getClass().getName());
        }
        return targetClass.cast(_value);
    }
}
