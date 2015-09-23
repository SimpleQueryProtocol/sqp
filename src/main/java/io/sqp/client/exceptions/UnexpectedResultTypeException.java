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

package io.sqp.client.exceptions;

import io.sqp.core.ErrorType;
import io.sqp.core.ErrorAction;
import io.sqp.core.exceptions.SqpException;

/**
 * @author Stefan Burnicki
 */
public class UnexpectedResultTypeException extends SqpException {
    private Object _result;
    private Class<?> _expectedClass;

    public UnexpectedResultTypeException(Object result, Class<?> expectedClass) {
        super(ErrorType.UnexpectedResultType, "Result was of type " +
              (result == null ? " null" : result.getClass().getName()) +
              ", but was expected to be of type " + expectedClass.getName(), ErrorAction.Recover);
        _result = result;
        _expectedClass = expectedClass;
    }

    public Object getResult() {
        return _result;
    }

    public Class<?> getExpectedClass() {
        return _expectedClass;
    }
}
