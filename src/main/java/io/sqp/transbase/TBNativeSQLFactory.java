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

import io.sqp.core.exceptions.BackendErrorException;
import io.sqp.core.exceptions.SqpException;
import transbase.jdbc.TBNativeSQL;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

/**
 * @author Stefan Burnicki
 * This is a dirty hack as the TBNativeSQL class has a protected constructor
 */
public class TBNativeSQLFactory {
    private Constructor<TBNativeSQL> _constructor = null;
    private Method _getNativeSqlMethod = null;

    public TBNativeSQLFactory() throws BackendErrorException {
        try {
            _constructor = TBNativeSQL.class.getDeclaredConstructor(String.class, transbase.jdbc.Connection.class);
            _constructor.setAccessible(true);
            _getNativeSqlMethod = TBNativeSQL.class.getDeclaredMethod("getNativeSQL");
            _getNativeSqlMethod.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new BackendErrorException(e);
        }
    }

    private TBNativeSQL create(String sql) throws BackendErrorException {
        try {
            return _constructor.newInstance(sql, null);
        } catch (ReflectiveOperationException e) {
            throw new BackendErrorException(e);
        }
    }

    public String getNativeSQL(String sql) throws SqpException {
        return sql;
        // TODO: with the current Transbase JDBC driver, the following doesn't work anymore:
        /*
        try {
            return (String) _getNativeSqlMethod.invoke(create(sql));
        } catch (IllegalAccessException e) {
            throw new BackendErrorException(e);
        } catch (InvocationTargetException e) {
            throw new PrepareFailedException("The SQL query seems to be invalid", e);
        }
        */
    }

}
