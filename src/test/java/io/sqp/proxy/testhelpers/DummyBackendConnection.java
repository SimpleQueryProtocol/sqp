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

package io.sqp.proxy.testhelpers;

import io.sqp.backend.*;
import io.sqp.backend.results.InformationRequestResult;
import io.sqp.core.types.SqpValue;
import io.sqp.backend.results.QueryResult;
import io.sqp.core.InformationSubject;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

/**
 * @author Stefan Burnicki
 */
public class DummyBackendConnection implements BackendConnection {
    private boolean _connected;
    private String _dbName;
    private boolean _closed;

    public DummyBackendConnection(boolean connected, boolean closed, String dbName) {
        _connected = connected;
        _closed = closed;
        _dbName = dbName;
    }

    @Override
    public void connect(String databaseName, Consumer<String> disconnectHandler, SuccessHandler connectionHandler) {
        _connected = true;
        _dbName = databaseName;
        connectionHandler.succeed();
    }

    @Override
    public void simpleQuery(String sql, String cursorId, boolean scrollable, int maxFetch, ResultHandler<QueryResult> resultHandler) {

    }

    @Override
    public void setFeatures(List<FeatureSetting<?>> featureSettings, SuccessHandler successHandler) {

    }

    @Override
    public void commit(SuccessHandler successHandler) {

    }

    @Override
    public void rollback(SuccessHandler successHandler) {

    }

    @Override
    public void getInformation(InformationSubject subject, String detail, ResultHandler<InformationRequestResult> resultHandler) {

    }

    @Override
    public void prepare(String sql, String statementId, SuccessHandler successHandler) {

    }

    @Override
    public void release(Collection<String> statementIds, Collection<String> cursorIds, SuccessHandler successHandler) {

    }

    @Override
    public void execute(String statementId, String cursorId, List<List<SqpValue>> parameters, boolean scrollable, ResultHandler<QueryResult> resultHandler) {

    }

    @Override
    public void fetch(String cursorId, int position, int numRows, boolean forward, ResultHandler<QueryResult> resultHandler) {

    }

    @Override
    public void close() {
        _closed = true;
    }

    @Override
    public void getLob(String id, long offset, long length, ResultHandler<LobStream> resultHandler) {

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DummyBackendConnection that = (DummyBackendConnection) o;

        if (_connected != that._connected) return false;
        if (_closed != that._closed) return false;
        return !(_dbName != null ? !_dbName.equals(that._dbName) : that._dbName != null);

    }

    @Override
    public int hashCode() {
        int result = (_connected ? 1 : 0);
        result = 31 * result + (_closed ? 1 : 0);
        result = 31 * result + (_dbName != null ? _dbName.hashCode() : 0);
        return result;
    }
}
