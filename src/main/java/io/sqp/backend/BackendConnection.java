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

public interface BackendConnection {
    void connect(String databaseName, Consumer<String> disconnectHandler, SuccessHandler connectionHandler);
    void close();

    void simpleQuery(String sql, String cursorId, boolean scrollable, int maxFetch, ResultHandler<QueryResult> resultHandler);
    void prepare(String sql, String statementId, SuccessHandler successHandler);
    void execute(String statementId, String cursorId, List<List<SqpValue>> parameters, boolean scrollable, ResultHandler<QueryResult> resultHandler);
    void fetch(String cursorId, int position, int numRows, boolean forward, ResultHandler<QueryResult> resultHandler);
    void release(Collection<String> statementIds, Collection<String> cursorIds, SuccessHandler successHandler);

    void setFeatures(List<FeatureSetting<?>> featureSettings, SuccessHandler successHandler);
    void commit(SuccessHandler successHandler);
    void rollback(SuccessHandler successHandler);

    void getInformation(InformationSubject subject, String detail, ResultHandler<InformationRequestResult> resultHandler);
    void getLob(String id, long offset, long length, ResultHandler<LobStream> resultHandler);
}
