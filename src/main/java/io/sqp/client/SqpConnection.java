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

package io.sqp.client;

import io.sqp.client.impl.SqpConnectionImpl;
import io.sqp.core.InformationSubject;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * @author Stefan Burnicki
 */
public interface SqpConnection extends Closeable {
    void close() throws IOException;
    boolean isConnected();

    // TODO: check and clear pending exceptions?

    // TODO: support an explicit cursor name that can auto-close other cursors with the same name

    <T extends QueryResult> CompletableFuture<T> execute(Class<T> resultClass, boolean scrollable, String query);

    default <T extends QueryResult> CompletableFuture<T>  execute(Class<T> resultClass, String query) {
        return execute(resultClass, false, query);
    }

    default CompletableFuture<QueryResult> execute(String query) {
        return execute(QueryResult.class, false, query);
    }
    default CompletableFuture<Cursor> executeSelect(String query) { return execute(Cursor.class, false, query); }
    default CompletableFuture<UpdateResult> executeUpdate(String query) { return execute(UpdateResult.class, false, query); }

    PreparedStatement prepare(String query);

    CompletableFuture<SqpConnection> connect(String host, int port, String path, String database);

    CompletableFuture<Void> setAutoCommit(boolean useAutoCommit);
    boolean getAutoCommit();
    CompletableFuture<Void> commit();
    CompletableFuture<Void> rollback();

    <T> CompletableFuture<T> getInformation(Class<T> infoType, InformationSubject subject);
    CompletableFuture<String> getTypeSchema(String typeName);

    CompletableFuture<Void> allowReceiveNativeTypes(String ...allowedTypes);
    CompletableFuture<String> registerTypeMapping(String name, String schema, String ...keywords);

    static SqpConnection create(ClientConfig config) {
        return new SqpConnectionImpl(config);
    }
    static SqpConnection create() {
        return create(ClientConfig.create());
    }
}
