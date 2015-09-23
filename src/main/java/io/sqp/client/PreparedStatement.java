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

import io.sqp.core.types.SqpTypeCode;
import io.sqp.core.types.SqpValue;
import io.sqp.core.exceptions.SqpException;

import java.io.Closeable;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.concurrent.CompletableFuture;

/**
 * @author Stefan Burnicki
 */
public interface PreparedStatement extends Closeable {
    boolean isClosed();

    <T extends QueryResult> CompletableFuture<T> execute(Class<T> resultClass, boolean scrollable);
    default <T extends QueryResult> CompletableFuture<T> execute(Class<T> resultClass) {
        return execute(resultClass, false);
    }
    default CompletableFuture<QueryResult> execute() {
        return execute(QueryResult.class, false);
    }
    default CompletableFuture<Cursor> executeSelect() {
        return execute(Cursor.class, false);
    }
    default CompletableFuture<UpdateResult> executeUpdate() {
        return execute(UpdateResult.class, false);
    }

    PreparedStatement addBatch();

    // TODO: add method to clear a parameter (or the whole batch)

    PreparedStatement bind(int param, String value) throws SqpException;
    PreparedStatement bind(int param, int value) throws SqpException;
    PreparedStatement bind(int param, long value) throws SqpException;
    PreparedStatement bind(int param, BigDecimal value) throws SqpException;
    PreparedStatement bind(int param, float value) throws SqpException;
    PreparedStatement bind(int param, double value) throws SqpException;
    PreparedStatement bind(int param, LocalDate value) throws SqpException;
    PreparedStatement bind(int param, OffsetTime value) throws SqpException;
    PreparedStatement bind(int param, LocalTime value) throws SqpException;
    PreparedStatement bind(int param, OffsetDateTime value) throws SqpException;
    PreparedStatement bind(int param, byte[] bytes) throws SqpException;
    PreparedStatement bind(int param, InputStream binStream) throws SqpException;
    PreparedStatement bind(int param, Reader charStream) throws SqpException;
    PreparedStatement bind(int param, SqpValue value) throws SqpException;
    PreparedStatement bind(int param, String customTypeName, Object value) throws SqpException;
    PreparedStatement bindNull(int param, SqpTypeCode type) throws SqpException;
}
