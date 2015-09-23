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

package io.sqp.client.impl;

import io.sqp.client.exceptions.UnexpectedMessageException;
import io.sqp.core.exceptions.DecodingException;
import io.sqp.core.exceptions.SqpException;
import io.sqp.core.messages.SqpMessage;

import java.util.concurrent.CompletableFuture;

/**
 * @author Stefan Burnicki
 */
public class ResponseHandler<T> {
    private final ResponseHandlerFunction _handler;
    private final CompletableFuture<T> _future;

    protected ResponseHandler(CompletableFuture<T> future) {
        _future = future;
        _handler = null;
    }

    public ResponseHandler(CompletableFuture<T> future, ResponseHandlerFunction handler) {
        _handler = handler;
        _future = future;
    }

    protected void succeed(T result) {
        _future.complete(result);
    }

    public boolean handle(SqpMessage message) throws UnexpectedMessageException, DecodingException {
        if (_handler == null) {
            throw new UnexpectedMessageException("using an invalid result handler", message);
        }
        return _handler.handle(message);
    }

    public void fail(SqpException error) {
        if (_future != null) {
            _future.completeExceptionally(error);
        }
    }

    public CompletableFuture<T> getAffectedFuture() {
        return _future;
    }
}
