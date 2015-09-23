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

package io.sqp.proxy.vertx;

import io.sqp.core.exceptions.BackendErrorException;
import io.vertx.core.Vertx;
import io.sqp.backend.AsyncExecutor;
import io.sqp.backend.ResultHandler;
import io.sqp.core.exceptions.SqpException;

/**
 * @author Stefan Burnicki
 */
public class VertxAsyncExecutor implements AsyncExecutor {
    private Vertx _vertx;

    public VertxAsyncExecutor(Vertx vertx) {
        _vertx = vertx;
    }

    @Override
    public <T> void callAsync(FallibleCallable<T> callable, ResultHandler<T> resultHandler) {
        _vertx.<T>executeBlocking(future -> {
            try {
                T result = callable.invoke();
                future.complete(result);
            } catch (Exception e) {
                future.fail(e);
            }
        }, res -> {
            if (res.failed()) {
                Throwable cause = res.cause();
                if (cause instanceof SqpException) {
                    resultHandler.fail((SqpException) cause);
                } else {
                    resultHandler.fail(new BackendErrorException(cause));
                }
            } else {
                resultHandler.handle(res.result());
            }
        });
    }
}
