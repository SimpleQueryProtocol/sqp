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

import io.sqp.client.QueryResult;
import io.sqp.client.UpdateResult;
import io.sqp.client.exceptions.UnexpectedMessageException;
import io.sqp.client.exceptions.UnexpectedResultTypeException;
import io.sqp.core.exceptions.DecodingException;
import io.sqp.core.messages.CursorDescriptionMessage;
import io.sqp.core.messages.ExecuteCompleteMessage;
import io.sqp.core.messages.SqpMessage;

import java.util.concurrent.CompletableFuture;

/**
 * @author Stefan Burnicki
 */
public class ExecuteResponseHandler<T extends QueryResult> extends ResponseHandler<T> {
    private QueryResult _queryResult;
    private FetchDataResponseHandler _fetchDataResponseHandler;
    private Class<T> _resultClass;
    private SqpConnectionImpl _connection;
    private boolean _autoFetch;

    public ExecuteResponseHandler(Class<T> resultClass, SqpConnectionImpl connection, boolean autoFetch) {
        super(new CompletableFuture<>());
        _queryResult = null;
        _resultClass = resultClass;
        _connection = connection;
        _autoFetch = autoFetch;
    }

    @Override
    public boolean handle(SqpMessage message) throws UnexpectedMessageException, DecodingException {
        String retrievingCursorStr = "retrieving a cursor";
        if (_fetchDataResponseHandler != null) {
            if (_fetchDataResponseHandler.handle(message)) {
                completeFuture();
                return true;
            }
            return false;
        }
        switch (message.getType()) {
            case ReadyMessage:
                // don't bother if there are ready messages before getting a result
                if (_queryResult != null) {
                    throw new UnexpectedMessageException(retrievingCursorStr, message);
                }
                return false;

            case ExecuteCompleteMessage:
                if (_queryResult != null) {
                    throw new UnexpectedMessageException(retrievingCursorStr, message);
                }
                ExecuteCompleteMessage executeCompleteMessage = message.secureCast();
                _queryResult = new UpdateResult(executeCompleteMessage.getAffectedRows());
                completeFuture();
                return true;

            case CursorDescriptionMessage:
                CursorDescriptionMessage cursorMsg = message.secureCast();
                CursorImpl cursor = new CursorImpl(_connection, cursorMsg.getCursorId(), cursorMsg.getColumns(),
                        cursorMsg.isScrollable());
                _queryResult = cursor;
                // when we need autoFetch, we just create the fetch handler and works continues
                if (_autoFetch) {
                    _fetchDataResponseHandler = new FetchDataResponseHandler(cursor);
                    return false;
                }
                // otherwise we're done here
                completeFuture();
                return true;

            default:
                throw new UnexpectedMessageException("waiting for a result", message);
        }
    }

    private void completeFuture() {
        if (!_resultClass.isAssignableFrom(_queryResult.getClass())) {
            fail(new UnexpectedResultTypeException(_queryResult, _resultClass));
            return;
        }
        succeed(_resultClass.cast(_queryResult));
    }
}
