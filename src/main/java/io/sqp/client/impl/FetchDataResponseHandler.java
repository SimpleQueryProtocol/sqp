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
import io.sqp.core.messages.EndOfDataMessage;
import io.sqp.core.messages.RowDataMessage;
import io.sqp.core.messages.SqpMessage;

import java.util.concurrent.CompletableFuture;

/**
 * @author Stefan Burnicki
 */
public class FetchDataResponseHandler extends ResponseHandler<CursorImpl> {
    private boolean _receivedData;
    private CursorImpl _cursor;

    public FetchDataResponseHandler(CursorImpl affectedCursor) {
        super(new CompletableFuture<>());
        _cursor = affectedCursor;
    }

    @Override
    public boolean handle(SqpMessage message) throws UnexpectedMessageException, DecodingException {
        switch (message.getType()) {
            case ReadyMessage:
                // don't bother if there are ready messages before getting a result
                if (_receivedData) {
                    throw new UnexpectedMessageException("retrieving result data", message);
                }
                return false;

            case RowDataMessage:
                _receivedData = true;
                RowDataMessage dataMsg = message.secureCast();
                _cursor.addDataRow(dataMsg.getData());
                return false;

            case EndOfDataMessage:
                EndOfDataMessage eodMsg = message.secureCast();
                _cursor.setHasMoreData(eodMsg.hasMore());
                succeed(_cursor);
                return true;

            default:
                throw new UnexpectedMessageException("waiting for result data", message);
        }
    }
}
