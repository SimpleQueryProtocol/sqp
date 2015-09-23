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

import io.sqp.core.messages.SqpMessage;
import io.sqp.client.exceptions.UnexpectedMessageException;
import io.sqp.core.exceptions.DecodingException;
import io.sqp.core.messages.MessageType;

import java.util.concurrent.CompletableFuture;

/**
 * @author Stefan Burnicki
 */
public class ConfirmationResponseHandler extends ResponseHandler<Void> {
    private MessageType _confirmationType;
    private String _errorMessage;

    protected ConfirmationResponseHandler(CompletableFuture<Void> future, MessageType confirmationType, String errorMessage) {
        super(future);
        _confirmationType = confirmationType;
        _errorMessage = errorMessage;
    }

    @Override
    public boolean handle(SqpMessage message) throws UnexpectedMessageException, DecodingException {
        if (message.isA(MessageType.ReadyMessage)) {
            return false; // Ready messages are okay
        } else if (message.isA(_confirmationType)) {
            succeed(null);
            return true;
        }
        throw new UnexpectedMessageException(_errorMessage, message);
    }
}
