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

package io.sqp.core.messages;

import io.sqp.core.exceptions.DecodingException;

/**
 * @author Stefan Burnicki
 */
public enum MessageType {
    DummyMessage('_', DummyMessage.class, true),
    ErrorMessage('!', ErrorMessage.class, true),
    CloseMessage('.', CloseMessage.class, false),
    ReadyMessage('r', ReadyMessage.class, false),

    PrepareCompleteMessage('p', PrepareCompleteMessage.class, false),
    ReleaseCompleteMessage('l', ReleaseCompleteMessage.class, false),
    SetFeatureCompleteMessage('t', SetFeatureCompleteMessage.class, false),
    TransactionFinishedMessage('k', TransactionFinishedMessage.class, false),
    TypeMappingRegisteredMessage('m', TypeMappingRegisteredMessage.class, true),
    LobReceivedMessage('o', LobReceivedMessage.class, false),

    CursorDescriptionMessage('c', CursorDescriptionMessage.class, true),
    RowDataMessage('#', RowDataMessage.class, true),
    ExecuteCompleteMessage('x', ExecuteCompleteMessage.class, true),
    EndOfDataMessage('e', EndOfDataMessage.class, true),

    InformationResponseMessage('i', InformationResponseMessage.class, true),

    HelloMessage('H', HelloMessage.class, true),
    AuthenticationResponseMessage('A', AuthenticationResponseMessage.class, true),
    SimpleQueryMessage('S', SimpleQueryMessage.class, true),

    PrepareQueryMessage('P', PrepareQueryMessage.class, true),
    ExecuteQueryMessage('X', ExecuteQueryMessage.class, true),
    FetchDataMessage('F', FetchDataMessage.class, true),

    ReleaseMessage('L', ReleaseMessage.class, true),
    InformationRequestMessage('I', InformationRequestMessage.class, true),
    SetFeatureMessage('T', SetFeatureMessage.class, true),
    TypeMappingMessage('M', TypeMappingMessage.class, true),

    CommitTransactionMessage('K', CommitTransactionMessage.class, false),
    RollbackTransactionMessage('B', RollbackTransactionMessage.class, false),

    LobAnnouncementMessage('*', LobAnnouncementMessage.class, true),
    LobRequestMessage('G', LobRequestMessage.class, true);

    char _id;
    Class<? extends SqpMessage> _type;
    boolean _hasContent;

    MessageType(char id, Class<? extends SqpMessage> messageType, boolean hasContent) {
        _id = id;
        _type = messageType;
        _hasContent = hasContent;
    }

    // TODO: make sure to *not* throw an InvalidFrameException, but something "neutral"
    public static MessageType fromId(char id) throws DecodingException {
        for (MessageType type : MessageType.values()) {
            if (id == type._id) {
                return type;
            }
        }
        throw new DecodingException("A message with identifier '" + id + "' does not exist.");
    }

    public boolean hasContent() {
        return _hasContent;
    }

    public char getId() {
        return _id;
    }

    public Class<? extends SqpMessage> getType() {
        return _type;
    }
}
