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

package io.sqp.proxy;

import io.sqp.core.ErrorAction;
import io.sqp.core.messages.MessageType;

import java.util.Arrays;
import java.util.EnumSet;

/**
 * @author Stefan Burnicki
 */
public enum ClientSessionState {
    Dead(false, ErrorAction.Abort),

    Uninitialised(false, ErrorAction.Abort,
            MessageType.HelloMessage),

    Connecting(true, ErrorAction.Abort),

    Authenticating(false, ErrorAction.Abort,
            MessageType.AuthenticationResponseMessage),

    Ready(false, ErrorAction.Recover,
            MessageType.SimpleQueryMessage,
            MessageType.PrepareQueryMessage,
            MessageType.ExecuteQueryMessage,
            MessageType.FetchDataMessage,
            MessageType.ReleaseMessage,
            MessageType.SetFeatureMessage,
            MessageType.RollbackTransactionMessage,
            MessageType.CommitTransactionMessage,
            MessageType.InformationRequestMessage,
            MessageType.TypeMappingMessage,
            MessageType.LobAnnouncementMessage,
            MessageType.LobRequestMessage),

// TODO: unify some of these states?
    SimpleExecuting(true, ErrorAction.Recover),
    PreparingQuery(true, ErrorAction.Recover),
    ExecutingQuery(true, ErrorAction.Recover),
    ReleasingCursor(true, ErrorAction.Recover),
    FetchingData(true, ErrorAction.Recover),
    SettingFeature(true, ErrorAction.Recover),
    RequestingInformation(true, ErrorAction.Recover),
    FinishingTransaction(true, ErrorAction.Recover),
    RegisteringTypeMapping(true, ErrorAction.Recover),
    WaitingForLob(false, ErrorAction.Recover),
    GettingLob(true, ErrorAction.Recover),
    Closing(true, ErrorAction.Abort);

    private boolean _isBlocking;
    private EnumSet<MessageType> _processableMessages;
    private ErrorAction _errorAction;

    ClientSessionState(boolean isBlocking, ErrorAction errorAction, MessageType... processableMessages) {
        _isBlocking = isBlocking;
        _errorAction = errorAction;
        // EnumSet.copyOf() fails with empty collection, so we initialize an empty one and fill it
        _processableMessages = EnumSet.noneOf(MessageType.class);
        _processableMessages.addAll(Arrays.asList(processableMessages));
    }

    public boolean isBlocking() {
        return _isBlocking;
    }

    public EnumSet<MessageType> getProcessableMessages() {
        return _processableMessages;
    }

    public ErrorAction getErrorAction() {
        return _errorAction;
    }

    public boolean canProcess(MessageType type) {
        return _processableMessages.contains(type);
    }
}
