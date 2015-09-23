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

import io.sqp.core.messages.LobAnnouncementMessage;
import io.sqp.core.messages.MessageType;

import java.io.InputStream;
import java.io.Reader;
import java.util.concurrent.CompletableFuture;

/**
 * @author Stefan Burnicki
 */
public class LobManager {
    private static final String CLOB_ID_PREFIX = "CLOB";
    private static final String BLOB_ID_PREFIX = "BLOB";
    private int _lobCounter;
    private final SqpConnectionImpl _connection;

    public LobManager(SqpConnectionImpl connection) {
        _connection = connection;
        _lobCounter = 0;
    }

    public CompletableFuture<Void> create(String id, Reader charStream) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        synchronized (_connection) {
            _connection.send(new LobAnnouncementMessage(id),
                    new ConfirmationResponseHandler(future, MessageType.LobReceivedMessage, "waiting for a CLOB confirmation"));
            _connection.send(charStream);
        }
        return future;
    }

    public CompletableFuture<Void> create(String id, InputStream binStream) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        synchronized (_connection) {
            _connection.send(new LobAnnouncementMessage(id),
                    new ConfirmationResponseHandler(future, MessageType.LobReceivedMessage, "waiting for a BLOB confirmation"));
            _connection.send(binStream);
        }
        return future;
    }

    public String createClobId() {
        return createLobId(CLOB_ID_PREFIX);
    }

    public String createBlobId() {
        return createLobId(BLOB_ID_PREFIX);
    }

    private String createLobId(String prefix) {
        _lobCounter++;
        return prefix + "_" + _lobCounter;
    }
}
