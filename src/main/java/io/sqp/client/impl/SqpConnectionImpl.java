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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.sqp.client.SqpConnection;
import io.sqp.client.exceptions.ConnectionException;
import io.sqp.client.exceptions.SqpIOException;
import io.sqp.core.*;
import io.sqp.core.exceptions.InvalidOperationException;
import io.sqp.core.exceptions.TypeConversionException;
import io.sqp.core.jackson.JacksonObjectMapperFactory;
import io.sqp.core.messages.*;
import org.glassfish.tyrus.client.ClientManager;
import io.sqp.client.ClientConfig;
import io.sqp.client.PreparedStatement;
import io.sqp.client.QueryResult;
import io.sqp.client.exceptions.UnexpectedMessageException;
import io.sqp.client.exceptions.UnexpectedResultTypeException;
import io.sqp.core.exceptions.SqpException;
import io.sqp.core.jackson.JacksonMessageEncoder;
import io.sqp.core.types.SqpTypeCode;

import javax.websocket.*;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * @author Stefan Burnicki
 */
public class SqpConnectionImpl extends Endpoint implements SqpConnection {
    private static final int MAX_MSG_BUFFER_SIZE = 32 * 1024;

    private ConnectionState _state;
    private Session _session;
    private RemoteEndpoint.Basic _endpoint;
    private SqpException _pendingError;
    private Logger _logger;
    private MessageEncoder _messageEncoder;
    private CompletableFuture<SqpConnection> _connectionFuture;
    private String _database;
    private ClientConfig _config;
    private Map<String, CloseableServerResource> _openServerResources;
    private long _cursorId;
    private long _statementId;
    private boolean _autocommit;
    private LobManager _lobManager;
    private io.sqp.client.impl.MessageHandler _messageHandler;
    private final ExecutorService _sendingService;

    public SqpConnectionImpl(ClientConfig config) {
        _logger = Logger.getGlobal();
        _messageEncoder = new JacksonMessageEncoder();
        _state = ConnectionState.Uninitialized;
        _config = config;
        _cursorId = 0;
        _statementId = 0;
        _openServerResources = new HashMap<>();
        _autocommit = true;
        _lobManager = new LobManager(this);
        _sendingService = Executors.newSingleThreadExecutor(); // important that this only one thread to assure correct order
    }

    @Override
    protected void finalize() throws Throwable {
        _sendingService.shutdownNow();
    }

    public synchronized void close() throws IOException {
        if (_state == ConnectionState.Closed) {
            return;
        }
        boolean wasConnecting = _state.equals(ConnectionState.Connecting);
        _state = ConnectionState.Closed;

        // fail all futures if some are left
        if (wasConnecting && _connectionFuture != null) {
            _connectionFuture.completeExceptionally(new ConnectionException("Connection closed."));
            _connectionFuture = null;
        } else if (_messageHandler != null) {
            _messageHandler.close();
        }

        // close all open server resources (statements & cursors)
        closeServerResources(_openServerResources.values());
        if (_session != null) {
            _logger.log(Level.INFO, "Closing connection.");
            _session.close();
        }
        _session = null;
        _sendingService.shutdown();
    }

    public boolean isConnected() {
        return (_state.equals(ConnectionState.ReadyToSend) && _session != null);
    }

    @Override
    public synchronized <T extends QueryResult> CompletableFuture<T> execute(Class<T> resultClass, boolean scrollable, String query) {
        ExecuteResponseHandler<T> responseHandler = new ExecuteResponseHandler<>(resultClass, this, true);
        CompletableFuture<T> future = responseHandler.getAffectedFuture();
        if (!checkOpenAndNoErrors(responseHandler.getAffectedFuture())) {
            return future;
        }
        String cursorId = generateNewCursorId();
        send(new SimpleQueryMessage(query, scrollable, cursorId, _config.getCursorMaxFetch()), responseHandler);
        return future;
    }

    @Override
    public synchronized PreparedStatement prepare(String query) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        String stmtId = generateNewStatementId();
        PreparedStatementImpl stmt = new PreparedStatementImpl(this, _lobManager, stmtId, future);
        // well, it is pretty awkward that we return a prepared stmt from which we know that it will fail
        // TODO: fix that awkwardness
        if (!checkOpenAndNoErrors(future)) {
            return stmt;
        }
        send(new PrepareQueryMessage(query, stmtId),
             new ConfirmationResponseHandler(future, MessageType.PrepareCompleteMessage, "waiting for a parse complete"));
        return stmt;
    }

    @Override
    public synchronized CompletableFuture<SqpConnection> connect(String host, int port, String path, String database) {
        _state = ConnectionState.Connecting;
        _database = database;
        ClientManager clientManager = ClientManager.createClient();
        ClientEndpointConfig cec = ClientEndpointConfig.Builder.create().build();
        String scheme = "ws";
        _connectionFuture = new CompletableFuture<>();

        URI uri;
        try {
            uri = new URI(scheme, null, host, port, path, null, null);
            _logger.log(Level.INFO, "Attempting to connect to '" + uri + "'");
            clientManager.connectToServer(this, cec, uri);
        } catch (URISyntaxException | DeploymentException | IOException e) {
            _connectionFuture.completeExceptionally(new ConnectionException("Failed to connect to server: " + e.getMessage(), e));
        }
        return _connectionFuture;
    }

    @Override
    public synchronized CompletableFuture<Void> setAutoCommit(boolean useAutoCommit) {
        if (useAutoCommit == _autocommit) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        if (!checkOpenAndNoErrors(future)) {
            return future;
        }
        send(new SetFeatureMessage().setAutoCommit(useAutoCommit),
             new ConfirmationResponseHandler(future, MessageType.SetFeatureCompleteMessage,
                     "waiting for a server settings complete message"));
        return future.thenApply(v -> {
            _autocommit = useAutoCommit;
            return null;
        });
    }

    @Override
    public boolean getAutoCommit() {
        return _autocommit;
    }

    @Override
    public CompletableFuture<Void> commit() {
        return finishTransaction(true);
    }

    @Override
    public CompletableFuture<Void> rollback() {
        return finishTransaction(false);
    }

    @Override
    public <T> CompletableFuture<T> getInformation(Class<T> infoType, InformationSubject subject) {
        return getInformation(infoType, subject, null);
    }

    @Override
    public CompletableFuture<String> getTypeSchema(String typeName) {
        return getInformation(String.class, InformationSubject.TypeSchema, typeName);
    }

    @Override
    public CompletableFuture<String> registerTypeMapping(String name, String schema, String... keywords) {
        CompletableFuture<String> future = new CompletableFuture<>();
        if (!checkOpenAndNoErrors(future)) {
            return future;
        }
        ObjectMapper objectMapper = JacksonObjectMapperFactory.objectMapper(_config.getProtocolFormat());
        JsonNode schemaNode;
        try {
            schemaNode = objectMapper.readTree(schema);
        } catch (IOException e) {
            SqpException error = new TypeConversionException("The schema could not be parsed: " + e.getMessage(), e);
            future.completeExceptionally(error);
            return future;
        }

        send(new TypeMappingMessage(name, schemaNode, Arrays.asList(keywords)), new ResponseHandler<>(future, m -> {
            if (m.isA(MessageType.ReadyMessage)) {
                return false; // just ignore them
            } else if (m.isA(MessageType.TypeMappingRegisteredMessage)) {
                TypeMappingRegisteredMessage response = m.secureCast();
                future.complete(response.getNative());
                return true;
            }
            throw new UnexpectedMessageException("waiting for information response", m);
        }));
        return future;
    }

    public synchronized <T extends QueryResult> CompletableFuture<T> execute(Class<T> resultClass, String statementId,
                                                                List<SqpTypeCode> parameterTypes, List<String> customTypes,
                                                                List<List<Object>> parameters, boolean scrollable) {
        ExecuteResponseHandler<T> responseHandler = new ExecuteResponseHandler<>(resultClass, this, false);
        CompletableFuture<T> future = responseHandler.getAffectedFuture();
        if (!checkOpenAndNoErrors(future)) {
            return future;
        }
        String cursorId = generateNewCursorId();
        send(new ExecuteQueryMessage(statementId, cursorId, parameterTypes, customTypes, parameters, scrollable), responseHandler);
        return future;
    }

    public CompletableFuture<CursorImpl> fetch(CursorImpl cursor, boolean forward) {
        FetchDataResponseHandler responseHandler = new FetchDataResponseHandler(cursor);
        CompletableFuture<CursorImpl> future = responseHandler.getAffectedFuture();
        if (!checkOpenAndNoErrors(future)) {
            return future;
        }
        // TODO: correctly set position parameter
        send(new FetchDataMessage(cursor.getId(), -1, _config.getCursorMaxFetch(), forward), responseHandler);
        return future;
    }

    private CompletableFuture<Void> finishTransaction(boolean commit) {
        String mode = commit ? "commit" : "rollback";
        CompletableFuture<Void> future = new CompletableFuture<>();
        if (_autocommit) {
            future.completeExceptionally(new InvalidOperationException("Cannot " + mode + " a transaction in autocommit mode"));
            return future;
        }
        if (!checkOpenAndNoErrors(future)) {
            return future;
        }
        SqpMessage finishMessage = commit ? new CommitTransactionMessage() : new RollbackTransactionMessage();
        send(finishMessage,
             new ConfirmationResponseHandler(future, MessageType.TransactionFinishedMessage,
                     "waiting for a " + mode + " complete confirmation"));
        return future;
    }

    private <T> CompletableFuture<T> getInformation(Class<T> infoType, InformationSubject subject, String detail) {
        CompletableFuture<T> future = new CompletableFuture<>();
        if (!checkOpenAndNoErrors(future)) {
            return future;
        }
        send(new InformationRequestMessage(subject, detail), new ResponseHandler<>(future, m -> {
            if (m.isA(MessageType.ReadyMessage)) {
                return false; // just ignore them
            } else if (m.isA(MessageType.InformationResponseMessage)) {
                InformationResponseMessage response = m.secureCast();
                Object value = response.getValue();
                if (!response.getResponseType().isCompatibleTo(infoType)) {
                    future.completeExceptionally(new UnexpectedResultTypeException(value, infoType));
                } else {
                    future.complete(infoType.cast(value));
                }
                return true;
            }
            throw new UnexpectedMessageException("waiting for information response", m);
        }));
        return future;
    }

    CompletableFuture<Void> send(SqpMessage msg, ResponseHandler responseHandler) {
        // TODO: use a timeout for response handler!
        if (responseHandler != null) {
            _messageHandler.addResponseHandler(responseHandler);
        }

        // TODO: optionally depend on previous future, so consecutive operations aren't executed if one fails?
        // as we always use the same, single threaded ExecutionService to run this send operation, all operations
        // are queued. This allows async-operations while assuring their order is kept
        return CompletableFuture.runAsync(() -> {
            try {
                if (_config.getProtocolFormat() == DataFormat.Binary) {
                    _messageEncoder.encode(_endpoint.getSendStream(), DataFormat.Binary, msg);
                } else {
                    _messageEncoder.encode(_endpoint.getSendWriter(), msg);
                }
            } catch (IOException e) {
                throw new CompletionException(new SqpIOException(e));
            }
        }, _sendingService).exceptionally(new FailHandler(this));
    }

    CompletableFuture<Void> send(InputStream stream) {
        // TODO: optionally depend on previous future, so consecutive operations aren't executed if one fails?
        int bufSize = Math.min(MAX_MSG_BUFFER_SIZE, _session.getMaxBinaryMessageBufferSize());
        return CompletableFuture.runAsync(() -> {
            byte[] buffer = new byte[bufSize];
            int bytesRead;
            try (OutputStream output = _endpoint.getSendStream()) {
                while ((bytesRead = stream.read(buffer)) > 0) {
                    output.write(buffer, 0, bytesRead);
                }
            } catch (IOException e) {
                throw new CompletionException(new SqpIOException(e));
            }
        }, _sendingService).exceptionally(new FailHandler(this));
    }

    CompletableFuture<Void> send(Reader reader) {
        int bufSize = Math.min(MAX_MSG_BUFFER_SIZE, _session.getMaxTextMessageBufferSize());
        return CompletableFuture.runAsync(() -> {
            char[] buffer = new char[bufSize];
            int bytesRead;
            try (Writer output = _endpoint.getSendWriter()) {
                while ((bytesRead = reader.read(buffer)) > 0) {
                    output.write(buffer, 0, bytesRead);
                }
            } catch (IOException e) {
                throw new CompletionException(new SqpIOException(e));
            }
        }, _sendingService).exceptionally(new FailHandler(this));
    }


    @Override
    public void onOpen(Session session, EndpointConfig config) {
        // TODO: check if we're already connected
        _session = session;
        _logger.log(Level.INFO, "Established connection.");
        _endpoint = _session.getBasicRemote();
        _messageHandler = new io.sqp.client.impl.MessageHandler(this, _session, _logger);

        send(new HelloMessage(_database), new ResponseHandler<>(_connectionFuture, m -> {
            if (m.isA(MessageType.ReadyMessage)) {
                _state = ConnectionState.ReadyToSend;
                _connectionFuture.complete(this);
                _connectionFuture = null;
                return true;
            }
            throw new UnexpectedMessageException("waiting for response for hello", m);
        }));
    }

    @Override
    public synchronized void onClose(Session session, CloseReason closeReason) {
        if (_state.equals(ConnectionState.Closed)) {
            return;
        }
        _logger.log(Level.INFO, "Connection closed by server.");
        _session = null;
        try {
            close();
        } catch (IOException e) {
            _logger.log(Level.WARNING, "Closing the connection after an external close failed.", e);
        }
    }

    @Override
    public synchronized void onError(Session session, Throwable thr) {
        SqpException e = new SqpException(ErrorType.Unknown, "An error occurred: " + thr.getMessage(),
                ErrorAction.Abort, thr);
        handleError(e);
    }

    @Override
    public CompletableFuture<Void> allowReceiveNativeTypes(String... nativeTypes) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        if (!checkOpenAndNoErrors(future)) {
            return future;
        }
        send(new SetFeatureMessage().setAllowedNativeTypes(nativeTypes),
                new ConfirmationResponseHandler(future, MessageType.SetFeatureCompleteMessage,
                        "waiting for a server settings complete message"));
        return future;
    }

    public CompletableFuture<byte[]> getLob(String id, long position) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        if (!checkOpenAndNoErrors(future)) {
            return future;
        }
        send(new LobRequestMessage(id, position, _config.getLobBufferSize()), new ResponseHandler<>(future, m -> {
            if (m.isA(MessageType.ReadyMessage)) {
                return false; // just ignore them
            } else if (m.isA(MessageType.LobAnnouncementMessage)) {
                _messageHandler.setLobConsumer(future);
                return true;
            }
            throw new UnexpectedMessageException("waiting for information response", m);
        }));
        return future;
    }

    void registerOpenServerResource(CloseableServerResource resource) {
        _openServerResources.put(resource.getId(), resource);
    }

    CompletableFuture<Void> closeServerResources(Collection<CloseableServerResource> resources) {
        if (resources.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> future = new CompletableFuture<>();

        List<String> cursorIds = resources.stream()
                .filter(r -> r instanceof CursorImpl)
                .map(CloseableServerResource::getId)
                .collect(Collectors.toList());
        List<String> statementIds = resources.stream()
                .filter(r -> r instanceof PreparedStatementImpl)
                .map(CloseableServerResource::getId)
                .collect(Collectors.toList());
        // now set their status to closed
        resources.forEach(CloseableServerResource::setClosed);

        // remove them from the open resource list
        // TODO: wait for success?
        cursorIds.forEach(_openServerResources::remove);
        statementIds.forEach(_openServerResources::remove);

        send(new ReleaseMessage(cursorIds, statementIds),
                new ConfirmationResponseHandler(future, MessageType.ReleaseCompleteMessage,
                        "waiting for a cursor/statement release confirmation"));
        return future;
    }

    void setState(ConnectionState state) {
        _state = state;
    }

    void handleError(SqpException error) {
        _logger.log(Level.INFO, "Handling Error: " + error.getMessage(), error);
        _state = ConnectionState.Error;
        // If there is a future affected, fail it

        if (!_messageHandler.failCurrentResponseHandler(error)) {
            _pendingError = error;
        }
        // TODO: other error handling? like aborting transactions
        // depending on the error type that might already have happened (ServerErrorMessage)
        if (error.getErrorAction().equals(ErrorAction.Abort)) {
            try {
                close();
            } catch (IOException e) {
                _logger.log(Level.WARNING, "Closing the connection after an error failed.", e);
            }
        }
    }

    private String generateNewCursorId() {
        _cursorId++;
        return "Cursor" + _cursorId;
    }

    private String generateNewStatementId() {
        _statementId++;
        return "Statement" + _statementId;
    }

    private boolean checkOpenAndNoErrors(CompletableFuture<?> future) {
        if (_pendingError != null) {
            future.completeExceptionally(_pendingError);
            _pendingError = null;
            return false;
        }
        if (!_state.equals(ConnectionState.ReadyToSend) && !_state.equals(ConnectionState.Connecting)) {
            future.completeExceptionally(new ConnectionException(_state.getDescription()));
            return false;
        }
        return true;
    }
}
