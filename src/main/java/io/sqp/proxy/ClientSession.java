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

import com.fasterxml.jackson.databind.JsonNode;
import io.sqp.backend.*;
import io.sqp.backend.results.*;
import io.sqp.core.DataFormat;
import io.sqp.core.ErrorAction;
import io.sqp.core.ErrorType;
import io.sqp.core.exceptions.BackendErrorException;
import io.sqp.core.exceptions.NotImplementedException;
import io.sqp.core.messages.*;
import io.sqp.core.types.SqpValue;
import io.sqp.proxy.customtypes.CustomTypeMapper;
import io.sqp.proxy.exceptions.*;
import io.sqp.core.exceptions.SqpException;
import io.sqp.core.messages.CloseMessage;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * @author Stefan Burnicki
 */
// TODO: really enter ready state always, even after a complete message and nice answers?
public class ClientSession implements ErrorHandler {
    private ClientConnection _clientConnection;
    private BackendConnectionPool _backendConnectionPool;
    private ClientSessionState _state;
    private BackendConnection _backendConnection;
    private long _backendConnectionId = -1;
    private TransactionState _transactionState;
    private InformationProvider _informationProvider;
    private CustomTypeMapper _customTypeMapper;
    private Queue<SqpMessage> _messageQueue;
    private Map<String, InputStream> _currentLobs;
    private String _awaitedLob;

    protected Logger logger;

    public ClientSession(BackendConnectionPool connectionPool, ClientConnection connection) {
        _clientConnection = connection;
        _state = ClientSessionState.Uninitialised;
        _backendConnectionPool = connectionPool;
        logger = Logger.getGlobal();
        _informationProvider = new InformationProvider(logger, _backendConnectionPool.getBackend().getTypeRepository());
        _customTypeMapper = new CustomTypeMapper(logger, _backendConnectionPool.getBackend().getTypeRepository());
        _transactionState = TransactionState.AutoCommit;
        _messageQueue = new LinkedList<>();
        _currentLobs = new HashMap<>();
    }

    public void recover() {
        // make sure we are in a state that we can recover from
        if (_state.equals(ClientSessionState.Dead) ||
            _state.equals(ClientSessionState.Uninitialised) ||
            _state.equals(ClientSessionState.Authenticating) ||
            _state.equals(ClientSessionState.Connecting)) {
            handleError(new InvalidStateException("Cannot recover from state '" + _state + "'. Closing.",
                    ErrorAction.Abort));
            return;
        }

        // TODO: rollback the current transaction if active and send a warning?
        _clientConnection.sendMessage(new ReadyMessage());
        enterReadyState();
    }

    public void processMessage(SqpMessage message) {
        _messageQueue.add(message);
        processMessageQueue();
    }

    public void registerLob(DataFormat format, InputStream data) {
        String clobBlob = format == DataFormat.Binary ? "BLOB" : "CLOB";
        if (!validateState("to register the new " + clobBlob, ClientSessionState.WaitingForLob)) {
            return;
        }
        InputStream existingLob = _currentLobs.get(_awaitedLob);
        if (existingLob != null) {
            logger.log(Level.WARNING, "Closing existing LOB " + _awaitedLob);
            // TODO: send warning? or even error?
            try {
                existingLob.close();
            } catch (IOException e) {
                logger.log(Level.WARNING, "Failed to close existing lob stream", e);
                // something else to do?
            }
        }
        _currentLobs.put(_awaitedLob, data);
        _clientConnection.sendMessage(new LobReceivedMessage());
        enterReadyState();
    }

    public void onClientClose() {
        logger.log(Level.INFO, "Client disconnected.");
        processMessage(new CloseMessage());
    }

    public ClientSessionState getState() {
        return _state;
    }

    @Override
    public void handleError(SqpException error) {
        logger.log(Level.INFO, "Handling an " + error.getErrorType() + " error: " + error.getMessage());
        _clientConnection.sendMessage(error.toErrorMessage());
        switch (error.getErrorAction()) {
            case Recover:
                // TODO: maybe reset the message queue?
                _clientConnection.reset();
                recover();
                break;

            default: // It's only Abort left (and new error actions that were forgotten to handle)
                _state = ClientSessionState.Dead;
                logger.log(Level.INFO, "Aborting: Closing connection to client");
                _clientConnection.close();
        }
    }

    private void processMessageQueue() {
        // check if there is a message to process
        if (_messageQueue.isEmpty()) {
            return;
        }
        // TODO: refactor
        SqpMessage message = _messageQueue.peek();
        try {
            // validate and check if we are allowed to proceed or need to wait
            if (!_state.canProcess(message.getType())) {
                // in a blocking state, just return and handle this message another time
                if (_state.isBlocking()) {
                    return;
                }
                // otherwise discard the message and throw an error
                _clientConnection.setMessageFormat(message.getMessageFormat()); // answer in same format
                _messageQueue.poll();
                // not blocking and cannot process: fail
                String allowed = _state.getProcessableMessages().stream().map(Object::toString).collect(Collectors.joining(", "));
                throw new UnexpectedMessageException("Cannot process message of type '" + message.getType() +
                        "' in state " + _state + ". Allowed message types are: " + allowed + ".", _state.getErrorAction());
            }
            // make sure we answer in the same format
            _clientConnection.setMessageFormat(message.getMessageFormat());
            _messageQueue.poll(); // actually remove the msg from queue
            processMessageInternal(message);
        } catch (Throwable e) {
            if (e instanceof SqpException) {
                handleError((SqpException) e);
            } else {
                handleError(new BackendErrorException("Unexpected error in backend: " + e.getMessage(), e));
            }
        }
    }

    private void processMessageInternal(SqpMessage message) throws SqpException {
        switch (message.getType()) {
            case HelloMessage:
                startAuthentication(message.secureCast());
                break;

            case SimpleQueryMessage:
                executeSimpleQuery(message.secureCast());
                break;

            case PrepareQueryMessage:
                executePrepareQuery(message.secureCast());
                break;

            case ExecuteQueryMessage:
                executeExecuteQuery(message.secureCast());
                break;

            case FetchDataMessage:
                executeFetchData(message.secureCast());
                break;

            case ReleaseMessage:
                executeRelease(message.secureCast());
                break;

            case SetFeatureMessage:
                executeSetFeature(message.secureCast());
                break;

            case CommitTransactionMessage:
                executeFinishTransaction(true);
                break;

            case RollbackTransactionMessage:
                executeFinishTransaction(false);
                break;

            case InformationRequestMessage:
                executeInformationRequest(message.secureCast());
                break;

            case TypeMappingMessage:
                executeTypeMapping(message.secureCast());
                break;

            case LobAnnouncementMessage:
                executeLobAnnouncement(message.secureCast());
                break;

            case LobRequestMessage:
                executeLobRequest(message.secureCast());
                break;

            case CloseMessage:
                executeCloseConnection();
                break;

            default:
                throw new NotImplementedException("Message processing of type '"
                        + message.getType() + "' is not yet implemented");
        }
    }

    public void executeCloseConnection() {
        // TODO: somehow pass something so the connection pool can tidy up the queue
        _state = ClientSessionState.Closing;
        _backendConnectionPool.closeConnection(_backendConnectionId);
        _state = ClientSessionState.Dead;
    }

    private void executeLobRequest(LobRequestMessage lobRequest) {
        _state = ClientSessionState.GettingLob;
        String id = lobRequest.getId();
        _backendConnection.getLob(id, lobRequest.getOffset(), lobRequest.getSize(), new ResultHandler<>(this, lobStream -> {
            _clientConnection.sendMessage(new LobAnnouncementMessage(id));
            _clientConnection.sendStream(lobStream.getInputStream(), lobStream.getDataFormat());
            enterReadyState();
        }));
    }

    private void executeLobAnnouncement(LobAnnouncementMessage lobAnnouncementMessage) {
        _state = ClientSessionState.WaitingForLob;
        _awaitedLob = lobAnnouncementMessage.getId();
    }

    private void executeTypeMapping(TypeMappingMessage mapping) {
        if (!validateState("register a type mapping", ClientSessionState.Ready)) {
            return;
        }
        _state = ClientSessionState.RequestingInformation;
        try {
            String name = mapping.getName();
            if (name == null || name.isEmpty()) {
                throw new TypeMappingNotPossibleException("Type name is null or empty");
            }
            JsonNode schema = mapping.getSchema();
            if (schema == null) {
                throw new TypeMappingNotPossibleException("Schema is null");
            }

            _customTypeMapper.registerMapping(name, schema, mapping.getKeywords(), new ResultHandler<>(this, nativeType -> {
                _clientConnection.sendMessage(new TypeMappingRegisteredMessage(nativeType));
                enterReadyState();
            }));
        } catch (SqpException e) {
            handleError(e);
        }
    }

    private void executeInformationRequest(InformationRequestMessage request) {
        if (!validateState("answer an information request", ClientSessionState.Ready)) {
            return;
        }
        _state = ClientSessionState.RequestingInformation;

        // first handle the proxy-wide requests
        InformationRequestResult proxyResponse;
        try {
            proxyResponse = _informationProvider.get(request.getSubject(), request.getDetail());
        } catch (SqpException e) {
            handleError(e);
            return;
        }
        // check if we already have an answer
        if (proxyResponse != InformationRequestResult.DELEGATE) {
            sendInformationResponse(proxyResponse);
            return;
        }

        // otherwise delegate the request to the backend
        _backendConnection.getInformation(request.getSubject(), request.getDetail(), new ResultHandler<>(this, result -> {
            if (result != InformationRequestResult.UNKNOWN && result != InformationRequestResult.DELEGATE) {
                sendInformationResponse(result);
                return;
            }
            // otherwise get the defaults
            try {
                sendInformationResponse(_informationProvider.getDefaults(request.getSubject(), request.getDetail()));
            } catch (SqpException e) {
                handleError(e);
            }
        }));
    }

    private void sendInformationResponse(InformationRequestResult result) {
        InformationResponseMessage responseMessage = new InformationResponseMessage(result.getType(), result.getValue());
        _clientConnection.sendMessage(responseMessage);
        enterReadyState();
    }

    private void executeFinishTransaction(boolean commit) {
        if (_transactionState.equals(TransactionState.AutoCommit)) {
            handleError(new SqpException(ErrorType.InvalidOperation,
                    "Rollback/Commit are not valid operations in AutoCommit mode.", ErrorAction.Recover));
            return;
        }
        if (!validateState("commit or roll back a transaction", ClientSessionState.Ready)) {
            return;
        }
        _state = ClientSessionState.FinishingTransaction;
        SuccessHandler handler = new SuccessHandler(this, () -> {
            _transactionState = commit ? TransactionState.Committed : TransactionState.Aborted;
           _clientConnection.sendMessage(new TransactionFinishedMessage());
            enterReadyState();
        });
        if (commit) {
            _backendConnection.commit(handler);
        } else {
            _backendConnection.rollback(handler);
        }
    }

    private void executeSetFeature(SetFeatureMessage setFeatureMessage) {
        if (!validateState("enable or disable a feature", ClientSessionState.Ready)) {
            return;
        }
        _state = ClientSessionState.SettingFeature;
        // TODO: set some proxy wide features first. e.g. support for compressed decimals
        List<FeatureSetting<?>> featureSettings = featureMessageToFeatureList(setFeatureMessage);
        if (featureSettings.size() < 1) {
            enterReadyState();
            return;
        }
        Boolean autocommit = setFeatureMessage.getAutoCommit();

        _backendConnection.setFeatures(featureSettings, new SuccessHandler(this, () -> {
            if (!validateState("process the results", ClientSessionState.SettingFeature)) {
                return;
            }
            if (autocommit != null) {
                _transactionState = autocommit ? TransactionState.AutoCommit : TransactionState.NoActiveTransaction;
            }
            _clientConnection.sendMessage(new SetFeatureCompleteMessage());
            enterReadyState();
        }));
    }

    private List<FeatureSetting<?>> featureMessageToFeatureList(SetFeatureMessage setFeatureMessage) {
        List<FeatureSetting<?>> featureList = new LinkedList<>();
        Boolean autoCommit = setFeatureMessage.getAutoCommit();
        if (autoCommit != null) {
            featureList.add(new FeatureSetting<>(FeatureSetting.Feature.AutoCommit, autoCommit));
        }
        String[] nativeTypes = setFeatureMessage.getAllowedNativeTypes();
        if (nativeTypes != null) {
            featureList.add(new FeatureSetting<>(FeatureSetting.Feature.AllowNativeTypes, nativeTypes));
        }
        return featureList;
    }

    private void executeRelease(ReleaseMessage releaseMessage) {
        if (!validateState("release statements or cursors", ClientSessionState.Ready)) {
            return;
        }
        _state = ClientSessionState.ReleasingCursor;
        _backendConnection.release(releaseMessage.getStatements(), releaseMessage.getCursors(), new SuccessHandler(this, () -> {
            _clientConnection.sendMessage(new ReleaseCompleteMessage());
            enterReadyState();
        }));
    }

    private void executePrepareQuery(PrepareQueryMessage prepareQueryMessage) {
        if (!validateState("parse a query", ClientSessionState.Ready)) {
            return;
        }
        _state = ClientSessionState.PreparingQuery;
        // TODO: validate that prepareQueryMessage.getQuery() is not null or empty. Throw error otherwise
        _backendConnection.prepare(prepareQueryMessage.getQuery(), prepareQueryMessage.getId(), new SuccessHandler(this, () -> {
            _clientConnection.sendMessage(new PrepareCompleteMessage());
            enterReadyState();
        }));
    }

    private void executeExecuteQuery(ExecuteQueryMessage message) {
        if (!validateState("execute a query", ClientSessionState.Ready)) {
            return;
        }
        _state = ClientSessionState.ExecutingQuery;
        // TODO: if early parameter decoding turns out to be too slow or memory consuming, we need to pass
        // parameterTypes, customTypes, and parameters to the backend connection's execute method
        List<List<SqpValue>> parameters;
        try {
            ParameterDecoder decoder = new ParameterDecoder(_customTypeMapper, message.getParameterTypes(), message.getCustomTypes(), _currentLobs);
            parameters = decoder.decodeParameterListBatch(message.getParameters());
        } catch (SqpException e) {
            handleError(e);
            return;
        }
        _backendConnection.execute(message.getStatementId(), message.getCursorId(), parameters,message.isScrollable(),
                new ResultHandler<>(this, this::handleExecuteQueryResult));
    }

    private void executeFetchData(FetchDataMessage fetchDataMessage) {
        if (!validateState("fetch data", ClientSessionState.Ready)) {
            return;
        }
        _state = ClientSessionState.FetchingData;
        String cursorId = fetchDataMessage.getCursorId();
        int pos = fetchDataMessage.getPosition();
        int num = fetchDataMessage.getMaxFetch();
        pos = pos >= 0 ? pos : -1;
        num = num >= 0 ? num : -1;

        _backendConnection.fetch(cursorId, pos, num, fetchDataMessage.isForward(),
                new ResultHandler<>(this, this::handleFetchDataResult));
    }

    private void executeSimpleQuery(SimpleQueryMessage simpleQueryMessage) {
        if (!validateState("execute a simple query", ClientSessionState.Ready)) {
            return;
        }
        _state = ClientSessionState.SimpleExecuting;
        // TODO: validate that simpleQueryMessage.getQuery() is not null or empty. Throw error otherwise
        _backendConnection.simpleQuery(simpleQueryMessage.getQuery(), simpleQueryMessage.getCursorId(),
                                       simpleQueryMessage.isScrollable(), simpleQueryMessage.getMaxFetch(),
                new ResultHandler<>(this, this::handleExecuteQueryResult));
    }

    private void handleFetchDataResult(QueryResult result) {
        if (!validateState("process the results", ClientSessionState.SimpleExecuting, ClientSessionState.FetchingData)) {
            return;
        }

        if (result instanceof RowDataResult) {
            // TODO: somehow we must pass the data format to be converted accordingly
            _clientConnection.sendMessage(RowDataMessage.fromTypedData(((RowDataResult) result).getData()));
        } else if (result instanceof EndQueryResult) {
            _clientConnection.sendMessage(new EndOfDataMessage(((EndQueryResult) result).hasMoreData()));
            enterReadyState();
        } else {
            logger.log(Level.SEVERE, "Unknown query result type '" + result.getClass().getName() + "'");
            handleError(new ServerErrorException("A server internal problem occurred processing the query results"));
        }
    }

    private void handleExecuteQueryResult(QueryResult result) {
        if (!validateState("process the results", ClientSessionState.SimpleExecuting, ClientSessionState.ExecutingQuery)) {
            return;
        }
        boolean isSimpleQuery = _state.equals(ClientSessionState.SimpleExecuting);

        if (!isSimpleQuery) {
            clearLobs();
        }
        // TODO: make this nicer
        if (result instanceof UpdateQueryResult) {
            _clientConnection.sendMessage(new ExecuteCompleteMessage(((UpdateQueryResult) result).getAffectedRows()));
            enterReadyState();
        } else if (result instanceof CursorDescriptionResult) {
            CursorDescriptionResult cursor = (CursorDescriptionResult) result;
            _clientConnection.sendMessage(new CursorDescriptionMessage(cursor.getCursorId(), cursor.isScrollable(), cursor.getColumns()));
            if (!isSimpleQuery) {
                enterReadyState();
            }
        } else if (isSimpleQuery) {
            // simple queries also fetch data
            handleFetchDataResult(result);
        } else {
            logger.log(Level.SEVERE, "Unknown query result type '" + result.getClass().getName() + "'");
            handleError(new ServerErrorException("A server internal problem occurred processing the query results"));
        }
    }

    private void startAuthentication(HelloMessage helloMessage) {
        if (!validateState("startAuthentication to backend", ClientSessionState.Uninitialised)) {
            return;
        }

        _state = ClientSessionState.Authenticating;
        // TODO: useful implementation that handles the client authentication calls endAuthentication afterwards

        handleEndAuthentication(helloMessage);
    }

    private void handleEndAuthentication(HelloMessage helloMessage) {
        if (!validateState("finish the authentication", ClientSessionState.Authenticating)) {
            return;
        }

        _state = ClientSessionState.Connecting;
        _backendConnectionId =_backendConnectionPool.createConnection(helloMessage.getDatabase(),
                this::handleBackendDisconnected, new ResultHandler<>(this, this::handleBackendConnected));
        if (_backendConnectionId < 0) {
            handleError(new SqpException(ErrorType.OverloadedServer,
                    "The server has already too many connections.", ErrorAction.Abort));
        }
    }

    private void handleBackendConnected(BackendConnection connection) {
        if(!validateState("set the connection and start authentication", ClientSessionState.Connecting)) {
            return;
        }
        // TODO: maybe we should send a HelloResponse with some basic information as protocol version
        _backendConnection = connection;
        _customTypeMapper.setBackendConnection(_backendConnection);
        logger.log(Level.INFO, "Connection to database backend was successful.");

        _clientConnection.sendMessage(new ReadyMessage());
        enterReadyState();
    }

    private void clearLobs() {
        _currentLobs.forEach((id, stream) -> {
            try {
                stream.close();
            } catch (IOException e) {
                logger.log(Level.INFO, "Stream of lob " + id + " is already closed");
            }
        });
        _currentLobs.clear();
    }

    private void enterReadyState() {
        _state = ClientSessionState.Ready;
        // important: start to process pending messages!
        processMessageQueue();
    }

    private void handleBackendDisconnected(String message) {
        logger.log(Level.INFO, "Backend disconnected.");
        // send error to the client and close the connection
        handleError(new BackendDisconnectedException("The database disconnected: " + message));
    }

    private boolean validateState(String notPossible, ClientSessionState... expected) {
        if (Arrays.asList(expected).contains(_state)) {
            return true;
        }

        notPossible = (notPossible == null || notPossible.isEmpty()) ? " execute this action" : notPossible;
        String msg = "The connection is in state '" + _state + "'. Cannot " + notPossible + ".";
        handleError(new InvalidStateException(msg, ErrorAction.Abort));
        return false;
    }
}
