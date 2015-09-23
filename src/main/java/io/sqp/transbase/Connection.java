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

package io.sqp.transbase;

import io.sqp.backend.*;
import io.sqp.backend.results.*;
import io.sqp.core.*;
import io.sqp.backend.exceptions.DatabaseConnectionException;
import io.sqp.backend.exceptions.LobReadFailedException;
import io.sqp.core.exceptions.BackendErrorException;
import io.sqp.core.exceptions.CursorProblemException;
import io.sqp.core.exceptions.NotImplementedException;
import io.sqp.core.exceptions.SqpException;
import io.sqp.core.types.SqpValue;
import transbase.tbx.TBConst;
import transbase.tbx.TBURL;
import transbase.tbx.TBXConnectionIf;
import transbase.tbx.java.TBXJavaConnection;
import transbase.tbx.types.helpers.BlobStream;
import transbase.tbx.types.helpers.CharEncodingFactory;

import java.io.ByteArrayInputStream;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * @author Stefan Burnicki
 */
public class Connection implements BackendConnection {
    private Logger _logger;
    private AsyncExecutor _asyncExecutor;
    private Transaction _transaction;
    private TBXConnectionIf _connection;
    private TBConfiguration _configuration;
    private Map<String, Statement> _openStatements;
    private Map<String, Cursor> _openCursors;
    private TBNativeSQLFactory _nativeSQLFactory;
    private CharEncodingFactory _charEncodingFactory;
    private ResultConverter _resultConverter;
    private LobManager _lobManager;

    public Connection(Logger logger, TBConfiguration config, TBNativeSQLFactory nativeSQLFactory, AsyncExecutor asyncExecutor)
    {
        _logger = logger;
        _configuration = config;
        _asyncExecutor = asyncExecutor;
        _openStatements = new HashMap<>();
        _openCursors = new HashMap<>();
        _nativeSQLFactory = nativeSQLFactory;
        _lobManager = new LobManager();
        _resultConverter = new ResultConverter(_lobManager);
    }

    @Override
    public void connect(String databaseName, Consumer<String> disconnectHandler, SuccessHandler connectionHandler) {
        TBURL connectionUrl = _configuration.getTBUrlForDatabase(databaseName);
        _asyncExecutor.runAsync(() -> {
            // blocking operation, worker thread
            try {
                _connection = new TBXJavaConnection(connectionUrl);
                _connection.setEncoder("UTF-8");
                _charEncodingFactory = _connection.getCharEncodingFactory();
                _connection.login(_configuration.getUsername(), _configuration.getPassword());
                _transaction = new Transaction(this);
            } catch (SQLException e) {
                throw new DatabaseConnectionException("Failed to connect or login:" + e.getMessage(), e);
            }
        }, connectionHandler);
    }

    @Override
    public void close() {
        if (_connection == null) {
            return;
        }
        // close whatever happens
        if (_openStatements.size() > 0 || _openCursors.size() > 0) {
            _logger.log(Level.INFO, "Auto-closing open statements " + String.join(", ", _openStatements.keySet()) +
                    " and cursors " + String.join(", ", _openCursors.keySet()));
            release(_openStatements.keySet(), _openCursors.keySet(), new SuccessHandler(error -> {
                _logger.log(Level.INFO, "Failed to close open statements/cursors", error);
                doCloseConnection();
            }, this::doCloseConnection));
        } else {
            doCloseConnection();
        }
    }

    private void doCloseConnection() {
        _asyncExecutor.runAsync(() -> {
            try {
                if (_transaction.getAutoCommit()) {
                    _transaction.commit();
                } else {
                    _transaction.rollback();
                }
            } finally {
                try {
                    _connection.close();
                } catch (SQLException e) {
                    throw new SqpException(ErrorType.CloseFailed, "Could not close TBX connection.", ErrorAction.Abort, e);
                }
            }
        }, new SuccessHandler(e -> _logger.log(Level.WARNING, "Closing the TB connection failed.", e), () -> {}));
    }

    @Override
    public void setFeatures(List<FeatureSetting<?>> featureSettings, SuccessHandler successHandler) {
        featureSettings = new ArrayList<>(featureSettings); // copy it to make sure it's modifiable
        try {
            featureSettings = setNonBlockingFeatures(featureSettings);
        } catch (BackendErrorException e) {
            successHandler.fail(e);
            return;
        }
        if (featureSettings.size() < 1) {
            successHandler.succeed();
            return;
        }
        final List<FeatureSetting<?>> blockingFeatureSettings = featureSettings;
        _asyncExecutor.runAsync(() -> setBlockingFeatures(blockingFeatureSettings), successHandler);
    }

    private void setBlockingFeatures(List<FeatureSetting<?>> featureSettings) throws SqpException {
        for (Iterator<FeatureSetting<?>> iterator = featureSettings.iterator(); iterator.hasNext(); ) {
            FeatureSetting<?> setting = iterator.next();
            switch (setting.getFeature()) {
                case AutoCommit:
                    _transaction.setAutoCommit(setting.getValue(Boolean.class));
                    iterator.remove();
                    break;
            }
        }
        if (featureSettings.size() < 1) {
            return;
        }
        String missing = featureSettings.stream().map(setting -> setting.getFeature().toString()).collect(Collectors.joining(", "));
        throw new NotImplementedException("The feature(s) " + missing + " are not implemented.");
    }

    private List<FeatureSetting<?>> setNonBlockingFeatures(List<FeatureSetting<?>> featureSettings) throws BackendErrorException {
        for (Iterator<FeatureSetting<?>> iterator = featureSettings.iterator(); iterator.hasNext(); ) {
            FeatureSetting<?> setting = iterator.next();
            switch (setting.getFeature()) {
                case AllowNativeTypes:
                    _resultConverter.addAllowedNativeTypes(Arrays.asList(setting.getValue(String[].class)));
                    iterator.remove();
                    break;
            }
        }
        return featureSettings;
    }

    @Override
    public void commit(SuccessHandler successHandler) {
        _asyncExecutor.runAsync(_transaction::commit, successHandler);
    }

    @Override
    public void rollback(SuccessHandler successHandler) {
        _asyncExecutor.runAsync(_transaction::rollback, successHandler);
    }

    @Override
    public void getInformation(InformationSubject subject, String detail, ResultHandler<InformationRequestResult> resultHandler) {
        switch (subject) {
            case DBMSName:
                resultHandler.handle(new InformationRequestResult(InformationResponseType.Text, "Transbase"));
                return;
        }
        resultHandler.handle(InformationRequestResult.UNKNOWN);
    }

    @Override
    public void prepare(String sql, String statementId, SuccessHandler successHandler) {
        Statement open = _openStatements.remove(statementId);
        _asyncExecutor.callAsync(() -> {
           if (open != null) {
               open.close();
           }
           return new Statement(this, _nativeSQLFactory, sql);
        }, new ResultHandler<>(successHandler::fail, statement -> {
            _openStatements.put(statementId, statement);
            successHandler.succeed();
        }));
    }

    @Override
    public void execute(String statementId, String cursorId, List<List<SqpValue>> parameters, boolean scrollable, ResultHandler<QueryResult> resultHandler) {
        // TODO: implement parameter support
        Statement statement = _openStatements.get(statementId);
        if (statement == null) {
            resultHandler.fail(new SqpException(ErrorType.StatementNotFound,
                    "Statement with id '" + statementId + "' was not found", ErrorAction.Recover));
            return;
        }
        Cursor open = _openCursors.remove(cursorId);
        _asyncExecutor.callAsync(() -> {
            if (open != null) {
                open.close();
            }
            statement.bind(parameters);
            if (statement.isSelectQuery()) {
                Cursor cursor = statement.executeSelect(_transaction, scrollable);
                _openCursors.put(cursorId, cursor);
                return new CursorDescriptionResult(cursorId, scrollable, cursor.getColumnMetadata());
            } else {
                return new UpdateQueryResult(statement.executeUpdate(_transaction));
            }
        }, resultHandler);
    }

    @Override
    public void fetch(String cursorId, int position, int numRows, boolean forward, ResultHandler<QueryResult> resultHandler) {
        Cursor cursor = _openCursors.get(cursorId);
        if (cursor == null) {
            resultHandler.fail(new CursorProblemException(cursorId, CursorProblemException.Problem.DoesNotExist));
            return;
        }
        if ((position >= 0 || !forward) && !cursor.isScrollable()) {
            resultHandler.fail(new CursorProblemException(cursorId, CursorProblemException.Problem.NotScrollable));
            return;
        }
        _asyncExecutor.callAsync(() -> {
            List<List<SqpValue>> rows = cursor.fetch(position, numRows, forward);
            List<QueryResult> results = rows.stream().map(RowDataResult::new).collect(Collectors.toList());
            results.add(new EndQueryResult(cursor.hasMore()));
            return results;
        }, new ResultHandler<>(resultHandler::fail, list -> list.forEach(resultHandler::handle)));
    }

    @Override
    public void simpleQuery(String sql, String cursorId, boolean scrollable, int maxFetch, ResultHandler<QueryResult> resultHandler) {
        Cursor open = _openCursors.remove(cursorId);
        _asyncExecutor.callAsync(() -> {
            if (open != null) {
                open.close();
            }
            SimpleStatement stmt = new SimpleStatement(this, _nativeSQLFactory, sql);
            if (!stmt.execute(_transaction, scrollable)) {
                return Collections.singletonList(new UpdateQueryResult(stmt.getAffectedRows()));
            }
            // otherwise it's a cursor
            Cursor cursor = stmt.getAssociatedCursor();
            _openCursors.put(cursorId, cursor);
            List<List<SqpValue>> rows = cursor.fetch(-1, maxFetch, true);
            List<QueryResult> results = rows.stream().map(RowDataResult::new).collect(Collectors.toList());
            results.add(0, new CursorDescriptionResult(cursorId, scrollable, cursor.getColumnMetadata()));
            results.add(new EndQueryResult(cursor.hasMore()));
            return results;
        }, new ResultHandler<>(resultHandler::fail, list -> list.forEach(resultHandler::handle)));
    }

    @Override
    public void release(Collection<String> statementIds, Collection<String> cursorIds, SuccessHandler successHandler) {
        // the following code takes some brute-force method: It captures exceptions and fails at the end if
        // anything failed. But it will try to close all, because that's what the user wants
        if (statementIds.size() < 1 && cursorIds.size() < 1) {
            successHandler.succeed();
            return;
        }
        _asyncExecutor.runAsync(() -> {
            List<String> failedCursors = new ArrayList<>();
            List<String> failedStatements= new ArrayList<>();
            boolean closedCursors = false;
            for (String cId : cursorIds) {
                Cursor cursor = _openCursors.remove(cId);
                if (cursor == null) {
                    continue;
                }
                try {
                    cursor.close();
                    closedCursors = true;
                } catch (SqpException e) {
                    failedCursors.add(cId);
                }
            }
            if (closedCursors) {
                _transaction.tryAutoCommit(Transaction.CommitType.CLOSE_CURSOR);
            }
            for (String sId : statementIds) {
                Statement stmt = _openStatements.remove(sId);
                if (stmt == null) {
                    continue;
                }
                try {
                    stmt.close();
                } catch (SqpException e) {
                    failedStatements.add(sId);
                }
            }
            if (failedStatements.size() > 0 || failedCursors.size() > 0) {
                successHandler.fail(new SqpException(ErrorType.CloseFailed, "Failed to close statements: '" +
                        String.join(", ", failedStatements) + "' and/or cursors: '" + String.join(", ", failedCursors) + "'.", ErrorAction.Recover));
            }
        }, successHandler);
    }

    @Override
    public void getLob(String id, long offset, long length, ResultHandler<LobStream> resultHandler) {
        if (length > Integer.MAX_VALUE) {
            resultHandler.fail(new LobReadFailedException("Requested chunk is too big"));
            return;
        }
        BlobStream blobStream = _lobManager.getBlobStream(id);
        if (blobStream == null) {
            resultHandler.fail(new LobReadFailedException("LOB with ID '" + id + "' does not exist."));
            return;
        }
        DataFormat format = blobStream.getBType() == TBConst.CLOBTYPE ? DataFormat.Text : DataFormat.Binary;
        // TODO: pass directly the BLOB stream that is restricted to the area from offset to offset+length
        _asyncExecutor.callAsync(() -> {
            long normalizedOffset = offset < 0 ? 0 : offset;
            long dataLeft = 0;
            try {
                dataLeft = blobStream.getLength() - normalizedOffset;
            } catch (SQLException e) {
                throw new LobReadFailedException("Could not get LOB size: " + e.getMessage(), e);
            }
            if (dataLeft <= 0) {
                return new LobStream(new ByteArrayInputStream(new byte[0]),format);
            }
            long fetchLength = length < 0 ? dataLeft : Math.min(length, dataLeft);
            try {
                // BlobStream takes a 1-based index
                byte[] buffer = blobStream.getByteArray(normalizedOffset + 1, (int) fetchLength);
                return new LobStream(new ByteArrayInputStream(buffer), format);
            } catch (Exception e) {
                throw new LobReadFailedException("Failed to read from internal stream: " + e.getMessage(), e);
            }
        }, resultHandler);
    }

    public ResultConverter getResultConverter() {
        return _resultConverter;
    }

    public int getNumOpenCursors() {
        return _openCursors.size();
    }

    public TBXConnectionIf getTBXConnection() {
        return _connection;
    }

    public CharEncodingFactory getCharEncodingFactory() {
        return _charEncodingFactory;
    }
}
