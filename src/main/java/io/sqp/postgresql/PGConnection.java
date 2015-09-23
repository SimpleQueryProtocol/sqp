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

package io.sqp.postgresql;

import io.sqp.backend.*;
import io.sqp.backend.ResultHandler;
import org.postgresql.core.*;
import org.postgresql.util.HostSpec;
import io.sqp.backend.exceptions.DatabaseConnectionException;
import io.sqp.backend.exceptions.TransactionFinishFailedException;
import io.sqp.backend.results.InformationRequestResult;
import io.sqp.backend.results.QueryResult;
import io.sqp.core.ErrorAction;
import io.sqp.core.ErrorType;
import io.sqp.core.InformationResponseType;
import io.sqp.core.InformationSubject;
import io.sqp.core.exceptions.BackendErrorException;
import io.sqp.core.exceptions.CursorProblemException;
import io.sqp.core.exceptions.NotImplementedException;
import io.sqp.core.exceptions.SqpException;
import io.sqp.core.types.SqpValue;

import java.sql.SQLException;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author Stefan Burnicki
 */
public class PGConnection implements BackendConnection {
    private static int _connectionIdCounter;

    private ProtocolConnection _connection;
    private int _connectionId;
    private HashMap<String, Statement> _openStatements;
    private HashMap<String, Cursor> _openCursors;
    private Query _commitQuery;
    private Query _rollbackQuery;
    private boolean _autocommit;
    private TypeConverter _typeConverter;
    private AsyncExecutor _asyncExecutor;
    private PGConfiguration _configuration;

    public PGConnection(PGConfiguration config, AsyncExecutor asyncExecutor) {
        _autocommit = true;
        _openStatements = new HashMap<>();
        _openCursors = new HashMap<>();
        _connectionId = _connectionIdCounter++;
        _asyncExecutor = asyncExecutor;
        _configuration = config;
    }

    @Override
    public void connect(String databaseName, Consumer<String> disconnectHandler, SuccessHandler connectionHandler) {
        Properties props = new Properties();
        props.setProperty("password", _configuration.getPassword());
        _asyncExecutor.callAsync(() -> {
            try {
                // TODO: this might be dangerous because of concurrency
                _connection = ConnectionFactory.openConnection(new HostSpec[]{_configuration.getHostSpec()},
                        _configuration.getUsername(), databaseName, props, new Logger(_connectionId));
                _typeConverter = new TypeConverter(this, _configuration.getServerLocale());
                _commitQuery = _connection.getQueryExecutor().createSimpleQuery("COMMIT");
                _rollbackQuery = _connection.getQueryExecutor().createSimpleQuery("ROLLBACK");
            } catch (Exception e) {
                throw new DatabaseConnectionException("Failed to connect to PostgreSQL database: " + e.getMessage(), e.getCause());
            }
            return null;
        }, connectionHandler);
    }

    @Override
    public void close() {
        if (_connection == null || _connection.isClosed()) {
            // can't communicate with server anymore
            _openStatements.clear();
            _openCursors.clear();
            return;
        }
        _asyncExecutor.callAsync(() -> {
            // release statements and cursors
            release(_openStatements.keySet(), _openCursors.keySet(), new SuccessHandler(e -> {
            },
                    // close connection afterwards
                    _connection::close
            ));
            return null;
        }, new ResultHandler<>(e -> {
        }, v -> {
        }));
    }

    @Override
    public void setFeatures(List<FeatureSetting<?>> featureSettings, SuccessHandler successHandler) {
        featureSettings = new ArrayList<>(featureSettings); // copy it to make sure it's modifiable
        try {
            featureSettings = setNonBlockingFeatures(featureSettings);
            if (featureSettings.size() < 1) {
                successHandler.succeed();
                return;
            }
            setBlockingFeatures(featureSettings, successHandler);
        } catch (BackendErrorException e) {
            successHandler.fail(e);
        }
    }

    public List<FeatureSetting<?>> setNonBlockingFeatures(List<FeatureSetting<?>> featureSettings) throws BackendErrorException {
        for (Iterator<FeatureSetting<?>> iterator = featureSettings.iterator(); iterator.hasNext(); ) {
            FeatureSetting<?> setting = iterator.next();
            switch (setting.getFeature()) {
                case AllowNativeTypes:
                    _typeConverter.addAllowedNativeTypes(Arrays.asList(setting.getValue(String[].class)));
                    iterator.remove();
                    break;
            }
        }
        return featureSettings;
    }

    public void setBlockingFeatures(List<FeatureSetting<?>> featureSettings, SuccessHandler successHandler) {
        // TODO: when we support more than one blocking feature, we need to change some stuff in order to execute
        // all changes in one async operation. Then this code should get nicer (check Transbase backend)

        FeatureSetting<?> firstSetting = featureSettings.get(0);
        if (featureSettings.size() > 1 || firstSetting.getFeature() != FeatureSetting.Feature.AutoCommit) {
            String unsupported = featureSettings.stream()
                    .filter(setting -> setting.getFeature() != FeatureSetting.Feature.AutoCommit)
                    .map(setting -> setting.getFeature().toString()).collect(Collectors.joining(", "));
            successHandler.fail(new NotImplementedException("The feature(s) " + unsupported + " are not implemented."));
        }
        // when we are here, we know it's autocommit to be set

        boolean wasEnabled = _autocommit;
        try {
            _autocommit = firstSetting.getValue(Boolean.class);
        } catch (BackendErrorException e) {
            successHandler.fail(e);
            return;
        }
        if (!wasEnabled && _autocommit && hasActiveTransaction()) {
            commit(successHandler);
        } else {
            successHandler.succeed();
        }
    }

    @Override
    public void commit(SuccessHandler successHandler) {
        executeTransactionCommand(_commitQuery, successHandler);
    }

    @Override
    public void rollback(SuccessHandler successHandler) {
        executeTransactionCommand(_rollbackQuery, successHandler);
    }

    @Override
    public void getInformation(InformationSubject subject, String detail, ResultHandler<InformationRequestResult> resultHandler) {
        switch (subject) {
            case DBMSName:
                resultHandler.handle(new InformationRequestResult(InformationResponseType.Text, "PostgreSQL"));
                return;
        }
        resultHandler.handle(InformationRequestResult.UNKNOWN);
    }

    @Override
    public synchronized void prepare(String sql, String statementId, SuccessHandler successHandler) {
        Statement open = _openStatements.remove(statementId);
        if (open != null) {
            open.close();
        }
        _openStatements.put(statementId, Statement.create(this, sql, successHandler));
    }

    @Override
    public void execute(String statementId, String cursorId, List<List<SqpValue>> parameters, boolean scrollable,
                        ResultHandler<QueryResult> resultHandler) {
        Statement affectedStmt = _openStatements.get(statementId);
        if (affectedStmt == null) {
            resultHandler.fail(new SqpException(ErrorType.StatementNotFound,
                    "Statement with id '" + statementId + "' was not found", ErrorAction.Recover));
            return;
        }

        try {
            affectedStmt.bind(parameters);
        } catch (SqpException e) {
            resultHandler.fail(e);
            return;
        }

        affectedStmt.execute(_autocommit, cursorId, scrollable, resultHandler);
    }

    @Override
    public void fetch(String cursorId, int position, int numRows, boolean forward, ResultHandler<QueryResult> resultHandler) {
        Cursor openCursor = _openCursors.get(cursorId);
        if (openCursor == null) {
            resultHandler.fail(new CursorProblemException(cursorId, CursorProblemException.Problem.DoesNotExist));
            return;
        }
        openCursor.fetch(position, numRows, forward, resultHandler);
    }

    @Override
    public void simpleQuery(String sql, String cursorId, boolean scrollable, int maxFetch, ResultHandler<QueryResult> resultHandler) {
        SimpleStatement stmt = new SimpleStatement(this, sql);
        stmt.execute(_autocommit, cursorId, scrollable, maxFetch, resultHandler);
    }

    @Override
    public void release(Collection<String> statementIds, Collection<String> cursorIds, SuccessHandler successHandler) {
        cursorIds.stream().map(_openCursors::remove).filter(cursor -> cursor != null).forEach(Cursor::close);
        statementIds.stream().map(_openStatements::remove).filter(stmt -> stmt != null).forEach(Statement::close);
        successHandler.succeed();
    }

    @Override
    public void getLob(String id, long offset, long length, ResultHandler<LobStream> resultHandler) {
        // TODO: support CLOBs and BLOBs. See the ParameterBinder on ideas of how to do this.
        // Also check the Transbase backend for a working CLOB/BLOB implementation
        throw new UnsupportedOperationException("Lob support is not yet implemented");
    }

    QueryExecutor getQueryExecutor() {
        return _connection.getQueryExecutor();
    }

    TypeConverter getTypeConverter() {
        return _typeConverter;
    }

    ProtocolConnection getProtocolConnection() {
        return _connection;
    }

    AsyncExecutor getAsyncExecutor() {
        return _asyncExecutor;
    }

    boolean haveMinimumServerVersion(String ver) {
        // TODO: maybe buffer this
        int requiredver = Utils.parseServerVersionStr(ver);
        if (requiredver == 0) {
            // Failed to parse input version. Fall back on legacy behaviour for BC.
            return (_connection.getServerVersion().compareTo(ver) >= 0);
        } else {
            return (_connection.getServerVersionNum() >= requiredver);
        }
    }

    private void executeTransactionCommand(Query query, SuccessHandler successHandler) {
        if (!hasActiveTransaction()) {
            // no transaction to roll back or commit. Also when using autocommit
            successHandler.fail(new TransactionFinishFailedException("No active transaction to commit/rollback."));
            return;
        }

        int flags = QueryExecutor.QUERY_NO_METADATA | QueryExecutor.QUERY_NO_RESULTS | QueryExecutor.QUERY_SUPPRESS_BEGIN;
        _asyncExecutor.callAsync(() -> {
            try {
                getQueryExecutor().execute(query, null, new TransactionExecutionHook(), 0, 0, flags);
                return null;
            } catch (SQLException e) {
                throw new TransactionFinishFailedException(query == _commitQuery, e);
            }
        }, new ResultHandler<>(successHandler::fail, v -> successHandler.succeed()));
    }

    private boolean hasActiveTransaction() {
        return _connection.getTransactionState() != ProtocolConnection.TRANSACTION_IDLE;
    }

    public synchronized void registerCursor(String cursorId, Cursor newCursor) {
        Cursor openCursor = _openCursors.remove(cursorId);
        if (openCursor != null) {
            openCursor.close();
        }
        _openCursors.put(cursorId, newCursor);
    }
}
