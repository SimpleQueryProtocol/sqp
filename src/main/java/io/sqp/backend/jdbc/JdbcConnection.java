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

package io.sqp.backend.jdbc;

import io.sqp.backend.*;
import io.sqp.backend.exceptions.PrepareFailedException;
import io.sqp.backend.results.*;
import io.sqp.core.*;
import io.sqp.backend.exceptions.DatabaseConnectionException;
import io.sqp.backend.exceptions.FeatureNotSupportedException;
import io.sqp.backend.exceptions.TransactionFinishFailedException;
import io.sqp.core.exceptions.BackendErrorException;
import io.sqp.core.exceptions.CursorProblemException;
import io.sqp.core.exceptions.SqpException;
import io.sqp.core.types.SqpValue;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Stefan Burnicki
 */
public class JdbcConnection implements BackendConnection {
    private String _jdbcUrl;
    private String _username;
    private String _password;

    private Logger _logger;

    private java.sql.Connection _jdbcConnection;

    private Map<String, SmartResultSet> _openResultSets;
    private Map<String, SmartStatement> _openStatements;

    public JdbcConnection(Logger logger, String jdbcUrl, String username, String password) {
        _openResultSets = new HashMap<>();
        _openStatements = new HashMap<>();
        _jdbcUrl = jdbcUrl;
        _username = username;
        _password = password;
        _logger = logger;
    }

    @Override
    public void commit(SuccessHandler successHandler) {
        try {
            _jdbcConnection.commit();
            successHandler.succeed();
        } catch (SQLException e) {
            successHandler.fail(new TransactionFinishFailedException(true, e));
        }
    }

    @Override
    public void rollback(SuccessHandler successHandler) {
        try {
            _jdbcConnection.rollback();
            successHandler.succeed();
        } catch (SQLException e) {
            successHandler.fail(new TransactionFinishFailedException(false, e));
        }
    }

    @Override
    public void getInformation(InformationSubject subject, String detail, ResultHandler<InformationRequestResult> resultHandler) {
        try {
            switch (subject) {
                case DBMSName:
                    String dbmsName = _jdbcConnection.getMetaData().getDatabaseProductName();
                    resultHandler.handle(new InformationRequestResult(InformationResponseType.Text, dbmsName));
                    return;
                default:
            }
        } catch (SQLException e) {
            resultHandler.fail(new SqpException(ErrorType.InformationRequestFailed, "Couldn't get information on " +
                    subject + ".", ErrorAction.Recover));
            return;
        }
        resultHandler.handle(InformationRequestResult.UNKNOWN);
    }

    @Override
    public void setFeatures(List<FeatureSetting<?>> featureSettings, SuccessHandler successHandler) {
        FeatureSetting<?> firstSetting = featureSettings.get(0);
        // This backend only supports AutoCommit
        if (firstSetting.getFeature() != FeatureSetting.Feature.AutoCommit) {
            successHandler.fail(new FeatureNotSupportedException(firstSetting.getFeature()));
            return;
        }
        boolean enable;
        try {
            enable = firstSetting.getValue(Boolean.class);
        } catch (BackendErrorException e) {
            successHandler.fail(e);
            return;
        }
        try {
            _jdbcConnection.setAutoCommit(enable);
            successHandler.succeed();
        } catch (SQLException e) {
            // TODO: use separate Exception type?
            String enableStr = enable ? "enable" : "disable";
            successHandler.fail(new SqpException(ErrorType.SetFeatureFailed,
                    "Failed to " + enableStr + "AutoCommit: " + e.getCause(), ErrorAction.Recover, e));
        }
    }

    @Override
    public void connect(String databaseName, Consumer<String> disconnectHandler, SuccessHandler successHandler) {
        // TODO: do this asynchronously with an AsyncExecutor
        // note we currently ignore the disconnect handler as JDBC cannot asynchronously report disconnects
        String url = _jdbcUrl + databaseName;
        try {
            _jdbcConnection = DriverManager.getConnection(url, _username, _password);
            _jdbcConnection.setAutoCommit(true); // our default
        } catch (SQLException e) {
            String errorMsg = "Could not connect to the database: " + e.getMessage();
            successHandler.fail(new DatabaseConnectionException(errorMsg));
            return;
        }
        // otherwise success!
        successHandler.succeed();
    }

    @Override
    public void simpleQuery(String sql, String cursorId, boolean scrollable, int maxFetch, ResultHandler<QueryResult> resultHandler) {
        try {
            Statement stmt = _jdbcConnection.createStatement();
            if (!stmt.execute(sql)) {
                resultHandler.handle(new UpdateQueryResult(stmt.getUpdateCount()));
                return;
            }
            // otherwise it's a cursor
            SmartResultSet smartResultSet = new SmartResultSet(stmt.getResultSet(), scrollable, null);
            processResultSet(smartResultSet, cursorId, scrollable, resultHandler);
            fetchData(smartResultSet, maxFetch, true, resultHandler);
        } catch (SQLException e) {
            String errorMsg = "Execution of the query failed: " + e.getMessage();
            resultHandler.fail(new SqpException(ErrorType.ExecutionFailed, errorMsg, ErrorAction.Recover));
        } catch (SqpException e) {
            resultHandler.fail(e);
        }
    }

    @Override
    public void prepare(String sql, String stmtId, SuccessHandler successHandler) {
        // TODO: do this asynchronously with an AsyncExecutor
        try {
            SmartStatement statement = _openStatements.get(stmtId);
            if (statement != null) {
                statement.close();
            }
            statement = new SmartStatement(sql, _jdbcConnection);
            _openStatements.put(stmtId, statement);
            successHandler.succeed();
        } catch (SQLException e) {
            successHandler.fail(new PrepareFailedException(e.getMessage(), e));
        }
    }

    @Override
    public void execute(String stmtId, String cursorId, List<List<SqpValue>> parameters, boolean scrollable,
                        ResultHandler<QueryResult> resultHandler) {
        SmartStatement currentStatement = _openStatements.get(stmtId);
        if (currentStatement == null) {
            resultHandler.fail(new SqpException(ErrorType.StatementNotFound,
                    "Statement with id '" + stmtId + "' was not found", ErrorAction.Recover));
            return;
        }
        try {
            currentStatement.bindParameterBatch(parameters);
            if (currentStatement.isUpdate()) {
                resultHandler.handle(new UpdateQueryResult(currentStatement.executeUpdate()));
            } else {
                SmartResultSet resultSet = currentStatement.executeQuery(scrollable);
                processResultSet(resultSet, cursorId, scrollable, resultHandler);
            }
        } catch (SQLException e) {
            // TODO: differentiate why the execution failed (if possible)
            String errorMsg = "Execution of the query failed: " + e.getMessage();
            resultHandler.fail(new SqpException(ErrorType.ExecutionFailed, errorMsg, ErrorAction.Recover));
        } catch (SqpException e) {
            resultHandler.fail(e);
        }
    }

    @Override
    public void release(Collection<String> statementIds, Collection<String> cursorIds, SuccessHandler successHandler) {
        StringBuilder failedCursors = null;
        StringBuilder failedStatements = null;
        for (String cursorId : cursorIds) {
            try {
                SmartResultSet rs = _openResultSets.remove(cursorId);
                if (rs == null) {
                    _logger.log(Level.INFO, "Attempt to close cursor '" + cursorId + "', but it doesn't exist");
                    // TODO: if we have support for warnings, we should somehow send one from here
                    continue;
                }
                rs.close();
            } catch (SQLException e) {
                _logger.log(Level.WARNING, "Failed to close cursor '" + cursorId + "'", e);
                if (failedCursors == null) {
                    failedCursors = new StringBuilder();
                } else {
                    failedCursors.append(", ");
                }
                failedCursors.append(cursorId);
            }
        }
        for (String statementId : statementIds) {
            try {
                SmartStatement st = _openStatements.remove(statementId);
                if (st == null) {
                    _logger.log(Level.INFO, "Attempt to close statement '" + statementId + "', but it doesn't exist.");
                    continue;
                }
                st.close();
            } catch (SQLException e) {
                _logger.log(Level.WARNING, "Failed to close statement '" + statementId + "'", e);
                if (failedStatements == null) {
                    failedStatements = new StringBuilder();
                } else {
                    failedStatements.append(", ");
                }
                failedStatements.append(statementId);
            }
        }
        if (failedCursors != null || failedStatements != null) {
            // TODO: really throw an error? Maybe a warning would be enough. Otherwise we just recover, meaning
            // we simply go to ReadyState...
            String cursors = failedCursors == null ? "" : failedCursors.toString();
            String stmts = failedStatements == null ? "" : failedStatements.toString();
            successHandler.fail(new SqpException(ErrorType.CloseFailed, "Failed to close statements: '" + stmts +
                    "' and/or cursors: '" + cursors + "'.", ErrorAction.Recover));
            return;
        }
        successHandler.succeed();
    }

    @Override
    public void fetch(String cursorId, int position, int numRows, boolean forward, ResultHandler<QueryResult> resultHandler) {
        // TODO: regard parameter <position>
        SmartResultSet resultSet = _openResultSets.get(cursorId);
        if (resultSet == null) {
            resultHandler.fail(new CursorProblemException(cursorId, CursorProblemException.Problem.DoesNotExist));
            return;
        } else if (!forward && !resultSet.isScrollable()) {
            resultHandler.fail(new CursorProblemException(cursorId, CursorProblemException.Problem.NotScrollable));
            return;
        }

        fetchData(resultSet, numRows, forward, resultHandler);
    }

    private void fetchData(SmartResultSet resultSet, int numRows, boolean forward, ResultHandler<QueryResult> resultHandler) {
        int numSent = 0;
        // now actual results
        List<ColumnMetadata> columnMetadata = resultSet.getColumnMetadata();
        int numCols = columnMetadata.size();
        boolean dataLeft = false;
        ResultSet rawResults = resultSet.getRawResultSet();
        try {
            while (true) {
                // first check for an artificial maximum
                if (numRows >= 0 && numSent >= numRows) {
                    // NOTE: if numSent == numRows, we cannot guarantee that there is more
                    // data. But calling rawResults.isLast() won't work without scrollable cursors
                    dataLeft = true;
                    break;
                }
                // otherwise check if there is actual data left
                if (forward) {
                    dataLeft = rawResults.next();
                } else {
                    dataLeft = rawResults.previous();
                }
                if (!dataLeft) {
                    break;
                }
                ArrayList<SqpValue> rowdata = new ArrayList<>(numCols);
                for (int i = 1; i <= numCols; i++) { // JDBC columns have a 1-based index
                    rowdata.add(ResultExtractor.extractResult(columnMetadata.get(i - 1).getType(), rawResults, i));
                }
                resultHandler.handle(new RowDataResult(rowdata));
                numSent++;
            }
        } catch (SQLException e) {
            String errorMsg = "Fetching data from cursor failed: " + e.getMessage();
            resultHandler.fail(new SqpException(ErrorType.FetchFailed, errorMsg, ErrorAction.Recover));
            return;
        } catch (SqpException e) {
            resultHandler.fail(e);
            return;
        }
        resultHandler.handle(new EndQueryResult(dataLeft));
    }


    private void processResultSet(SmartResultSet resultSet, String cursorId, boolean scrollable, ResultHandler<QueryResult> resultHandler)
            throws SQLException, SqpException {
        // TODO: handle type conversion exceptions more carefully
        SmartResultSet existing = _openResultSets.remove(cursorId);
        if (existing != null) {
            _logger.log(Level.INFO, "Auto-closing cursor '" + cursorId + "' because of a new execution with same ID");
            existing.close();
        }
        _openResultSets.put(cursorId, resultSet);
        CursorDescriptionResult cursorResult = new CursorDescriptionResult(cursorId, scrollable,
                resultSet.getColumnMetadata());
        resultHandler.handle(cursorResult);
    }


    @Override
    public void getLob(String id, long offset, long length, ResultHandler<LobStream> resultHandler) {
        throw new UnsupportedOperationException("Lob support is not yet implemented");
    }

    @Override
    public void close() {
        try {
            for (Map.Entry<String, SmartResultSet> entry : _openResultSets.entrySet()) {
                _logger.log(Level.INFO, "Closing open result set '" + entry.getKey() + "'");
                entry.getValue().close();
            }
            for (Map.Entry<String, SmartStatement> entry : _openStatements.entrySet()) {
                _logger.log(Level.INFO, "Closing open statement '" + entry.getKey() + "'");
                entry.getValue().close();
            }
            if (_jdbcConnection != null) {
                _jdbcConnection.close();
            }
        } catch (SQLException e) {
            _logger.log(Level.WARNING, "Closing the JDBC connection failed.", e);
        }
        _openResultSets.clear();
        _openStatements.clear();
        _jdbcConnection = null;
    }
}
