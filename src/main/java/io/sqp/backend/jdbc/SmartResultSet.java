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

import io.sqp.core.ColumnMetadata;
import io.sqp.core.TypeDescription;
import io.sqp.core.exceptions.NotImplementedException;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Stefan Burnicki
 */
public class SmartResultSet {
    private SmartStatement _origin;
    private ResultSet _resultSet;
    private ArrayList<ColumnMetadata> _columnMetadata;
    private boolean _scrollable;

    public SmartResultSet(ResultSet resultSet, boolean scrollable, SmartStatement origin) throws SQLException, NotImplementedException {
        _origin = origin;
        _resultSet = resultSet;
        _scrollable = scrollable;
        initializeColumnMetadata();
    }

    public void close() throws SQLException {
        try {
            _resultSet.close();
        } finally {
            if (_origin != null) {
                _origin.releaseResult();
            }
        }
    }

    public List<ColumnMetadata> getColumnMetadata() {
        return _columnMetadata;
    }

    public ResultSet getRawResultSet() {
        return _resultSet;
    }

    public boolean isScrollable() {
        return _scrollable;
    }

    private void initializeColumnMetadata() throws SQLException, NotImplementedException {
        ResultSetMetaData metadata = _resultSet.getMetaData();
        int numCols = metadata.getColumnCount();

        // First get and send column metadata
        // TODO: check precision/scale for numeric/decimal, varchar, varbinary, ...
        _columnMetadata = new ArrayList<>(numCols);
        for (int i = 1; i <= numCols; i++) { // JDBC columns have a 1-based index
            TypeDescription curType = ResultExtractor.getStandardDataType(metadata, i); // use 0-based index
            _columnMetadata.add(new ColumnMetadata(metadata.getColumnName(i), curType, metadata.getColumnTypeName(i)));
        }
    }
}
