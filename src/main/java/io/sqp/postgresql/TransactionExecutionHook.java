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

import org.postgresql.core.Field;
import org.postgresql.core.Query;
import org.postgresql.core.ResultCursor;

import java.util.List;

/**
 * @author Stefan Burnicki
 */
public class TransactionExecutionHook extends ExecutionHook {
    @Override
    public void handleResultRows(Query fromQuery, Field[] fields, List tuples, ResultCursor cursor) {
        // we don't care since we don't expect anything
    }

    @Override
    public void handleCommandStatus(String status, int updateCount, long insertOID) {
        // we don't care since we don't expect anything
    }
}
