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

package io.sqp.client.testhelpers;

import io.sqp.client.SqpConnection;
import io.sqp.client.QueryResult;

import java.time.OffsetDateTime;
import java.util.concurrent.CompletableFuture;

/**
 * @author Stefan Burnicki
 */
public class IncomeTable {

    public static CompletableFuture<QueryResult> clear(SqpConnection connection) {
        return connection.execute("DELETE FROM income");
    }

    public static CompletableFuture<QueryResult> insert(SqpConnection connection, OffsetDateTime timestamp, Double money,
                                                        double[] pos, Short attempts) {
        // we don't use a prepared statement since we might want to test them. simple statements are less erroneous
        String timestampLiteral = timestamp == null ? "NULL" : "'" + timestamp.toString().replace('T', ' ') + "'";
        String posLiteral = pos == null ? "NULL" : "'(" + pos[0] + "," + pos[1] + ")'";
        String moneyLiteral = money == null ? "NULL" :  money.toString();
        String attemptsLiteral = attempts == null ? "NULL" : attempts.toString();
        String sql = "INSERT INTO income (timestamp, value, position, attempts) VALUES ("
          + String.join(", ", timestampLiteral, moneyLiteral, posLiteral, attemptsLiteral) + ")";
        return connection.execute(sql);
    }

    public static CompletableFuture<QueryResult> prepare(SqpConnection connection, OffsetDateTime timestamp,
                                                         Double money, double[] pos, Short attempts) {
        return clear(connection).thenCompose(v -> insert(connection, timestamp, money, pos, attempts));
    }

}
