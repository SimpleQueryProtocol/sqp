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

package io.sqp.examples;

import io.sqp.client.Cursor;
import io.sqp.client.PreparedStatement;
import io.sqp.client.SqpConnection;
import io.sqp.core.ErrorType;
import io.sqp.core.exceptions.SqpException;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.logging.Level;

/**
 * @author Stefan Burnicki
 */
public class ConceptionalExample {

    public void UseMonadicFeatures() {
        try(SqpConnection conn = SqpConnection.create()) {
            conn.connect("localhost", 8080, "/", "proxytest");

            conn.executeUpdate("DELETE FROM users WHERE name='Foo'")
                .thenCompose(result -> {
                     System.out.println("Removed " + result.getAffectedRows() + " rows.");
                     String stmt = "INSERT INTO users (name, age) values ('Foo', 43)";
                     return conn.execute(stmt);
                }).thenCompose(result ->
                     conn.executeSelect("SELECT * FROM users WHERE name='Foo'")
                ).thenAcceptAsync(cursor ->
                     printAllResults(cursor, true)
                ).exceptionally(this::handleError);


        } catch (Exception e) {
            // deal with the connection error
        }
    }

    private Void handleError(Throwable error) {
        return null;
    }

    private void printAllResults(Cursor cursor, boolean foo) {

    }


    public void AsyncAPI() {
        try(SqpConnection connection = SqpConnection.create()) {
            connection.connect("localhost", 8080, "/", "proxytest");

            connection.executeUpdate("DELETE FROM sales WHERE revenue < 1.0").thenAccept(
                    updateResult -> log(Level.INFO, "Sanity check removed " + updateResult + " revenues < 1.00.")
            );

            PreparedStatement stmt = connection.prepare(
                    "SELECT count(s.revenue) as `revenue`, e.name as `name`" +
                            "FROM employees e, sales s" +
                            "WHERE employees.id = s.eid AND location = ? AND year > ? " +
                            "GROUP BY e.id");
            CompletableFuture<Cursor> future = stmt.bind(0, "Germany").bind(1, 2000).executeSelect();

            // the query is running, however, we can do some other
            timeExpensiveOperation();

            // now we actually need the results of the cursor
            try (Cursor cursor = future.join()) {
                System.out.println("German employee revenues since year 2000");
                while(cursor.nextRow()) {
                    System.out.println(cursor.at("name").asString() + " made a total of " +
                                       cursor.at("revenue").asBigDecimal());
                }
            } catch (CompletionException e) {
                // deal with the error while execution
            } catch (SqpException e) {
                // problems with getting data from the cursor (type conversion, etc)
            }
        } catch (SqpException e) {
            // problem with parameter binding
        } catch (IOException e) {
            // deal with the connection error
        }
    }

    private void log(Level info, String s) {

    }

    public void UseOfCursor() {
        try(SqpConnection connection = SqpConnection.create()) {
            CompletableFuture<SqpConnection> connect = connection.connect("localhost", 8080, "/", "proxytest");
            // we can do something while it's connection...
            // next row directly waits for the result as we don't have other things to do anyway
            try (Cursor cursor = connect.thenCompose(c -> c.execute(Cursor.class, "SELECT * FROM weather")).join()) {
                // iterate over the rows
                while(cursor.nextRow()) {
                    // do something with the current row
                    System.out.print(cursor.at("city").asString() + " low temp: " + cursor.at("temp_lo").asInt());
                }
            } catch (CompletionException e) {
                // execute went wrong
                ErrorType errorType = ((SqpException) e.getCause()).getErrorType();
                // deal with the error based on type
            } catch (SqpException e) {
                // cursor operations went wrong
            }
        } catch (IOException e) {
            // deal with it
        }
    }

    private void timeExpensiveOperation() {
        // something pretty time expensive
    }
}
