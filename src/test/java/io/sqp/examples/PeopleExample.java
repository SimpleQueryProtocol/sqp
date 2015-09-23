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
import io.sqp.core.InformationSubject;
import io.sqp.core.exceptions.SqpException;

import java.io.IOException;
import java.time.LocalDate;
import java.time.Month;

/*
 --pg
CREATE TABLE people
(
   name character varying(80),
   birthday date,
   height real
);


--tb
CREATE TABLE people
(
   name character varying(80),
   birthday date,
   height float
);
*/
public class PeopleExample {
    public static void main(String[] args) {
        try (SqpConnection connection = SqpConnection.create()) {
            connection.connect("localhost", 8080, "/", "exampleDB");
            testBirthdays(connection);
        } catch (Exception e) {
            report(e);
        }
    }

    private static void testBirthdays(SqpConnection connection) throws IOException, SqpException {
        connection.execute("DELETE FROM people");
        connection.getInformation(String.class, InformationSubject.DBMSName)
                .thenAccept(name -> System.out.println("DBMS: " + name));
        String insertStmt = "INSERT INTO people " +
                "(name, birthday, height) VALUES (?, ?, ?)";
        try (PreparedStatement stmt = connection.prepare(insertStmt)) {
            stmt.bind(0, "John Doe").bind(1, LocalDate.of(1989, Month.JULY, 31)).bind(2, 1.82f).addBatch()
                .bind(0, "Jan Bauer").bind(1, LocalDate.of(1960, Month.DECEMBER, 2)).bind(2, 1.48f);
            stmt.executeUpdate().thenAccept(res -> System.out.println(
                    "Inserted " + res.getAffectedRows() + " people."));
        }

        String selectStmt = "SELECT * FROM people";
        try (Cursor cursor = connection.executeSelect(selectStmt).join()) {
            System.out.println("Actual data: ");
            while (cursor.nextRow()) {
                System.out.println(
                    cursor.at("name").asString() +
                    " (" + cursor.at("height").asFloat() + "m) has bday on " +
                    cursor.at("birthday").asLocalDate());
            }
        }
    }

    private static void report(Exception e) {
        System.out.println("Error occurred: " + e.getMessage());
    }
}
