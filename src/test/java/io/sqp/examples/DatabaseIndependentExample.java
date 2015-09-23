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
import io.sqp.client.SqpConnection;
import io.sqp.client.PreparedStatement;
import io.sqp.core.InformationSubject;
import io.sqp.core.exceptions.SqpException;

import java.io.IOException;
import java.time.Month;
import java.time.format.TextStyle;
import java.util.Locale;

/**
 * @author Stefan Burnicki
 * Tables that need to exist for use:
-- for transbase
CREATE TABLE friends
(
    name character varying(50),
    birthday DATETIME[MO:DD],
    height float
)

-- for postgres
CREATE TABLE friends
(
    name character varying(50),
    birthday point,
    height real
)
 */
public class DatabaseIndependentExample {
    public final static String MONTH_DAY_SCHEMA = "{ \n" +
            "  \"type\": \"array\", \n" +
            "  \"minItems\": 2, \n" +
            "  \"additionalItems\": false, \n" +
            "  \"items\": [\n" +
            "    {\"type\": \"integer\", \"minimum\": 1, \"maximum\": 12}, \n" +
            "    {\"type\": \"integer\", \"minimum\": 1, \"maximum\": 31}\n" +
            "  ]\n" +
            "}";

    public static void main(String[] args) {
        try (SqpConnection connection = SqpConnection.create()) {
            connection.connect("localhost", 8080, "/", "exampleDB");
            testBirthdays(connection);
        } catch (Exception e) {
            report(e);
        }
    }

    private static void testBirthdays(SqpConnection connection) throws IOException, SqpException {
        connection.execute("DELETE FROM friends");
        connection.getInformation(String.class, InformationSubject.DBMSName)
                .thenAccept(name -> System.out.println("DBMS: " + name));
        connection.registerTypeMapping("monthDay", MONTH_DAY_SCHEMA,
                                       "point", "[mo:dd]")
                .thenAccept(orig -> System.out.println("Mapping to " + orig));
        String[] names = { "John Doe", "Mary Jane" };
        int[][] birthdays = { {2, 29}, {12, 31} };
        float[] heights = { 1.84f, 1.59f };

        System.out.println("Data to insert: ");
        for (int i = 0; i < names.length; i++) {
            printPerson(names[i], birthdays[i], heights[i]);
        }

        String insertStmt = "INSERT INTO friends " +
                "(name, birthday, height) VALUES (?, ?, ?)";
        try (PreparedStatement stmt = connection.prepare(insertStmt)) {
            for (int i = 0; i < names.length; i++) {
                stmt.bind(0, names[i]).bind(2, heights[i])
                    .bind(1, "monthDay", birthdays[i]);
                if (i != names.length -1) stmt.addBatch();
            }
            stmt.executeUpdate().thenAccept(res -> System.out.println(
                "Inserted " + res.getAffectedRows() + " friends."));
        }

        String selectStmt = "SELECT * FROM friends";
        try (Cursor cursor = connection.executeSelect(selectStmt).join()) {
            System.out.println("Actual data: ");
            while (cursor.nextRow()) {
                printPerson(
                    cursor.at("name").asString(),
                    cursor.at("birthday").as(int[].class),
                    cursor.at("height").asFloat()
                );
            }
        }
    }

    private static void printPerson(String name, int[] birthday, float height) {
        String month = Month.of(birthday[0])
                .getDisplayName(TextStyle.SHORT, Locale.getDefault());
        System.out.println(name + " is " + height + "m tall and has birthday on "
                + month + ", " + birthday[1]);
    }

    private static void report(Exception e) {
        System.out.println("Error occurred: " + e.getMessage());
    }

}
