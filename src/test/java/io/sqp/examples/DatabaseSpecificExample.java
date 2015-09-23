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
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

public class DatabaseSpecificExample {
    public static void main(String[] args) {
        try (SqpConnection connection = SqpConnection.create()) {
            connection.connect("localhost", 8080, "/", "exampleDB").thenRunAsync(() -> {
                try {
                    testWaypoints(connection);
                } catch (Exception e) {
                    report(e);
                }
            }).join();
        } catch (Exception e) {
            report(e);
        }
    }

    private static void testWaypoints(SqpConnection connection) throws IOException, SqpException {
        connection.getInformation(String.class, InformationSubject.DBMSName)
                  .thenAccept(name -> System.out.println("DBMS: " + name));
        connection.execute("DROP TABLE IF EXISTS waypoints");
        connection.execute("CREATE TABLE waypoints " +
                "(" +
                "  \"id\" serial," +
                "  \"timestamp\" timestamp with time zone NOT NULL," +
                "  \"position\" point NOT NULL" +
                ")").thenRun(() -> System.out.println("Created table."));

        OffsetDateTime now = OffsetDateTime.now();
        OffsetDateTime[] timestamps = {
                now.minusHours(1).withNano(123456789), now.withNano(987654321)};
        double[][] positions = {{1.5, -3.2}, {1.2, -2.8}};
        System.out.println("Data to insert: ");
        for (int i = 0; i < timestamps.length; i++) {
            printWaypoint(i + 1, positions[i], timestamps[i]);
        }

        String insertStmt = "INSERT INTO waypoints " +
                "(timestamp, position) VALUES (?, ?)";
        try (PreparedStatement stmt = connection.prepare(insertStmt)) {
            stmt.bind(0, timestamps[0]).bind(1, "pg_point", positions[0]).addBatch()
                .bind(0, timestamps[1]).bind(1, "pg_point", positions[1]);
            stmt.executeUpdate().thenAccept(
                res -> System.out.println(
                       "Inserted " + res.getAffectedRows() + " rows."));
        }

        String selectStmt = "SELECT * FROM waypoints ORDER BY id";
        connection.allowReceiveNativeTypes("pg_point");
        try (Cursor cursor = connection.executeSelect(selectStmt).join()) {
            System.out.println("Actual data: ");
            while (cursor.nextRow()) {
                printWaypoint(
                        cursor.at("id").asInt(),
                        cursor.at("position").as(double[].class),
                        cursor.at("timestamp").asOffsetDateTime()
                );
            }
        }
    }

    private static void printWaypoint(int id, double[] pos, OffsetDateTime timestamp) {
        String isoDT = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(timestamp);
        System.out.println("Waypoint " + id + ": (x=" + pos[0] + "," +
                "y=" + pos[1] + ") at " + isoDT);
    }

    private static void report(Exception e) {
        System.out.println("Error occurred: " + e.getMessage());
    }

}
