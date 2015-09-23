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

package io.sqp.client;

import io.sqp.core.exceptions.InvalidOperationException;
import org.hamcrest.MatcherAssert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import io.sqp.client.testhelpers.BirthdayTable;
import io.sqp.client.testhelpers.IncomeTable;
import io.sqp.client.testhelpers.OtherUtils;
import io.sqp.core.types.SqpTypeCode;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.testng.Assert.fail;

/**
 * @author Stefan Burnicki
 */
public class PreparedStatementTest extends AutoConnectTestBase {

    @BeforeMethod
    public void ClearWeatherTable() {
        clearTestTable(connection).join();
    }

    @Test
    public void CanUsePreparedStatementWithSimpleInsert() throws Exception {
        PreparedStatement prep = connection.prepare(
                "INSERT INTO weather (city, temp_lo, temp_hi, prob, date) " +
                        "VALUES      ('BarCity', -1, 1, 0.000002, '1990-01-03')");
        UpdateResult updateRes = prep.execute(UpdateResult.class, false).join();
        assertThat(updateRes.getAffectedRows(), is(1));
        Cursor cursor = connection.execute(Cursor.class, "SELECT COUNT(*) FROM weather WHERE city='BarCity'").join();
        cursor.nextRow();
        MatcherAssert.assertThat(cursor.at(0).asInt(), is(1));
    }

    // TODO: enable this @Test
    public void CanUsePreparedStatementTwiceWithSimpleInsert() throws Exception {
        PreparedStatement prep = connection.prepare(
                "INSERT INTO weather (city, temp_lo, temp_hi, prob, date) " +
                        "VALUES      ('BarCity', -1, 1, 0.000002, '1990-01-03')");
        CompletableFuture<UpdateResult> updateFuture1 = prep.execute(UpdateResult.class, false);
        CompletableFuture<UpdateResult> updateFuture2 = prep.execute(UpdateResult.class, false);
        assertThat(updateFuture2.get().getAffectedRows(), is(1));
        assertThat(updateFuture1.get().getAffectedRows(), is(1));
        Cursor cursor = connection.execute(Cursor.class, "SELECT COUNT(*) FROM weather WHERE city='BarCity'").join();
        cursor.nextRow();
        MatcherAssert.assertThat(cursor.at(0).asInt(), is(2));
    }

    @Test
    public void CanUsePreparedStatementTwiceWithParameters() throws Exception {
        PreparedStatement prep = connection.prepare(
                "INSERT INTO weather (city, temp_lo, temp_hi, prob, date) " +
                        "VALUES      (?, -1, 1, 0.000002, '1990-01-03')");
        prep.bind(0, "BazCity");
        CompletableFuture<UpdateResult> updateFuture1 = prep.execute(UpdateResult.class, false);
        prep.bind(0, "FooCity").addBatch();
        prep.bind(0, "BarCity");
        CompletableFuture<UpdateResult> updateFuture2 = prep.execute(UpdateResult.class, false);
        assertThat(updateFuture2.get().getAffectedRows(), is(2));
        assertThat(updateFuture1.get().getAffectedRows(), is(1));
        Cursor cursor = connection.execute(Cursor.class,
                "SELECT city FROM weather WHERE city LIKE '%City' ORDER BY city"
        ).join();

        assertThat(cursor.nextRow(), is(true));
        MatcherAssert.assertThat(cursor.at(0).asString(), is("BarCity"));

        assertThat(cursor.nextRow(), is(true));
        MatcherAssert.assertThat(cursor.at(0).asString(), is("BazCity"));

        assertThat(cursor.nextRow(), is(true));
        MatcherAssert.assertThat(cursor.at(0).asString(), is("FooCity"));
    }

    @Test
    public void CanUseTwoPreparedStatementsInParallel() throws Exception {
        PreparedStatement prep1 = connection.prepare(
                "INSERT INTO weather (city, temp_lo, temp_hi, prob, date) " +
                        "VALUES      ('BarCity', -1, 1, 0.000002, '1990-01-03')");
        PreparedStatement prep2 = connection.prepare(
                "INSERT INTO weather (city, temp_lo, temp_hi, prob, date) " +
                        "VALUES      ('FooCity', -100, 100, 1.2, '2009-05-29')");
        CompletableFuture<UpdateResult> updateFuture = prep2.execute(UpdateResult.class, false);
        assertThat(updateFuture.join().getAffectedRows(), is(1));
        updateFuture = prep1.execute(UpdateResult.class, false);
        assertThat(updateFuture.join().getAffectedRows(), is(1));

        Cursor cursor = connection.execute(Cursor.class, "SELECT COUNT(*) FROM weather WHERE city='FooCity'").join();
        cursor.nextRow();
        // if all works correctly, the prep2 and prep1 were executed only once, so the SELECT should be 1. If the
        // prepared statements somehow got mixed up, the count would be either 2 or 0
        MatcherAssert.assertThat(cursor.at(0).asInt(), is(1));
    }

    @Test
    public void ConnectionCloseClosesPreparedStmt() throws Exception {
        PreparedStatement prep1 = connection.prepare(
                "INSERT INTO weather (city, temp_lo, temp_hi, prob, date) " +
                        "VALUES      ('BarCity', -1, 1, 0.000002, '1990-01-03')");
        connection.close();
        assertThat(prep1.isClosed(), is(true));
        try {
            prep1.execute().join();
            fail("Execute doesn't throw after connection close.");
        } catch (CompletionException e) {
            assertThat(e.getCause(), is(instanceOf(InvalidOperationException.class)));
        }
    }

    @Test
    public void ClosedStatementCanNotBeExecuted() throws Exception {
        PreparedStatement prep1 = connection.prepare(
                "INSERT INTO weather (city, temp_lo, temp_hi, prob, date) " +
                        "VALUES      ('BarCity', -1, 1, 0.000002, '1990-01-03')");
        prep1.close();
        assertThat(prep1.isClosed(), is(true));
        try {
            prep1.execute().join();
            fail("Execute doesn't throw after statement close.");
        } catch (CompletionException e) {
            assertThat(e.getCause(), is(instanceOf(InvalidOperationException.class)));
        }
    }

    @Test
    public void SimpleParameterBindWorks() throws Exception {
        PreparedStatement stmt = connection.prepare(
                "INSERT INTO weather (city, temp_lo, temp_hi, prob, date) VALUES (?, ?, ?, ?, ?)"
        );
        LocalDate today = LocalDate.now();
        // bind in some strange order to test that this works
        double floatVal = 0.12455454;
        stmt.bind(0, "ElRey").bind(4, today).bind(3, floatVal).bind(1, -10).bind(2, 0);
        CompletableFuture<UpdateResult> insertFuture = stmt.execute(UpdateResult.class);
        Cursor cursor = insertFuture.thenCompose(v ->
                connection.execute(Cursor.class, "SELECT * FROM weather")
        ).join();
        assertThat(insertFuture.get().getAffectedRows(), is(1));
        assertThat(cursor.nextRow(), is(true));
        MatcherAssert.assertThat(cursor.at("city").asString(), is("ElRey"));
        MatcherAssert.assertThat(cursor.at("temp_lo").asInt(), is(-10));
        MatcherAssert.assertThat(cursor.at("temp_hi").asInt(), is(0));
        assertThat((double) cursor.at("prob").asFloat(), is(closeTo(floatVal, 0.0000001)));
        MatcherAssert.assertThat(cursor.at("date").asLocalDate(), is(today));
    }

    // TODO: test with wrong parameter binding: too many

    // TODO: test with wrong parameter binding: not enough

    // TODO: test with wrong parameter binding: wrong type

    // TODO: test with null value binding

    // TODO: test with implicit null value binding by leaving out parameters

    // TODO: what happens on batch select?

    @Test
    public void ParameterBatchBindingWorks() throws Exception {
        PreparedStatement stmt = connection.prepare(
                "INSERT INTO weather (city, temp_lo, temp_hi, prob, date) VALUES (?, ?, ?, ?, ?)"
        );
        LocalDate today = LocalDate.now();
        LocalDate secondDate = LocalDate.of(1990, 12, 31);
        // bind in some strange order to test that this works
        double floatVal = 0.12455454;
        stmt.bind(0, "ElRey").bind(4, today).bind(3, floatVal).bind(1, -10).bind(2, 0);
        stmt.addBatch();
        stmt.bind(0, "Los Santos").bind(1, 10).bind(2, 10000).bind(3, 0.0).bind(4, secondDate);
        CompletableFuture<UpdateResult> insertFuture = stmt.execute(UpdateResult.class);

        Cursor cursor = insertFuture.thenCompose(v ->
                        connection.execute(Cursor.class, "SELECT * FROM weather")
        ).join();
        assertThat(insertFuture.get().getAffectedRows(), is(2));
        assertThat(cursor.nextRow(), is(true));
        MatcherAssert.assertThat(cursor.at("city").asString(), is("ElRey"));
        MatcherAssert.assertThat(cursor.at("temp_lo").asInt(), is(-10));
        MatcherAssert.assertThat(cursor.at("temp_hi").asInt(), is(0));
        assertThat((double) cursor.at("prob").asFloat(), is(closeTo(floatVal, 0.0000001)));
        MatcherAssert.assertThat(cursor.at("date").asLocalDate(), is(today));
        assertThat(cursor.nextRow(), is(true));
        MatcherAssert.assertThat(cursor.at("city").asString(), is("Los Santos"));
        MatcherAssert.assertThat(cursor.at("temp_lo").asInt(), is(10));
        MatcherAssert.assertThat(cursor.at("temp_hi").asInt(), is(10000));
        MatcherAssert.assertThat(cursor.at("prob").asFloat(), is(0.0f));
        MatcherAssert.assertThat(cursor.at("date").asLocalDate(), is(secondDate));
    }

    // TODO: test with batch binding: one fails

    @Test(groups = {"native-postgres-only"})
    public void CanUseNativePGDataType() throws Exception {
        IncomeTable.clear(connection).join();
        PreparedStatement stmt = connection.prepare("INSERT INTO income (timestamp, value, position, attempts) VALUES(?,?,?,?)");

        OffsetDateTime now = OffsetDateTime.now();
        BigDecimal money = BigDecimal.valueOf(3.45);
        double[] point = {-1.22222, 56.12345};
        stmt.bind(0, now).bind(1, money).bind(2, "pg_point", point).bindNull(3, SqpTypeCode.SmallInt);
        int affected = stmt.execute(UpdateResult.class).join().getAffectedRows();
        assertThat(affected, is(1));

        Cursor cursor = connection.executeSelect("SELECT * FROM income").join();
        assertThat(cursor.nextRow(), is(true));
        BigDecimal error = BigDecimal.valueOf(0.000001);
        MatcherAssert.assertThat(cursor.at("value").asBigDecimal(), is(closeTo(money, error)));
        MatcherAssert.assertThat(cursor.at("timestamp").asOffsetDateTime(), is(now));
        MatcherAssert.assertThat(cursor.at("attempts").isNull(), is(true));
        MatcherAssert.assertThat(cursor.at("position").asString().matches("^\\(-1\\.22\\d+,56\\.1234\\d+\\)$"), is(true)); // use regex to roughly check double data
    }

    @Test(groups = {"native-transbase-only"})
    public void CanUseNativeTransbaseDataType() throws Exception {
        BirthdayTable.clear(connection).join();
        PreparedStatement stmt = connection.prepare("INSERT INTO birthday (birthday, firstname, picture) VALUES(?,?,?)");

        int month = 5;
        int day = 29;
        String name = "Joe";
        byte[] picture = new byte[512];
        new Random().nextBytes(picture);

        stmt.bind(0, "tb_datetime[mo:dd]", new int[] {month, day}).bind(1, name).bind(2, picture);
        int affected = stmt.execute(UpdateResult.class).join().getAffectedRows();
        assertThat(affected, is(1));

        Cursor cursor = connection.executeSelect("SELECT * FROM birthday").join();
        assertThat(cursor.nextRow(), is(true));
        MatcherAssert.assertThat(cursor.at("birthday").asLocalDate(), is(LocalDate.of(1970, month, day))); // filled with epoch
        MatcherAssert.assertThat(cursor.at("firstname").asString(), is(OtherUtils.padRight(name, 20)));
        MatcherAssert.assertThat(cursor.at("picture").asBytes(), is(picture));
    }

    @Test(groups = {"native-postgres-only"})
    public void NativeTypeSendAndReceive() throws Exception {
        IncomeTable.clear(connection);

        PreparedStatement stmt = connection.prepare(
           "INSERT INTO income (timestamp, value, position, attempts)" +
           "VALUES(?,?,?,?)");

        OffsetDateTime now = OffsetDateTime.now();
        BigDecimal money = BigDecimal.valueOf(3.45);
        double[] point = {-1.22222, 56.12345};
        stmt.bind(0, now).bind(1, money).bind(2, "pg_point", point)
                .bindNull(3, SqpTypeCode.SmallInt);
        int affected = stmt.execute(UpdateResult.class).join().getAffectedRows();
        assertThat(affected, is(1));

        connection.allowReceiveNativeTypes("pg_point");
        Cursor cursor = connection.executeSelect("SELECT * FROM income").join();
        assertThat(cursor.nextRow(), is(true));
        BigDecimal error = BigDecimal.valueOf(0.000001);
        MatcherAssert.assertThat(cursor.at("value").asBigDecimal(), is(closeTo(money, error)));
        MatcherAssert.assertThat(cursor.at("timestamp").asOffsetDateTime(), is(now));
        MatcherAssert.assertThat(cursor.at("attempts").isNull(), is(true));

        double[] value = cursor.at("position").as(double[].class);
        assertThat(value[0], is(closeTo(point[0], 0.000001)));
        assertThat(value[1], is(closeTo(point[1], 0.000001)));
    }
}
