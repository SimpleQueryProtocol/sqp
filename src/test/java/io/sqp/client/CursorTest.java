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

import io.sqp.client.testhelpers.BirthdayTable;
import io.sqp.client.testhelpers.ColumnMetadataMatcher;
import io.sqp.client.testhelpers.IncomeTable;
import io.sqp.core.exceptions.CursorProblemException;
import io.sqp.core.exceptions.InvalidOperationException;
import io.sqp.core.types.SqpTypeCode;
import io.sqp.core.types.SqpValue;
import io.sqp.core.types.SqpVarChar;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.hamcrest.core.Is;
import org.testng.FileAssert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import io.sqp.core.ColumnMetadata;

import java.time.LocalDate;
import java.time.Month;
import java.time.OffsetDateTime;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.testng.Assert.fail;

/**
 * @author Stefan Burnicki
 */
public class CursorTest extends AutoConnectTestBase {

    @BeforeMethod
    public void ClearTableInsertData() {
        clearTestTable(connection).thenCompose(v -> insertData(connection)).join();
    }

    @Test
    public void ConnectionCloseClosesCursor() throws Exception {
        Cursor cursor = connection.execute(Cursor.class, "SELECT * FROM " + TEST_TABLE + " WHERE city LIKE '%City'").join();
        connection.close();
        assertThat(cursor.isClosed(), is(true));
        try {
            cursor.nextRow();
            fail("NextRow doesn't throw after connection close.");
        } catch (InvalidOperationException e) {
        }
    }

    @Test
    public void ClosedCursorIsClosed() throws Exception {
        Cursor cursor = connection.execute(Cursor.class, "SELECT * FROM " + TEST_TABLE + " WHERE city LIKE '%City'").join();
        cursor.close();
        assertThat(cursor.isClosed(), is(true));
        try {
            cursor.nextRow();
            fail("nextRow doesn't throw after cursor close.");
        } catch (InvalidOperationException e) {
        }
    }

    @Test
    public void SimpleQuerySelectWorks() throws Exception {
        Cursor cursor = connection.execute(Cursor.class,
                "SELECT * FROM " + TEST_TABLE + " WHERE city LIKE '%City' ORDER BY city DESC"
        ).join();

        assertThat(cursor, Is.is(not(nullValue())));

        // check column metadata first
        List<ColumnMetadata> columns = cursor.getColumnMetadata();
        assertThat(columns.size(), Is.is(5));
        assertThat(columns, Matchers.contains(
                ColumnMetadataMatcher.columnMetadataWith(SqpTypeCode.VarChar, "city"),
                ColumnMetadataMatcher.columnMetadataWith(SqpTypeCode.Integer, "temp_lo"),
                ColumnMetadataMatcher.columnMetadataWith(SqpTypeCode.Integer, "temp_hi"),
                ColumnMetadataMatcher.columnMetadataWith(SqpTypeCode.Real, "prob"),
                ColumnMetadataMatcher.columnMetadataWith(SqpTypeCode.Date, "date")
        ));

        // check for data
        assertThat(cursor.nextRow(), is(true));

        // check city field in detail
        SqpValue firstField = cursor.at(0);
        assertThat(firstField, is(instanceOf(SqpVarChar.class)));
        SqpVarChar strField = (SqpVarChar) firstField;
        assertThat(strField.asString(), is("TestCity"));

        // check named date field for correct conversion
        LocalDate date = cursor.at("date").asLocalDate();
        assertThat(date.getYear(), is(1015));
        assertThat(date.getMonth(), is(Month.JULY));
        assertThat(date.getDayOfMonth(), is(12));

        // make sure the next row has different data
        assertThat(cursor.nextRow(), is(true));
        MatcherAssert.assertThat(cursor.at("city").asString(), is("FooCity"));
        assertThat(cursor.nextRow(), is(false)); // should be at end
        // is not scrollable, so previousRow is not available
        try {
            cursor.previousRow();
            FileAssert.fail();
        } catch (CursorProblemException e) {
        }
    }


    @Test
    public void TwoDifferentCursorsAtSameTime() throws Exception {
        Cursor cursor1 = connection.execute(Cursor.class, false,
                        "SELECT * FROM " + TEST_TABLE + " WHERE city LIKE '%City' ORDER BY city ASC"
        ).join();
        Cursor cursor2 = connection.execute(Cursor.class,
                false, "SELECT * FROM " + TEST_TABLE + " ORDER BY city DESC").join();

        // first check the cursors for correct data
        assertThat(cursor1.nextRow(), is(true));
        MatcherAssert.assertThat(cursor1.at("city").asString(), is("FooCity"));
        assertThat(cursor2.nextRow(), is(true));
        MatcherAssert.assertThat(cursor2.at("city").asString(), is("T\u00dcbingen"));

        // now it's getting interesting, as the cursors need to fetch more data from the server
        assertThat(cursor2.nextRow(), is(true));
        MatcherAssert.assertThat(cursor2.at("city").asString(), is("TestCity"));
        assertThat(cursor2.nextRow(), is(true));
        MatcherAssert.assertThat(cursor2.at("city").asString(), is("Stuttgart"));

        assertThat(cursor1.nextRow(), is(true));
        MatcherAssert.assertThat(cursor1.at("city").asString(), is("TestCity"));
        assertThat(cursor1.nextRow(), is(false)); // no more rows

        assertThat(cursor2.nextRow(), is(true));
        MatcherAssert.assertThat(cursor2.at("city").asString(), is("FooCity"));
        assertThat(cursor2.nextRow(), is(false));
    }

    @Test
    public void SimpleQueryWithScrollableCursorWorks() throws Exception {
        Cursor cursor = connection.execute(Cursor.class, true,
                "SELECT * FROM " + TEST_TABLE + " WHERE city LIKE '%City' ORDER BY city DESC"
        ).join();

        assertThat(cursor.nextRow(), is(true));
        assertThat(cursor.nextRow(), is(true));
        assertThat(cursor.nextRow(), is(false));
        // cursor should be at end now
        assertThat(cursor.previousRow(), is(true));
        // cursor is now on first data set
        MatcherAssert.assertThat(cursor.at("city").asString(), is("FooCity"));
        assertThat(cursor.previousRow(), is(true));
        MatcherAssert.assertThat(cursor.at("city").asString(), is("TestCity"));
        assertThat(cursor.previousRow(), is(false));
    }

    // TODO: test with one statement but two cursors!

    @Test(groups = {"native-postgres-only"})
    public void CanGetNativeData() throws Exception {
        double[] pos = {1.5, -3.2};
        Cursor cursor = IncomeTable.prepare(connection, OffsetDateTime.now(), 0.0, pos, null).thenCompose( v->
                        connection.allowReceiveNativeTypes("pg_point")
        ).thenCompose(v -> connection.execute(Cursor.class,
                "SELECT position FROM income"
        )).join();

        assertThat(cursor.nextRow(), is(true));
        Object value = cursor.at(0).asObject();
        assertThat(value, instanceOf(List.class));
        List list = (List) value;
        assertThat(list.size(), is(2));
        assertThat(list.get(0), instanceOf(Double.class));
        assertThat(list.get(1), instanceOf(Double.class));
        assertThat((double) list.get(0), is(closeTo(pos[0], 0.000001)));
        assertThat((double) list.get(1), is(closeTo(pos[1], 0.000001)));
    }

    @Test(groups = {"native-postgres-only"})
    public void CanGetNativeDataAndQuicklyMapIt() throws Exception {
        double[] pos = {1.5, -3.2};
        Cursor cursor = IncomeTable.prepare(connection, OffsetDateTime.now(), 0.0, pos, null).thenCompose( v->
                        connection.allowReceiveNativeTypes("pg_point")
        ).thenCompose(v -> connection.execute(Cursor.class,
                "SELECT position FROM income"
        )).join();

        assertThat(cursor.nextRow(), is(true));
        double[] value = cursor.at(0).as(double[].class);
        assertThat(value[0], is(closeTo(pos[0], 0.000001)));
        assertThat(value[1], is(closeTo(pos[1], 0.000001)));
    }

    @Test(groups = {"native-transbase-only"})
    public void CanGetNativeTransbaseDateTimeValue() throws Exception {
        int[] birthday = {10, 14};
        Cursor cursor = BirthdayTable.clear(connection).thenCompose(v ->
            connection.execute("INSERT INTO birthday (firstname, birthday) VALUES ('John', DATETIME[MO:DD](10-14))")
        ).thenCompose(v ->
            connection.allowReceiveNativeTypes("tb_datetime[mo:dd]")
        ).thenCompose(v -> connection.execute(Cursor.class,
            "SELECT birthday FROM birthday"
        )).join();

        assertThat(cursor.nextRow(), is(true));
        int[] value = cursor.at(0).as(int[].class);
        assertThat(value[0], is(birthday[0]));
        assertThat(value[1], is(birthday[1]));
    }
}
