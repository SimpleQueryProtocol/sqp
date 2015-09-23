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

import io.sqp.core.DataFormat;
import org.hamcrest.MatcherAssert;
import io.sqp.client.testhelpers.BirthdayTable;
import io.sqp.core.InformationSubject;
import io.sqp.core.exceptions.SqpException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.core.Is.is;

/**
 * @author Stefan Burnicki
 */
public class TextProtocolTest extends AutoConnectTestBase {

    @BeforeMethod
    public void ClearTestTable() {
        clearTestTable(connection).join();
    }

    @Override
    protected DataFormat getProtocolFormat() {
        return DataFormat.Text;
    }

    @Test
    public void TextConnectAndClose() throws Exception {
        defaultConnect(connection).join();
        assertThat(connection.isConnected(), is(true));
        connection.close();
        assertThat(connection.isConnected(), is(false));
    }

    @Test
    public void TextInformationRequest() {
        String answer = connection.getInformation(String.class, InformationSubject.DBMSName).join();
        assertThat(answer, is(dbmsName));
    }

    @Test
    public void TextParameterBindWorks() throws Exception {
        PreparedStatement stmt = connection.prepare(
          "INSERT INTO weather (city, temp_lo, temp_hi, prob, date)" +
          "VALUES (?,?,?,?,?)"
        );
        LocalDate today = LocalDate.now();
        // bind in some strange order to test that this works
        double floatVal = 0.12455454;
        stmt.bind(0, "ElRey").bind(4, today).bind(3, floatVal)
            .bind(1, -10).bind(2, 0);
        CompletableFuture<UpdateResult> insertFuture = stmt.executeUpdate();
        Cursor cursor = connection.executeSelect("SELECT * FROM weather").join();
        assertThat(insertFuture.get().getAffectedRows(), is(1));
        assertThat(cursor.nextRow(), is(true));
        MatcherAssert.assertThat(cursor.at("city").asString(), is("ElRey"));
        MatcherAssert.assertThat(cursor.at("temp_lo").asInt(), is(-10));
        MatcherAssert.assertThat(cursor.at("temp_hi").asInt(), is(0));
        MatcherAssert.assertThat(cursor.at("prob").asDouble(), is(closeTo(floatVal, 0.0000001)));
        MatcherAssert.assertThat(cursor.at("date").asLocalDate(), is(today));
    }

    @Test
    public void TextSimpleQueryInsert() throws Exception {
        String query = "INSERT INTO " + TEST_TABLE + " (city, temp_lo, temp_hi, prob, date)" +
                       "VALUES " +                  " ('Tuebingen', -13, 34, 1.987654321, '2014-02-02')";
        UpdateResult result = connection.execute(UpdateResult.class, query).join();
        assertThat(result.getAffectedRows(), is(1));
    }

    @Test(groups = {"without-jdbc-postgres"})
    public void TextMappedTypeQuery() throws SqpException {
        String name = "Max";
        byte[] picture = new byte[2];
        new Random().nextBytes(picture);
        int[] birthday = {12, 31};

        BirthdayTable.clear(connection);
        connection.registerTypeMapping("repeatingDate", TypeMappingTest.REPEATING_DATE_SCHEMA, "point", "[mo:dd]");
        UpdateResult updateResult = connection.prepare("INSERT into birthday(firstname, birthday, picture) VALUES (?, ?, ?)")
                .bind(0, name).bind(1, "repeatingDate", birthday).bind(2, picture)
                .executeUpdate().join();
        assertThat(updateResult.getAffectedRows(), is(1));

        Cursor cursor = connection.executeSelect("SELECT firstname, birthday, picture FROM birthday").join();

        assertThat(cursor.nextRow(), is(true));
        MatcherAssert.assertThat(cursor.at("firstname").asString(), is("Max                 "));
        MatcherAssert.assertThat(cursor.at("picture").asBytes(), is(picture));
        MatcherAssert.assertThat(cursor.at("birthday").as(int[].class), is(birthday));
    }
}
