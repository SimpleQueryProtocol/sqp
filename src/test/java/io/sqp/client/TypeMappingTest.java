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

import io.sqp.client.exceptions.ErrorResponseException;
import io.sqp.client.testhelpers.OtherUtils;
import io.sqp.core.ErrorType;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.testng.annotations.Test;
import io.sqp.client.testhelpers.BirthdayTable;
import io.sqp.core.exceptions.SqpException;

import java.util.Random;
import java.util.concurrent.CompletionException;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.testng.FileAssert.fail;

/**
 * @author Stefan Burnicki
 */
public class TypeMappingTest extends AutoConnectTestBase {
    public final static String REPEATING_DATE_SCHEMA = "{ \n" +
            "  \"type\": \"array\", \n" +
            "  \"minItems\": 2, \n" +
            "  \"additionalItems\": false, \n" +
            "  \"items\": [\n" +
            "    {\"type\": \"integer\", \"minimum\": 1, \"maximum\": 12}, \n" +
            "    {\"type\": \"integer\", \"minimum\": 1, \"maximum\": 31}\n" +
            "  ]\n" +
            "}";

    @Test(groups = {"without-jdbc-postgres"})
    public void CanRegisterTypeMapping() {
        String orig = connection.registerTypeMapping("repeatingDate", REPEATING_DATE_SCHEMA, "point", "[mo:dd]").join();
        assertThat(orig, anyOf(is("pg_point"), is("tb_DATETIME[MO:DD]")));
    }

    @Test(groups = {"without-jdbc-postgres"})
    public void RegisteringUnavailableTypesResultsInException() {
        // this should just not throw an exception
        String unavailableSchema = "{ \"type\": \"array\", \"items\":[ { \"type\": \"string\" }, { \"type\": \"integer\"} ] }";
        try {
            connection.registerTypeMapping("invalid", unavailableSchema).join();
        } catch (CompletionException e) {
            assertThat(e.getCause(), is(instanceOf(ErrorResponseException.class)));
            ErrorResponseException cause = (ErrorResponseException) e.getCause();
            assertThat(cause.getErrorType(), CoreMatchers.is(ErrorType.TypeMappingNotPossible));
        }
    }

    @Test(groups = {"without-jdbc-postgres"})
    public void CanSendMappedTypeData() throws SqpException {
        String name = "Max";
        byte[] picture = new byte[1111];
        new Random().nextBytes(picture);
        int[] birthday = {12, 31};

        connection.registerTypeMapping("repeatingDate", REPEATING_DATE_SCHEMA, "point", "[mo:dd]");
        PreparedStatement stmt = connection.prepare("INSERT into birthday(firstname, birthday, picture) VALUES (?, ?, ?)");
        stmt.bind(0, name).bind(1, "repeatingDate", birthday).bind(2, picture);
        UpdateResult result = stmt.executeUpdate().join();

        assertThat(result.getAffectedRows(), is(1));
    }

    @Test(groups = {"without-jdbc-postgres"})
    public void SendingMappedTypeOutOfBoundsFails() throws SqpException {
        String name = "Foo";
        byte[] picture = new byte[1];
        new Random().nextBytes(picture);
        int[] birthday = {2, 0};

        connection.registerTypeMapping("repeatingDate", REPEATING_DATE_SCHEMA, "point", "[mo:dd]");
        PreparedStatement stmt = connection.prepare("INSERT into birthday(firstname, birthday, picture) VALUES (?, ?, ?)");
        stmt.bind(0, name).bind(1, "repeatingDate", birthday).bind(2, picture);
        try {
            stmt.executeUpdate().join();
            fail("Insert with invalid field worked!");
        } catch (CompletionException e) {
            assertThat(e.getCause(), is(instanceOf(ErrorResponseException.class)));
            ErrorResponseException cause = (ErrorResponseException) e.getCause();
            assertThat(cause.getErrorType(), is(ErrorType.ValidationFailed));
        }
    }

    @Test(groups = {"without-jdbc-postgres"})
    public void CanReceiveMappedTypeData() throws SqpException {
        String name = "Max";
        byte[] picture = new byte[2];
        new Random().nextBytes(picture);
        int[] birthday = {12, 31};

        BirthdayTable.clear(connection);
        connection.registerTypeMapping("repeatingDate", REPEATING_DATE_SCHEMA, "point", "[mo:dd]");
        UpdateResult updateResult = connection.prepare("INSERT into birthday(firstname, birthday, picture) VALUES (?, ?, ?)")
                .bind(0, name).bind(1, "repeatingDate", birthday).bind(2, picture)
                .executeUpdate().join();
        assertThat(updateResult.getAffectedRows(), is(1));

        Cursor cursor = connection.executeSelect("SELECT firstname, birthday, picture FROM birthday").join();

        assertThat(cursor.nextRow(), is(true));
        MatcherAssert.assertThat(cursor.at("firstname").asString(), CoreMatchers.is(OtherUtils.padRight(name, 20)));
        MatcherAssert.assertThat(cursor.at("picture").asBytes(), is(picture));
        MatcherAssert.assertThat(cursor.at("birthday").as(int[].class), is(birthday));
    }
}
