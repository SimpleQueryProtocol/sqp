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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import io.sqp.client.exceptions.UnexpectedResultTypeException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.testng.FileAssert.fail;

/**
 * @author Stefan Burnicki
 */
public class SimpleQueryTest extends AutoConnectTestBase {

    @BeforeMethod
    public void ClearTestTable() {
        clearTestTable(connection).join();
    }

    @Test
    public void SimpleQueryUpdateWorks() throws Exception {
        String query = "INSERT INTO " + TEST_TABLE + " (city, temp_lo, temp_hi, prob, date)" +
                        "VALUES " +                  " ('Tuebingen', -13, 34, 1.987654321, '2014-02-02')";
        UpdateResult result = connection.execute(UpdateResult.class, query).join();
        assertThat(result.getAffectedRows(), is(1));
    }

    @Test
    public void SimpleQueryWithWrongResultTypeThrows() throws Exception {
        String query = "INSERT INTO " + TEST_TABLE + " (city, temp_lo, temp_hi, prob, date)" +
                        "VALUES " +                   " ('Tuebingen', -13, 34, 1.987654321, '2014-02-02')";
        Throwable thrown = connection.execute(Cursor.class, query).handle((cursor, exception) -> {
            if (cursor != null) {
                fail();
            }
            return exception;
        }).join();
        assertThat(thrown, is(not(nullValue())));
        assertThat(thrown, is(instanceOf(UnexpectedResultTypeException.class)));
        UnexpectedResultTypeException error = (UnexpectedResultTypeException) thrown;
        assertThat(error.getResult(), is(not(nullValue())));
        assertThat(error.getResult(), is(instanceOf(UpdateResult.class)));
        assertThat(error.getExpectedClass().getName(), is(Cursor.class.getName()));
    }
}
