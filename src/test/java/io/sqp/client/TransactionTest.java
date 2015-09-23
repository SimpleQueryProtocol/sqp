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
import org.hamcrest.Matcher;
import org.hamcrest.collection.IsArrayContainingInAnyOrder;
import org.hamcrest.core.Is;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import io.sqp.core.exceptions.SqpException;

import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

/**
 * @author Stefan Burnicki
 */
public class TransactionTest extends AutoConnectTestBase {

    @BeforeMethod
    public void ClearTestTable() {
        clearTestTable(connection).join();
    }

    @Test
    public void TurningOffAutocommitSucceeds() {
        connection.setAutoCommit(false).join();
        assertThat(connection.getAutoCommit(), is(false));
    }

    @Test
    public void UsingCommitWithAutocommitFails() {
        connection.commit().handle((v, error) -> {
            assertThat(error, is(not(nullValue())));
            assertThat(error, is(instanceOf(InvalidOperationException.class)));
            return true;
        }).join();
        assertThat(connection.getAutoCommit(), is(true));
    }

    @Test
    public void UsingRollbackWithAutocommitFails() {
        connection.rollback().handle((v, error) -> {
            assertThat(error, is(not(nullValue())));
            assertThat(error, is(instanceOf(InvalidOperationException.class)));
            return true;
        }).join();
        assertThat(connection.getAutoCommit(), is(true));
    }

    @Test
    public void InsertWithCommitAffectsTable() throws SqpException {
        checkHasData(); // no data in table
        connection.setAutoCommit(false).join();
        connection.execute(
                "INSERT INTO weather (city, temp_lo, temp_hi, prob, date) " +
                        "VALUES              ('test1', 0, 50, 0.1, '2015-01-01')");
        connection.execute(
                "INSERT INTO weather (city, temp_lo, temp_hi, prob, date) " +
                        "VALUES              ('test2', -10, 3, 9.99999, '1111-11-11')");
        connection.commit().join();
        assertThat(connection.getAutoCommit(), is(false)); // still off
        checkHasData("test1", "test2"); // data in table since transaction committed both inserts
    }

    @Test
    public void InsertWithRollbackDoesNotAffectTable() throws SqpException {
        checkHasData(); // no data in table
        connection.setAutoCommit(false).join();
        connection.execute(
                "INSERT INTO weather (city, temp_lo, temp_hi, prob, date) " +
                        "VALUES              ('test1', 0, 50, 0.1, '2015-01-01')");
        connection.execute(
                "INSERT INTO weather (city, temp_lo, temp_hi, prob, date) " +
                        "VALUES              ('test2', -10, 3, 9.99999, '1111-11-11')");
        connection.rollback().join();
        assertThat(connection.getAutoCommit(), is(false)); // still off
        checkHasData(); // no data in table since rollback rolled back both inserts
    }

    @Test
    public void InsertWithCommitAndRollbackAffectsTablePartially() throws SqpException {
        checkHasData(); // no data in table
        connection.setAutoCommit(false).join();
        connection.execute(
                "INSERT INTO weather (city, temp_lo, temp_hi, prob, date) " +
                        "VALUES              ('test1', 0, 50, 0.1, '2015-01-01')");
        connection.commit();
        connection.execute(
                "INSERT INTO weather (city, temp_lo, temp_hi, prob, date) " +
                        "VALUES              ('test2', -10, 3, 9.99999, '1111-11-11')");
        connection.rollback();
        checkHasData("test1"); // data in table from the first transaction
    }

    @Test
    public void FailingOperationAndCommitResultsInRollback() throws SqpException {
        checkHasData(); // no data in table
        connection.setAutoCommit(false).join();
        connection.execute(
                "INSERT INTO weather (city, temp_lo, temp_hi, prob, date) " +
                        "VALUES              ('test1', 0, 50, 0.1, '2015-01-01'");
        connection.execute(
                "INSERT INTO weather (city, temp_lo, temp_hi, prob, date, fooo) VALUES ()" //wrong
        );
        // just try a commit anyway
        connection.commit();

        checkHasData(); // since both inserts were in the same transaction, the table should be empty
    }

    @Test
    public void EnablingAutoCommitAgainCommitsTransaction() throws SqpException {
        checkHasData(); // no data in table
        connection.setAutoCommit(false).join();
        connection.execute(
                "INSERT INTO weather (city, temp_lo, temp_hi, prob, date) " +
                        "VALUES              ('test1', 0, 50, 0.1, '2015-01-01')");
        connection.execute(
                "INSERT INTO weather (city, temp_lo, temp_hi, prob, date) " +
                        "VALUES              ('test2', -10, 3, 9.99999, '1111-11-11')");
        connection.setAutoCommit(true).join();
        assertThat(connection.getAutoCommit(), is(true));
        checkHasData("test1", "test2"); // data in table since transaction committed both inserts
    }

    private void checkHasData(String... expectedCities) throws SqpException {
        Cursor cursor = connection.execute(Cursor.class, "SELECT city FROM weather").join();
        Collection<Matcher<String>> cities = new ArrayList<>();
        while(cursor.nextRow()) {
            cities.add(Is.is(cursor.at(0).asString()));
        }
        assertThat(expectedCities, new IsArrayContainingInAnyOrder(cities));
    }
}
