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

package io.sqp.client.impl;

import io.sqp.client.ConnectionTestBase;
import io.sqp.client.Cursor;
import io.sqp.client.exceptions.ErrorResponseException;
import io.sqp.core.ErrorType;
import org.hamcrest.MatcherAssert;
import org.hamcrest.core.Is;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import io.sqp.client.ClientConfig;
import io.sqp.core.ColumnMetadata;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;
import static org.testng.Assert.fail;

/**
 * @author Stefan Burnicki
 */
public class SqpConnectionImplTest extends ConnectionTestBase {
    private SqpConnectionImpl _connection;

    @BeforeMethod
    private void ConnectClearAndInsert() {
        ClientConfig config = ClientConfig.create().setCursorMaxFetch(1);
        _connection = spy(new SqpConnectionImpl(config));
        defaultConnect(_connection).thenCompose(
            v -> clearTestTable(_connection)
        ).thenCompose(
            v -> insertData(_connection)
        ).join();
    }

    @AfterMethod
    private void CloseConnection() throws IOException {
        _connection.close();
    }

    @Test
    public void CursorDoesAutoFetch() throws Exception {
        Cursor cursor = _connection.execute(Cursor.class, false,
                "SELECT * FROM " + TEST_TABLE + " WHERE city LIKE '%City'"
        ).join();
        assertThat(cursor.nextRow(), is(true)); // came directly from server
        assertThat(cursor.nextRow(), is(true)); // needed to be fetched because of max fetch=1
        assertThat(cursor.nextRow(), is(false)); // no more rows
        verify(_connection, atLeastOnce()).fetch(anyObject(), eq(true)); // fetch should have been called exactly once
    }

    // TODO: enable this test as soon as there is real support for positional fetch
    // @Test()
    public void CursorCanBeScrollableWithAutoFetch() throws Exception {
        Cursor cursor = _connection.execute(Cursor.class, true, "SELECT * FROM " + TEST_TABLE + " WHERE city LIKE '%City'").join();
        assertThat(cursor.nextRow(), is(true)); // came directly from server
        assertThat(cursor.nextRow(), is(true)); // needed to be fetched because of max fetch=1
        verify(_connection, times(1)).fetch(anyObject(), eq(true)); // fetch should have been called exactly once
        MatcherAssert.assertThat(cursor.at(0).asString(), is("FooCity")); // cursor is at last data set
        // now the tricky thing: we create a new cursor with the same id, but it will have an empty
        // buffer. Because the protocol works id-based, the cursor should attempt to get data from
        // the server... backwards!

        CursorImpl emptyScrollable = new CursorImpl(_connection, ((CursorImpl) cursor).getId(),
                cursor.getColumnMetadata(), true);
        assertThat(emptyScrollable.previousRow(), is(true)); // previous data set should be the first
        assertThat(emptyScrollable.at(0).asString(), is("TestCity"));
        verify(_connection, times(1)).fetch(anyObject(), eq(false)); // another fetch happened
    }

    @Test
    public void SameCursorIdIsSameServerCursor() throws Exception {
        CursorImpl cursor = (CursorImpl) _connection.execute(Cursor.class, true,
                "SELECT * FROM " + TEST_TABLE + " ORDER BY city ASC"
        ).join();
        assertThat(cursor.nextRow(), is(true));
        assertThat(cursor.at("city").asString(), is("FooCity"));
        assertThat(cursor.nextRow(), is(true));
        assertThat(cursor.at("city").asString(), is("Stuttgart"));

        // now we create another cursor with the same id, but it will have an empty buffer. So it will fetch data
        // from the server. As the server doesn't know client objects, only cursor ids, it will just send the next data set
        CursorImpl newCursor = new CursorImpl(_connection, cursor.getId(), cursor.getColumnMetadata(), false);
        assertThat(newCursor.nextRow(), is(true));
        assertThat(newCursor.at("city").asString(), is("TestCity"));

        // now we close the first cursor, which should of course also close the server cursor
        cursor.close();
        // as a consequence, fetching new results from the second cursor should throw an exception that the cursor
        // doesn't exist
        try {
            newCursor.nextRow();
            fail("Looks like the server cursor still exists");
        } catch (ErrorResponseException e) {
            assertThat(e.getErrorType(), Is.is(ErrorType.CursorProblem));
        }
    }


    @Test(groups = {"native-transbase-only"})
    public void NativeTransbaseDateTimeHasSpecifiers() throws Exception {
        CursorImpl cursor = (CursorImpl) _connection.execute(Cursor.class, true,
                "SELECT date FROM " + TEST_TABLE + " ORDER BY city ASC"
        ).join();
        ColumnMetadata metadata = cursor.getColumnMetadata().get(0);
        assertThat(metadata.getNativeType(), is("tb_DATETIME[YY:DD]"));
    }
}
