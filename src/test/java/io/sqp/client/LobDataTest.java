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

import org.hamcrest.MatcherAssert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import io.sqp.core.exceptions.SqpException;
import io.sqp.proxy.util.ResourceUtil;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Random;
import java.util.logging.Logger;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Stefan Burnicki
 */
public class LobDataTest extends AutoConnectTestBase {
    private static Random _random = new Random();

    @BeforeMethod
    public void ClearMediaTable() {
        connection.execute("DELETE FROM media").join();
    }

    @Test(groups = "native-transbase-only") // TODO: add support for native-postgres
    public void CanSendBlob() throws SqpException {
        byte[] data = new byte[50 * 1024 * 1024]; // 50mb
        _random.nextBytes(data);

        OffsetDateTime now = OffsetDateTime.now();
        UpdateResult result = connection.prepare("INSERT INTO media (name, added, content) VALUES(?, ?, ?)")
                .bind(0, "Random Bytes").bind(1, now).bind(2, new ByteArrayInputStream(data)).executeUpdate().join();
        assertThat(result.getAffectedRows(), is(1));
    }

    @Test(groups = "native-transbase-only") // TODO: add support for native-postgres
    public void CanSendClob() throws Exception {
        String utf8text = ResourceUtil.readResourceAsString("UTF-8-demo.txt", Logger.getGlobal());
        String clob = new String(new char[10]).replace("\0", utf8text); // make it bigger

        OffsetDateTime now = OffsetDateTime.now();
        UpdateResult result = connection.prepare("INSERT INTO media (name, added, transcription) VALUES(?, ?, ?)")
                .bind(0, "Random Bytes").bind(1, now).bind(2, new StringReader(clob)).executeUpdate().join();
        assertThat(result.getAffectedRows(), is(1));
    }

    @Test(groups = "native-transbase-only") // TODO: add support for native-postgres
    public void CanFetchBlob() throws SqpException {
        byte[] data = new byte[50 * 1024 * 1024]; // 50mb
        _random.nextBytes(data);

        OffsetDateTime now = OffsetDateTime.now();
        connection.prepare("INSERT INTO media (name, added, content) VALUES(?, ?, ?)")
                .bind(0, "Random Bytes").bind(1, now).bind(2, new ByteArrayInputStream(data)).executeUpdate();

        Cursor cursor = connection.executeSelect("SELECT * FROM media").join();
        assertThat(cursor.nextRow(), is(true));
        MatcherAssert.assertThat(cursor.at("name").asString(), is("Random Bytes"));
        assertThat(toUtcDateTime(cursor.at("added").asOffsetDateTime()), is(toUtcDateTime(now)));
        byte[] receivedBytes = cursor.at("content").asBytes();
        assertThat(receivedBytes, is(data));
    }

    // TODO: implemen this @Test
    public void CanFetchClob() {

    }

    private LocalDateTime toUtcDateTime(OffsetDateTime offsetDateTime) {
        return offsetDateTime.toLocalDateTime().minusSeconds(offsetDateTime.getOffset().getTotalSeconds());
    }

}
