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
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.io.IOException;

/**
 * @author Stefan Burnicki
 */
public class AutoConnectTestBase extends ConnectionTestBase {
    protected SqpConnection connection;

    @BeforeMethod
    public void ConnectToServer() {
        // To test cursors, we set max fetch to 1
        ClientConfig config = ClientConfig.create().setCursorMaxFetch(1).setProtocolFormat(getProtocolFormat());
        connection = SqpConnection.create(config);
        // TODO: remove this try-catch as soon as the problem of sometimes occurring NPEs is resolved
        defaultConnect(connection).join();
    }

    @AfterMethod
    public void CloseConnection() throws IOException {
        if (connection != null) {
            connection.close();
        }
    }

    protected DataFormat getProtocolFormat() {
        return DataFormat.Text;
    }
}
