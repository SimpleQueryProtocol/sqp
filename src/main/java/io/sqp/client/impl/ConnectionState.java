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

/**
 * @author Stefan Burnicki
 */
public enum ConnectionState {
    Uninitialized("Still unitialized"),
    Connecting("Not yet connected"),
    ReadyToSend("Ready to send"),
    Closed("Connection was closed"),
    Error("A error occured and the server didn't recover (yet)");

    private String _description;

    ConnectionState(String description) {
        _description = description;
    }

    public String getDescription() {
        return _description;
    }
}
