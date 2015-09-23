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

import io.sqp.core.exceptions.InvalidOperationException;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

/**
 * @author Stefan Burnicki
 */
public abstract class CloseableServerResource {
    private String _id;
    private boolean _closed;
    private SqpConnectionImpl _connection;
    private String _resourceType;

    public CloseableServerResource(SqpConnectionImpl connection, String id, String resourceType) {
        _connection = connection;
        _id = id;
        _resourceType = resourceType;
        _connection.registerOpenServerResource(this);
    }

    public void close() throws IOException {
        // TODO: provide an async variant of this
        if (_closed) {
            return;
        }
        _closed = true;
        try {
            _connection.closeServerResources(Arrays.asList(this)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException("Failed to close the cursor", e);
        }
    }

    protected void validateOpen() throws InvalidOperationException {
        if (_closed) {
            throw new InvalidOperationException("The " + _resourceType + " with id '" + _id + "' is closed.");
        }
    }

    protected SqpConnectionImpl getConnection() {
        return _connection;
    }

    public String getId() {
        return _id;
    }

    public boolean isClosed() {
        return _closed;
    }

    // Does not close the resource, just sets it to closed, if closed externally
    public void setClosed() {
        _closed = true;
    }

}
