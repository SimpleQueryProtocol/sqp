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

import io.sqp.core.ColumnMetadata;
import io.sqp.core.exceptions.SqpException;
import io.sqp.core.types.SqpValue;

import java.io.Closeable;
import java.util.List;

/**
 * @author Stefan Burnicki
 */
public interface Cursor extends QueryResult, Closeable {
    boolean isClosed();
    // TODO: provide an async variant of close

    // TODO: provide an async version of this
    boolean nextRow() throws SqpException;
    boolean previousRow() throws SqpException;

    SqpValue at(int i) throws SqpException;
    SqpValue at(String name) throws SqpException;

    List<ColumnMetadata> getColumnMetadata();
}
