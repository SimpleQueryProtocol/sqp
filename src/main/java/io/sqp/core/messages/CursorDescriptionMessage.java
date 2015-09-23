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

package io.sqp.core.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.sqp.core.ColumnMetadata;

import java.util.List;

/**
 * @author Stefan Burnicki
 */
public class CursorDescriptionMessage extends SqpMessage {
    final private String _cursorId;
    final private List<ColumnMetadata> _columns;
    final private boolean _scrollable;

    @JsonCreator
    public CursorDescriptionMessage(
            @JsonProperty("cursorId") String cursorId,
            @JsonProperty("scrollable") boolean scrollable,
            @JsonProperty("columns") List<ColumnMetadata> columns
    ) {
        _scrollable = scrollable;
        _cursorId = cursorId;
        _columns = columns;
    }

    public boolean isScrollable() {
        return _scrollable;
    }

    public String getCursorId() {
        return _cursorId;
    }

    public List<ColumnMetadata> getColumns() {
        return _columns;
    }
}
