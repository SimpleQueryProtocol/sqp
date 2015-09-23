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

import io.sqp.core.Defaults;
import io.sqp.core.types.SqpTypeCode;

import java.util.Collections;
import java.util.List;

/**
 * @author Stefan Burnicki
 */
public class ExecuteQueryMessage extends SqpMessage {
    private String _statementId = Defaults.DefaultStatementID;
    private boolean _scrollable = false;
    private List<SqpTypeCode> _parameterTypes = Collections.emptyList();
    private List<List<Object>> _parameters = Collections.emptyList();
    private List<String> _customTypes = Collections.emptyList();
    private String _cursorId = Defaults.DefaultCursorID;

    // necessary for decoding
    public ExecuteQueryMessage() {}

    public ExecuteQueryMessage(String statementId, String cursorId, List<SqpTypeCode> parameterTypes,
                               List<String> customTypes, List<List<Object>> parameters, boolean scrollable) {
        _statementId = statementId;
        _scrollable = scrollable;
        _parameterTypes = parameterTypes;
        _parameters = parameters;
        _cursorId = cursorId;
        _customTypes = customTypes;
    }

    public void setStatementId(String statementId) {
        _statementId = statementId;
    }

    public void setScrollable(boolean scrollable) {
        _scrollable = scrollable;
    }

    public void setParameterTypes(List<SqpTypeCode> parameterTypes) {
        _parameterTypes = parameterTypes;
    }

    public void setParameters(List<List<Object>> parameters) {
        _parameters = parameters;
    }

    public void setCursorId(String cursorId) {
        _cursorId = cursorId;
    }

    public void setCustomTypes(List<String> customTypes) {
        _customTypes = customTypes;
    }

    public String getStatementId() {
        return _statementId;
    }

    public boolean isScrollable() {
        return _scrollable;
    }

    public String getCursorId() {
        return _cursorId;
    }

    public List<SqpTypeCode> getParameterTypes() {
        return _parameterTypes;
    }

    public List<List<Object>> getParameters() {
        return _parameters;
    }

    public List<String> getCustomTypes() {
        return _customTypes;
    }
}
