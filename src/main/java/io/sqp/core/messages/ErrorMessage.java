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
import io.sqp.core.ErrorType;

/**
 * @author Stefan Burnicki
 */
public class ErrorMessage extends SqpMessage {
    private ErrorType _errorType;
    private String _message;

    @JsonCreator
    public ErrorMessage(
            @JsonProperty("errorType") ErrorType errorType,
            @JsonProperty("message") String message
    ) {
        _errorType = errorType;
        _message = message;
    }

    @JsonProperty("errorType")
    public ErrorType getErrorType() {
        return _errorType;
    }

    @JsonProperty("message")
    public String getMessage() {
        return _message;
    }

}
