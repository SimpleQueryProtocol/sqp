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
import io.sqp.core.InformationResponseType;

/**
 * @author Stefan Burnicki
 */
public class InformationResponseMessage extends SqpMessage {
    private Object _value;
    private InformationResponseType _responseType;

    @JsonCreator
    public InformationResponseMessage(
            @JsonProperty("responseType") InformationResponseType responseType,
            @JsonProperty("value") Object value) {
        _value = value;
        _responseType = responseType;
    }

    public InformationResponseType getResponseType() {
        return _responseType;
    }

    public Object getValue() {
        return _value;
    }
}
