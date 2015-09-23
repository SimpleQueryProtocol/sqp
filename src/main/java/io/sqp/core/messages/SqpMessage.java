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

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.sqp.core.DataFormat;
import io.sqp.core.exceptions.DecodingException;

/**
 * @author Stefan Burnicki
 */
public class SqpMessage {
    private DataFormat _messageFormat = DataFormat.Text;
    private MessageType _type;

    protected SqpMessage() {
        _type = MessageType.valueOf(this.getClass().getSimpleName());
    }

    @JsonIgnore
    public DataFormat getMessageFormat() {
        return _messageFormat;
    }

    @JsonIgnore
    public void setMessageFormat(DataFormat format) {
        _messageFormat = format;
    }

    @JsonIgnore
    public MessageType getType() {
        return _type;
    }

    public boolean isA(MessageType type) {
        return _type.equals(type);
    }

    public <T extends SqpMessage> T secureCast() throws DecodingException {
        // TODO: this method doesn't make sense because of type erasure
        try {
            return (T) _type.getType().cast(this);
        } catch (ClassCastException e) {
            throw new DecodingException("Error processing the message. Couldn't cast to type " + _type.getType(), e);
        }
    }
}
