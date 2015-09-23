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

package io.sqp.core.jackson;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.sqp.core.DataFormat;
import io.sqp.core.messages.SqpMessage;
import io.sqp.core.MessageDecoder;
import io.sqp.core.exceptions.DecodingException;
import io.sqp.core.messages.MessageType;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

/**
 * Implements {@link MessageDecoder} with the Jackson framework to decode MsgPack or JSON messages.
 * @author Stefan Burnicki
 */
public class JacksonMessageDecoder implements MessageDecoder {

    @Override
    public SqpMessage decode(MessageType type, DataFormat format, InputStream stream) throws DecodingException {
        return decode(type, format, (mapper, msgClass) -> mapper.readValue(stream, msgClass));
    }

    @Override
    public SqpMessage decode(DataFormat format, InputStream stream) throws DecodingException {
        // TODO: check if we need to close the InputStream
        // pass a "ValueReader" since we don't have dynamic type dispatch
        try {
            MessageType type = MessageType.fromId((char) stream.read());
            return decode(type, format, stream);
        } catch (IOException e) {
            throw new DecodingException("Failed to read the message identifier: " + e.getMessage(), e);
        }
    }

    @Override
    public SqpMessage decode(Reader message) throws DecodingException {
        // TODO: check if we need to close the Reader
        // pass a "ValueReader" since we don't have dynamic type dispatch
        try {
            MessageType type = MessageType.fromId((char) message.read());
            return decode(type, DataFormat.Text, (mapper, msgClass) -> mapper.readValue(message, msgClass));
        } catch (IOException e) {
            throw new DecodingException("Failed to read the message identifier: " + e.getMessage(), e);
        }
    }

    private SqpMessage decode(MessageType type, DataFormat format, ValueReader reader) throws DecodingException {
        Class<? extends SqpMessage> msgType = type.getType();

        if (!type.hasContent()) {
            try {
                return msgType.newInstance();
            } catch (ReflectiveOperationException e) {
                throw new DecodingException("Couldn't create a message of type " + msgType, e);
            }
        }

        ObjectMapper mapper = JacksonObjectMapperFactory.objectMapper(format);
        try {
            return reader.readValue(mapper, msgType);
        } catch (IOException e) {
            throw new DecodingException("Error to decoding message of type " + msgType + ": " + e.getMessage(), e);
        }
    }

    @FunctionalInterface
    interface ValueReader {
        SqpMessage readValue(ObjectMapper mapper, Class<? extends SqpMessage> msgClass) throws IOException;
    }
}
