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

package io.sqp.core;

import io.sqp.core.exceptions.DecodingException;
import io.sqp.core.messages.MessageType;
import io.sqp.core.messages.SqpMessage;

import java.io.InputStream;
import java.io.Reader;

/**
 * Interface for decoding a SQP message from a stream.
 * @author Stefan Burnicki
 * @see MessageEncoder
 * @see SqpMessage
 * @see DataFormat
 * @see MessageType
 */
public interface MessageDecoder {
    /**
     * Decodes a message like {@link #decode(DataFormat, InputStream)} but the message type is passed explicitly,
     * and the message id must not be part of the input stream.
     * @param type The type of the message
     * @param format The format of the message
     * @param stream The encoded content of the message, without the type id as first byte
     * @return The decoded message
     * @throws DecodingException
     */
    SqpMessage decode(MessageType type, DataFormat format, InputStream stream) throws DecodingException;

    /**
     * Decodes any {@link SqpMessage} in a given format from an input stream.
     * @param format The format of the message
     * @param stream The stream with the encoded message. The message id as the first byte, then the content
     * @return The decoded message
     * @throws DecodingException
     * @see MessageType
     */
    SqpMessage decode(DataFormat format, InputStream stream) throws DecodingException;

    /**
     * Decodes a {@link SqpMessage} in text format.
     * @param message The encoded message, with the first character being the message id
     * @return The decoded message
     * @throws DecodingException
     * @see MessageType
     */
    SqpMessage decode(Reader message) throws DecodingException;
}
