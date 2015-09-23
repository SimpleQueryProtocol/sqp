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

import io.sqp.core.messages.MessageType;
import io.sqp.core.messages.SqpMessage;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;

/**
 * Interface for encoding a SQP message to a stream.
 * @author Stefan Burnicki
 * @see MessageDecoder
 * @see SqpMessage
 * @see DataFormat
 * @see MessageType
 */
public interface MessageEncoder {
    /**
     * Encodes a message to a stream in the given format. The first byte will be the message's identifier,
     * then the content of the message will follow in either MsgPack or JSON format.
     * @param stream The output stream the encoded message gets written to
     * @param format The format of the encoded message - text means JSON, binary means MsgPack
     * @param message The message to encode
     * @throws IOException
     * @see MessageType
     */
    void encode(OutputStream stream, DataFormat format, SqpMessage message) throws IOException;

    /**
     * Encodes a message in JSON text format. The first character will be the message's identifier,
     * then the content of the message will follow.
     * @param writer Where the encoded message is written to
     * @param message The message to encode
     * @throws IOException
     * @see MessageType
     */
    void encode(Writer writer, SqpMessage message) throws IOException;
}
