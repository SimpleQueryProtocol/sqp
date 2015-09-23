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
import io.sqp.core.MessageEncoder;
import io.sqp.core.messages.SqpMessage;
import io.sqp.core.messages.MessageType;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;

/**
 * Implements {@link MessageEncoder} with the Jackson framework to encode messages with MsgPack or JSON.
 * @author Stefan Burnicki
 */
public class JacksonMessageEncoder implements MessageEncoder {

    @Override
    public void encode(OutputStream stream, DataFormat format, SqpMessage msg) throws IOException {
        MessageType type = msg.getType();
        try {
            stream.write(type.getId());
        } catch (IOException e) {
            stream.close();
            throw e;
        }

        if (!type.hasContent()) {
            stream.close();
            return;
        }

        ObjectMapper mapper = JacksonObjectMapperFactory.objectMapper(format);
        mapper.writeValue(stream, msg);
    }

    public void encode(Writer writer, SqpMessage message) throws IOException {
        MessageType type = message.getType();
        try {
            writer.write(type.getId());
        } catch (IOException e) {
            writer.close();
            throw e;
        }

        if (!type.hasContent()) {
            writer.close();
            return;
        }

        ObjectMapper mapper = JacksonObjectMapperFactory.objectMapper(DataFormat.Text);
        mapper.writeValue(writer, message);
    }

}
