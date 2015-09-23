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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.sqp.core.DataFormat;
import org.msgpack.jackson.dataformat.MessagePackFactory;

/**
 * Provides the ObjectMapper used by {@link JacksonMessageDecoder} and {@link JacksonMessageEncoder}
 * to decode/encode a message from/to a specific format.
 * @see JacksonMessageDecoder
 * @see JacksonMessageEncoder
 */
public final class JacksonObjectMapperFactory {
    static private JsonFactory _jsonFactory = new JsonFactory();
    static private ObjectMapper _jsonObjectMapper = new ObjectMapper(_jsonFactory);

    static private MessagePackFactory _msgpackFactory = new MessagePackFactory();
    static private ObjectMapper _msgpackObjectMapper = new ObjectMapper(_msgpackFactory);

    static {
        _msgpackObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        _jsonObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    private JacksonObjectMapperFactory() {}

    /**
     * Returns a Jackson ObjectMapper for the specified data format.
     * If the data format is {@link DataFormat#Binary}, then the ObjectMapper will use MsgPack, if the
     * format is {@link DataFormat#Text} the ObjectMapper will use JSON.
     * @param format The format the ObjectWrapper should support
     * @return The ObjectMapper for either JSON or MsgPack
     */
    public static ObjectMapper objectMapper(DataFormat format) {
        if (format == DataFormat.Binary) {
            return _msgpackObjectMapper;
        } else {
            return _jsonObjectMapper;
        }
    }
}
