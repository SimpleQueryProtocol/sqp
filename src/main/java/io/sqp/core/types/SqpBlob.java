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

package io.sqp.core.types;

import io.sqp.core.util.TypeUtil;

import java.util.List;

/**
 * The "Blob" SQP type, a Binary Large OBject.
 * In SQP, LOBs (large objects) are sent and received separately. This object only represents the metadata of a binary
 * large object on the wire, namely it's ID and it's size. An InputStream can get associated after creation, for
 * convenient use on the server/client side, but it won't be part of the serialization/deserialization
 * @author Stefan Burnicki
 * @see SqpClob
 */
public class SqpBlob extends SqpAbstractLob {
    /**
     * Constructs an object with the given id and size
     * @param id Used to identify this BLOB on the server
     * @param size The total size of the BLOB in bytes, if known. Otherwise -1
     */
    public SqpBlob(String id, long size) {
        super(SqpTypeCode.Blob, id, size);
    }

    /**
     * Constructs the object from a tuple of string and optional number.
     * This method is used with a JSON-like object structure that was created upon deserialization.
     * @param jsonFormatValue A tuple-like object with an id (as String) and optionally the BLOB's size (as a number)
     * @return The created object
     * @throws IllegalArgumentException If the jsonFormatValue was not a tuple-like object as described.
     */
    public static SqpBlob fromJsonFormatValue(Object jsonFormatValue) throws IllegalArgumentException {
        List list = TypeUtil.checkAndConvert(jsonFormatValue, List.class, "The JSON format value");
        int listSize = list.size();
        if (listSize < 1) {
            throw new IllegalArgumentException("The BLOB tuple doesn't contain lements.");
        }
        String id = TypeUtil.checkAndConvert(list.get(0), String.class, "The id");
        long size = listSize > 1 ? TypeUtil.checkAndConvert(list.get(1), Long.class, "The BLOB size") : -1;
        return new SqpBlob(id, size);
    }

    @Override
    protected SqpAbstractLob copy() {
        return new SqpBlob(getId(), getSize());
    }

}
