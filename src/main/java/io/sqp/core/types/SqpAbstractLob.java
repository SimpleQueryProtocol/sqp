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

import io.sqp.core.exceptions.TypeConversionException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Abstract base class of large object types.
 * It can have an associated input stream, but this will not be part of the JSON format value.
 * It's just a convenient way to associate data to this type when it's processed on client or server.
 *
 * @author Stefan Burnicki
 * @see SqpBlob
 * @see SqpClob
 */
public abstract class SqpAbstractLob extends SqpValue {
    private String _id;
    private long _size;
    private InputStream _inputStream;

    /**
     * Constructor for use by derived classes.
     * @param type Type code of the actual type
     * @param id An identifier to uniquely reference a CLOB/BLOB on the server
     * @param size The size of the large object in bytes (-1 if unknown)
     */
    protected SqpAbstractLob(SqpTypeCode type, String id, long size) {
        super(type);
        _id = id;
        _size = size;
    }

    /**
     * Gets the identifier for this large object.
     * @return Identifier
     */
    public String getId() {
        return _id;
    }

    /**
     * Gets the total size of the represented object, if known.
     * @return Size in bytes, -1 if unknown
     */
    public long getSize() {
        return _size;
    }

    /**
     * Returns the content, if there is one associated with this object, otherwise null.
     * @return The associated content or null
     */
    public InputStream getInputStream() {
        return _inputStream;
    }

    /**
     * Returns the complete content read from the associated input stream.
     * This works only, if an input stream is associated with this object, and the object has a known size <= 2GB.
     * Then the complete stream is read into a byte array and returned.
     *
     * @return The associated content of this object.
     * @throws TypeConversionException If input stream reading failed, or the size is unknown or > 2GB
     */
    @Override
    public byte[] asBytes() throws TypeConversionException {
        // this function sucks a little as it has to copy the array if we got less bytes than expected
        // otherwise the user wouldn't now the actual size
        if (_size < 0 || _size > Integer.MAX_VALUE) {
            throw new TypeConversionException("Can only convert LOBs with known size < 2^32 to byte[]");
        }
        if (_inputStream == null) {
            throw new TypeConversionException("No stream associated with this LOB");
        }
        try {
            return inputStreamAsByteArray(_inputStream);
        } catch (IOException e) {
            throw new TypeConversionException("Failed reading the LOB's stream: " + e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * This does not include the input stream, only id and size as a tuple.
     * @return A tuple with id and size.
     */
    @Override
    public Object getJsonFormatValue() {
        return new Object[] {_id, _size};
    }

    /**
     * Copies the object and associates an InputStream with it.
     * @param stream The stream to associated with the new object
     * @return A copy of this object with the associated input stream
     */
    public SqpAbstractLob createWithStream(InputStream stream) {
        SqpAbstractLob copy = copy();
        copy._inputStream = stream;
        return copy;
    }

    /**
     * Needs to be overridden in order to copy the object.
     * @return A copy of this object.
     */
    protected abstract SqpAbstractLob copy();

    private byte[] inputStreamAsByteArray(InputStream stream) throws IOException {
        int read;
        byte[] buffer = new byte[1024_00];
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        while ((read = stream.read(buffer, 0, buffer.length)) != -1) {
            output.write(buffer, 0, read);
        }
        output.flush();
        return output.toByteArray();
    }
}
