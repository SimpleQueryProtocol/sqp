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

package io.sqp.transbase;

import transbase.tbx.types.helpers.BlobStream;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author Stefan Burnicki
 */
public class LobManager {
    private static final String CLOB_ID_PREFIX = "CLOB";
    private static final String BLOB_ID_PREFIX = "BLOB";
    private long _lobCounter;
    private Map<String, BlobStream> _lobStreams;
    private Map<Cursor, List<String>> _cursorLobMapping;

    public LobManager() {
        _lobCounter = 0;
        _lobStreams = new HashMap<>();
        _cursorLobMapping = new HashMap<>();
    }

    public String registerLob(BlobStream blobStream, Cursor cursor, boolean isClob) {
        String id = createCursorId(isClob);
        _lobStreams.put(id, blobStream);
        List<String> cursorMapping = _cursorLobMapping.get(cursor);
        if (cursorMapping == null) {
            cursorMapping = new LinkedList<>();
            _cursorLobMapping.put(cursor, cursorMapping);
        }
        cursorMapping.add(id);
        return id;
    }

    public BlobStream getBlobStream(String id) {
        return _lobStreams.get(id);
    }

    public void closeAll(Cursor cursor) throws IOException {
        List<String> cursorMapping = _cursorLobMapping.remove(cursor);
        if (cursorMapping == null) {
            return;
        }
        IOException error = null;
        for (String id : cursorMapping) {
            try {
                closeStream(id);
            } catch (IOException e) {
                error = error == null ? e : new IOException("Failed to close multiple LOBs: " + e.getMessage() +
                                                            "\n and \n" + error.getMessage(), e);
            }
        }
        if (error != null) {
            throw error;
        }
    }

    private void closeStream(String id) throws IOException {
        BlobStream stream = _lobStreams.remove(id);
        if (stream == null) {
            return;
        }
        stream.close();
    }

    private String createCursorId(boolean isClob) {
        _lobCounter++;
        return (isClob ? CLOB_ID_PREFIX : BLOB_ID_PREFIX) + _lobCounter;
    }
}
