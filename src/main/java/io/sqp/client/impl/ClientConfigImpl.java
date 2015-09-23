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

package io.sqp.client.impl;

import io.sqp.core.DataFormat;
import io.sqp.client.ClientConfig;

/**
 * @author Stefan Burnicki
 */
public class ClientConfigImpl implements ClientConfig {
    private int _maxFetch = -1;
    private int _lobBufferSize = 1024_00;
    private DataFormat _dataFormat = DataFormat.Binary;

    @Override
    public ClientConfig setProtocolFormat(DataFormat dataFormat) {
        _dataFormat = dataFormat;
        return this;
    }

    @Override
    public DataFormat getProtocolFormat() {
        return _dataFormat;
    }

    @Override
    public ClientConfig setCursorMaxFetch(int maxFetch) {
        _maxFetch = maxFetch;
        return this;
    }

    @Override
    public int getCursorMaxFetch() {
        return _maxFetch;
    }

    @Override
    public ClientConfig setLobBufferSize(int size) {
        _lobBufferSize = size;
        return this;
    }

    @Override
    public int getLobBufferSize() {
        return _lobBufferSize;
    }
}
