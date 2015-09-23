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

package io.sqp.proxy;

/**
 * @author Stefan Burnicki
 */
public interface ByteBuffer {
    byte getByte(int i);

    default ByteBuffer getBuffer(int start) {
        return getBuffer(start, length());
    }

    ByteBuffer getBuffer(int start, int end);

    int length();

    ByteBuffer append(ByteBuffer buffer);

    ByteBuffer append(String string, String encoding);

    default ByteBuffer append(String string) {
        return append(string, "UTF-8");
    }

    byte[] getBytes();

    ByteBuffer append(byte b);

    String toString(String encoding);
}
