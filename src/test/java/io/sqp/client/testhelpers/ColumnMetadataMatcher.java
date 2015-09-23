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

package io.sqp.client.testhelpers;

import io.sqp.core.types.SqpTypeCode;
import org.mockito.ArgumentMatcher;
import io.sqp.core.ColumnMetadata;

/**
 * @author Stefan Burnicki
 */
public class ColumnMetadataMatcher extends ArgumentMatcher<ColumnMetadata> {
    private final SqpTypeCode _type;
    private final String _name;

    private ColumnMetadataMatcher(SqpTypeCode type, String name) {
        _type = type;
        _name = name;
    }


    @Override
    public boolean matches(Object argument) {
        if (!(argument instanceof ColumnMetadata)) {
            return false;
        }
        ColumnMetadata metadata = (ColumnMetadata) argument;
        return metadata.getType().getTypeCode().equals(_type) && metadata.getName().equals(_name);
    }

    public static ColumnMetadataMatcher columnMetadataWith(SqpTypeCode type, String name) {
        return new ColumnMetadataMatcher(type, name);
    }

    @Override
    public String toString() {
        return "ColumnMetadataMatcher{" +
                "_type=" + _type +
                ", _name='" + _name + '\'' +
                '}';
    }
}
