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
import io.sqp.core.util.TypeUtil;

/**
 * @author Stefan Burnicki
 */
public class SqpSmallInt extends SqpAbstractNumber<Short> {

    public SqpSmallInt(short value) {
        super(SqpTypeCode.SmallInt, value);
    }

    @Override
    public short asShort() throws TypeConversionException {
        return getValue();
    }

    @Override
    public int asInt() throws TypeConversionException {
        return getValue();
    }

    @Override
    public long asLong() throws TypeConversionException {
        return getValue();
    }

    public static SqpSmallInt fromJsonFormatValue(Object value) throws IllegalArgumentException {
        return new SqpSmallInt(TypeUtil.checkAndConvert(value, Short.class, "The SmallInt value"));
    }
}
