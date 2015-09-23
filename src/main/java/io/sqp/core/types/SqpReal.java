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
import io.sqp.core.exceptions.SqpException;
import io.sqp.core.util.TypeUtil;

import java.math.BigDecimal;

/**
 * @author Stefan Burnicki
 * @see SqpDouble
 */
public class SqpReal extends SqpAbstractNumber<Float> {

    public SqpReal(float value) throws SqpException {
        super(SqpTypeCode.Real, value);
    }

    @Override
    public float asFloat() throws TypeConversionException {
        return getValue();
    }

    @Override
    public double asDouble() throws TypeConversionException {
        return (double) getValue();
    }

    @Override
    public BigDecimal asBigDecimal() throws TypeConversionException {
        return new BigDecimal(getValue());
    }

    public static SqpReal fromJsonFormatValue(Object value) throws SqpException {
        if (value instanceof String) {
            return new SqpReal(Float.parseFloat((String) value));
        }
        return new SqpReal(TypeUtil.checkAndConvert(value, Float.class, "The Real value"));
    }

}
