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

/**
 * Abstract base class for text-based types.
 * @author Stefan Burnicki
 * @see SqpChar
 * @see SqpVarChar
 * @see SqpXml
 */
public abstract class SqpAbstractText extends SqpValue {
    String _value;

    /**
     * Constructor to be used by derived classes.
     * @param type The type code of the actual type
     * @param value The associated text
     */
    protected SqpAbstractText(SqpTypeCode type, String value) {
        super(type);
        _value = value;
    }

    /**
     * Returns the associated text, as a text is JSON compatible in general
     * @return The associated text
     */
    @Override
    public String getJsonFormatValue() {
        return _value;
    }

    /**
     * Simply returns the associated text.
     * @return The associated text
     */
    @Override
    public String asString() {
        return _value;
    }

}
