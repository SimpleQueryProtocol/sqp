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

package io.sqp.proxy.customtypes;

import io.sqp.proxy.exceptions.ValidationFailedException;

/**
 * @author Stefan Burnicki
 */
public class CustomType {
    private String _name;
    private TypeValidator _validator;

    public CustomType(String name, TypeValidator validator) {
        _name = name;
        _validator = validator;
    }

    public String getName() {
        return _name;
    }

    public void validate(Object value) throws ValidationFailedException {
        _validator.validate(value);
    }
}
