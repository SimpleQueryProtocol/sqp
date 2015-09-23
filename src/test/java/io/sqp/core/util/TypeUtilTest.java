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

package io.sqp.core.util;

import org.testng.annotations.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Stefan Burnicki
 */
public class TypeUtilTest {

    @Test
    public void numberConversionCanAutobox() {
        int num = 5;
        Short value = TypeUtil.checkAndConvert(num, Short.class, "");
        assertThat(value.intValue(), is(num));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void numberConversionDoesRangeCheck() {
        TypeUtil.checkAndConvert(Integer.MAX_VALUE, Short.class, "");
    }
}
