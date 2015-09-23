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

package io.sqp.core;

import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.testng.FileAssert.fail;

/**
 * @author Stefan Burnicki
 */
public class TryOutTests {
    @Test(expectedExceptions = ArithmeticException.class)
    public void longValueInBigDecimalToIntByNumberConversionThrows() {
        BigDecimal num = new BigDecimal(1234567891234322345l);
        int intValue = num.intValueExact();
        fail("Conversion from " + num + " to int worked, value is " + intValue);
    }

    @Test
    public void decodeLongFromList() {
        long value = 0;
        for (int i : new int[]{1, 23, 22, 6, 42, 14, 0, 0, 1, 0}) {
            value *= 100;
            value += i;
        }
        assertThat(value, is(1232206421400000100l));
    }

    @Test
    public void encodeLongAsList() {
        long value = 1232206421400000100l;
        ArrayList<Integer> arrayList = new ArrayList<>();
        while (value > 0) {
            arrayList.add((int) (value % 100));
            value /= 100;
        }
        Collections.reverse(arrayList);
        assertThat(arrayList, contains(1, 23, 22, 6, 42, 14, 0, 0, 1, 0));
    }
}
