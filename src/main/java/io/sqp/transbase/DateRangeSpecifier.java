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

import transbase.tbx.TBConst;

import java.util.Arrays;
import java.util.List;

/**
 * @author Stefan Burnicki
 */
enum DateRangeSpecifier {
    Years(TBConst.TB__YY, "YY", 1, 32767),
    Months(TBConst.TB__MO, "MO", 1, 12),
    Days(TBConst.TB__DD, "DD", 1, 31),
    Hours(TBConst.TB__HH, "HH", 0, 23),
    Minutes(TBConst.TB__MI, "MI", 0, 59),
    Seconds(TBConst.TB__SS, "SS", 0, 59),
    MilliSeconds(TBConst.TB__MS, "MS", 0, 999);

    private short _tbValue;
    private String _specifier;
    private int _minValue;
    private int _maxValue;

    DateRangeSpecifier(short tbValue, String specifier, int minValue, int maxValue) {
        _tbValue = tbValue;
        _specifier = specifier;
        _minValue = minValue;
        _maxValue = maxValue;
    }

    public short getTbValue() {
        return _tbValue;
    }

    public String getSpecifier() {
        return _specifier;
    }

    public int getMinValue() {
        return _minValue;
    }

    public int getMaxValue() {
        return _maxValue;
    }

    public static DateRangeSpecifier fromTbValue(int tbValue) {
        for (DateRangeSpecifier spec : DateRangeSpecifier.values()) {
            if (spec.getTbValue() == tbValue) {
                return spec;
            }
        }
        throw new IllegalArgumentException("Invalid specifier value '" + tbValue + "'");
    }

    public static DateRangeSpecifier fromSpecifier(String specifier) {
        specifier = specifier.toUpperCase();
        for (DateRangeSpecifier spec : DateRangeSpecifier.values()) {
            if (spec.getSpecifier().equals(specifier)) {
                return spec;
            }
        }
        throw new IllegalArgumentException("Invalid specifier value '" + specifier + "'");
    }

    public static List<DateRangeSpecifier> range(DateRangeSpecifier higher, DateRangeSpecifier lower) {
        List<DateRangeSpecifier> values = Arrays.asList(DateRangeSpecifier.values());
        int highField = -1;
        int lowField = -1;
        for (int i = 0; i < values.size(); i++) {
            if (higher.equals(values.get(i))) {
                highField = i;
            }
            if (highField >= 0 && lower.equals(values.get(i))) {
                lowField = i;
                break;
            }
        }
        if (highField < 0 || lowField < 0) {
            throw new IllegalArgumentException("Invalid range from high field " + higher + " to low field " + lower);
        }
        return values.subList(highField, lowField + 1);
    }
}
