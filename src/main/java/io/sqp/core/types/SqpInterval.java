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

import java.time.Duration;

/**
 * @author Stefan Burnicki
 */
// TODO: more support for this
public class SqpInterval extends SqpValue {
    public enum Sign {
        Positive,
        Negative
    }

    private int _years;
    private int _months;
    private int _days;
    private int _hours;
    private int _minutes;
    private int _seconds;
    private int _nanos;
    private Sign _sign;

    public SqpInterval(Sign sign, int years, int months, int days, int hours, int minutes, int seconds, int nanos) {
        super(SqpTypeCode.Interval);
        _years = years;
        _months = months;
        _days = days;
        _hours = hours;
        _minutes = minutes;
        _seconds = seconds;
        _nanos = nanos;
        _sign = sign;
    }

    public int getYears() {
        return _years;
    }

    public int getMonths() {
        return _months;
    }

    public int getDays() {
        return _days;
    }

    public int getHours() {
        return _hours;
    }

    public int getMinutes() {
        return _minutes;
    }

    public int getSeconds() {
        return _seconds;
    }

    public int getNanos() {
        return _nanos;
    }

    public Sign getSign() {
        return _sign;
    }

    @Override
    public Duration asDuration() throws TypeConversionException {
        throw new UnsupportedOperationException("Not yet possible");
    }

    @Override
    public Object getJsonFormatValue() {
        boolean sign = _sign == Sign.Positive;
        return new Object[]{sign, new Object[]{_years, _months, _days}, new Object[]{_hours, _minutes, _seconds, _nanos}};
    }

    public static SqpInterval fromJsonFormatValue(Object value) {
        throw new UnsupportedOperationException("Not yet possible");
    }
}
