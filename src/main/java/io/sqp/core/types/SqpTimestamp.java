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

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.List;

/**
 * @author Stefan Burnicki
 */
public class SqpTimestamp extends SqpValue {
    private SqpDate _date;
    private SqpTime _time;

    public static SqpTimestamp fromJsonFormatValue(Object value) throws SqpException {
        List list = TypeUtil.checkAndConvert(value, List.class, "The JSON format value");
        if (list.size() < 2) {
            throw new IllegalArgumentException("The date/time list doesn't contain two elements");
        }
        SqpDate date = SqpDate.fromJsonFormatValue(list.get(0));
        SqpTime time = SqpTime.fromJsonFormatValue(list.get(1));
        return new SqpTimestamp(date, time);
    }

    public SqpTimestamp(OffsetDateTime offsetDateTime) {
        this(new SqpDate(offsetDateTime.toLocalDate()), new SqpTime(offsetDateTime.toOffsetTime()));
    }

    public SqpTimestamp(int year, int month, int day, int hour, int minute, int second, int nano) {
        this(year, month, day, hour, minute, second, nano, null);
    }

    public SqpTimestamp(int year, int month, int day, int hour, int minute, int second, int nano, Integer offset) {
        this(new SqpDate(year, month, day),  new SqpTime(hour, minute, second, nano, offset));
    }

    public SqpTimestamp(SqpDate date, SqpTime time) {
        super(SqpTypeCode.Timestamp);
        _date = date;
        _time = time;
    }

    public SqpDate getDate() {
        return _date;
    }

    public SqpTime getTime() {
        return _time;
    }

    @Override
    public Object[] getJsonFormatValue() {
        return new Object[] {_date.getJsonFormatValue(), _time.getJsonFormatValue()};
    }

    @Override
    public LocalDate asLocalDate() throws TypeConversionException {
        return _date.asLocalDate();
    }

    @Override
    public OffsetTime asOffsetTime() throws TypeConversionException {
        return _time.asOffsetTime();
    }

    @Override
    public OffsetDateTime asOffsetDateTime() throws TypeConversionException {
        return _date.asLocalDate().atTime(_time.asOffsetTime());
    }
}
