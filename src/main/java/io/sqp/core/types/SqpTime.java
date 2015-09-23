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
import io.sqp.core.exceptions.SqpException;

import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Stefan Burnicki
 */
public class SqpTime extends SqpValue {
    private int _hour;
    private int _minute;
    private int _second;
    private int _nano;

    private Integer _offsetSeconds;

    public static SqpTime fromJsonFormatValue(Object value) throws SqpException {
        // TODO: regard precision
        List list = TypeUtil.checkAndConvert(value, List.class, "The JSON format value");
        if (list.size() < 1) {
            throw new IllegalArgumentException("The list doesn't contain elements.");
        }
        List time = TypeUtil.checkAndConvert(list.get(0), List.class, "The time list");
        if (time.size() < 4) {
            throw new IllegalArgumentException("The time list doesn't contain 4 elements.");
        }
        int hour = TypeUtil.checkAndConvert(time.get(0), Integer.class, "The hour field");
        int minute = TypeUtil.checkAndConvert(time.get(1), Integer.class, "The minute field");
        int second = TypeUtil.checkAndConvert(time.get(2), Integer.class, "The seconds field");
        int nano = TypeUtil.checkAndConvert(time.get(3), Integer.class, "The nanoseconds field");
        if (list.size() < 2) {
            return new SqpTime(hour, minute, second, nano);
        }
        Integer offsetSeconds = TypeUtil.checkAndConvert(list.get(1), Integer.class, "The offset");
        return new SqpTime(hour, minute, second, nano, offsetSeconds);
    }

    public SqpTime(OffsetTime offsetTime) {
        this(offsetTime.toLocalTime(), offsetTime.getOffset().getTotalSeconds());
    }

    public SqpTime(LocalTime localTime) {
        this(localTime, null);
    }

    public SqpTime(LocalTime localTime, Integer offsetSeconds) {
        this(localTime.getHour(), localTime.getMinute(), localTime.getSecond(), localTime.getNano(), offsetSeconds);
    }

    public SqpTime(int hour, int minute, int second, int nano) {
        this(hour, minute, second, nano, null);
    }

    public SqpTime(int hour, int minute, int second, int nano, Integer offsetSeconds) {
        super(SqpTypeCode.Time);
        _hour = hour;
        _minute = minute;
        _second = second;
        _nano = nano;
        _offsetSeconds = offsetSeconds;
    }

    public boolean hasOffset() {
        return _offsetSeconds != null;
    }

    public int getHour() {
        return _hour;
    }

    public int getMinute() {
        return _minute;
    }

    public int getSecond() {
        return _second;
    }

    public int getNano() {
        return _nano;
    }

    public int getOffsetSeconds() {
        return _offsetSeconds == null ? 0 : _offsetSeconds;
    }

    @Override
    public List<Object> getJsonFormatValue() {
        ArrayList<Object> timeList = new ArrayList<>(2);
        timeList.add(new int[]{_hour, _minute, _second, _nano});
        if (hasOffset()) {
            timeList.add(_offsetSeconds);
        }
        return timeList;
    }

    @Override
    public OffsetTime asOffsetTime() throws TypeConversionException {
        return LocalTime.of(_hour, _minute, _second, _nano).atOffset(ZoneOffset.ofTotalSeconds(getOffsetSeconds()));
    }
}
