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

import java.time.LocalDate;
import java.util.List;

/**
 * The "Date" SQP type, a calendar date without time.
 * The date is represented by three values: The year, the month and the day.
 * The year might be negative for dates BC.
 * @author Stefan Burnicki
 */
public class SqpDate extends SqpValue {
    private int _year;
    private int _month;
    private int _day;

    /**
     * Constructs the object from a JSON-compatible deserialized value. This should be a triple with the values year,
     * month, and day. All being convertible to integers.
     * @param jsonFormatValue The triple [year, month, day]
     * @return The created object
     */
    public static SqpDate fromJsonFormatValue(Object jsonFormatValue) {
        List date = TypeUtil.checkAndConvert(jsonFormatValue, List.class, "The JSON format value");
        if (date.size() < 3) {
            throw new IllegalArgumentException("The date list doesn't contain 3 elements.");
        }
        int year = TypeUtil.checkAndConvert(date.get(0), Integer.class, "The year field");
        int month = TypeUtil.checkAndConvert(date.get(1), Integer.class, "The month field");
        int day = TypeUtil.checkAndConvert(date.get(2), Integer.class, "The day field");
        return new SqpDate(year, month, day);
    }

    /**
     * Contrcuts the object from a LocalDate
     * @param date The date
     */
    public SqpDate(LocalDate date) {
        this(date.getYear(), date.getMonthValue(), date.getDayOfMonth());
    }

    /**
     * Constructs the object from the three values.
     * @param year The year, can be negative, but must be != 0
     * @param month The month, value needs to be >= 1 and <= 12
     * @param day The day, value needs to be >= 1 and <= 31
     */
    public SqpDate(int year, int month, int day) {
        super(SqpTypeCode.Date);
        // TODO: validate the field ranges?
        _year = year; // check != 0
        _month = month; // check >= 1 && <= 12
        _day = day; // check >= 1 && <= 31
    }

    /**
     * Returns the year value
     * @return The year, which might be negative
     */
    public int getYear() {
        return _year;
    }

    /**
     * Returns the month value
     * @return The month, starting by 1 and ending with 12
     */
    public int getMonth() {
        return _month;
    }

    /**
     * Returns the day value
     * @return The day, starting by 1 and ending with 31
     */
    public int getDay() {
        return _day;
    }

    /**
     * {@inheritDoc}
     * <p>
     * In this case, the value is an integer array with the three values: year, month, day
     * @return The triple [year, month, day]
     */
    @Override
    public int[] getJsonFormatValue() {
        return new int[] {_year, _month, _day};
    }

    /**
     * Constructs and returns a LocalDate from the internal values.
     * @return The LocalDate corresponding to this
     * @throws TypeConversionException If the date is not valid
     */
    @Override
    public LocalDate asLocalDate() throws TypeConversionException {
        return LocalDate.of(_year, _month, _day);
    }
}
