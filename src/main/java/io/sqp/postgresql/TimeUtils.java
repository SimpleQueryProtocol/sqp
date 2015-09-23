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

package io.sqp.postgresql;

import io.sqp.core.exceptions.BackendErrorException;
import io.sqp.core.exceptions.TypeConversionException;
import io.sqp.core.exceptions.SqpException;
import io.sqp.core.types.SqpDate;
import io.sqp.core.types.SqpTime;
import io.sqp.core.types.SqpTimestamp;

import java.sql.SQLException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * @author Stefan Burnicki
 */
public class TimeUtils {
    private static final String TIMESTAMP_UTIL_TYPE_DATE = "date";
    private static final String TIMESTAMP_UTIL_TYPE_TIME = "time";
    private static final String TIMESTAMP_UTIL_TYPE_TIMESTAMP = "timestamp";

    private PGTimestampUtils _pgTimestampUtils;
    private Calendar _utcCalendar;

    public TimeUtils(PGConnection connection) throws BackendErrorException {
        _utcCalendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        boolean minVer82 = connection.haveMinimumServerVersion("8.2");
        _pgTimestampUtils = new PGTimestampUtils(minVer82);
    }

    public SqpTimestamp parseSqpTimestamp(String value) throws SqpException {
        PGTimestampUtils.ParsedTimestamp parsed = parseDateTime(value, TIMESTAMP_UTIL_TYPE_TIMESTAMP);
        return new SqpTimestamp(parsed.year, parsed.month, parsed.day,
                                parsed.hour, parsed.minute, parsed.second, parsed.nanos, getTZOffset(parsed));
    }

    public SqpDate parseSqpDate(String value) throws SqpException {
        PGTimestampUtils.ParsedTimestamp parsed = parseDateTime(value, TIMESTAMP_UTIL_TYPE_DATE);
        return new SqpDate(parsed.year, parsed.month, parsed.day);

    }

    public SqpTime parseSqpTime(String value) throws SqpException {
        PGTimestampUtils.ParsedTimestamp parsed = parseDateTime(value, TIMESTAMP_UTIL_TYPE_TIME);
        return new SqpTime(parsed.hour, parsed.minute, parsed.second, parsed.nanos, getTZOffset(parsed));
    }

    private Integer getTZOffset(PGTimestampUtils.ParsedTimestamp parsed) {
        return parsed.tz == null ? null : parsed.tz.getTimeZone().getRawOffset() / 1000;
    }

    private PGTimestampUtils.ParsedTimestamp parseDateTime(String value, String type) throws SqpException {
        try {
            return _pgTimestampUtils.loadCalendar(_utcCalendar, value, type);
        } catch (SQLException e) {
            throw new TypeConversionException("Failed to parse date/time value from server: " + e.getMessage(), e);
        }
    }
}
