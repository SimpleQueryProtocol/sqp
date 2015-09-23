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

import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.SQLException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.SimpleTimeZone;
import java.util.TimeZone;

/**
 * The following code is taken from  org.postgresql.jdbc2.TimestampUtils.
 * See https://github.com/pgjdbc/pgjdbc/blob/master/org/postgresql/jdbc2/TimestampUtils.java
 * It's copied, because I don't want to rewrite the timestamp-parsing logic.
 * Using the class by reflection would by a very complicated option, since the loadCalendar
 * method (which is actually needed) returns a private type. In addition, I don't want to rely
 * on the consistency of private methods. I marked the modified lines with #modified.
 * Methods that aren't needed were removed
 */



/**
 * Misc utils for handling time and date values.
 */
public class PGTimestampUtils { // #modified: Renamed to PGTimestampUtils
    // #modified: removed not needed fields
    private Calendar calCache;
    private int calCacheZone;

    private final boolean min82;

    // #modified: Renamed to PGTimestampUtils, removed not needed parameters
    PGTimestampUtils( boolean min82) {
        this.min82 = min82;
    }

    private Calendar getCalendar(int sign, int hr, int min, int sec) {
        int rawOffset = sign * (((hr * 60 + min) * 60 + sec) * 1000);
        if (calCache != null && calCacheZone == rawOffset)
            return calCache;

        StringBuilder zoneID = new StringBuilder("GMT");
        zoneID.append(sign < 0 ? '-' : '+');
        if (hr < 10) zoneID.append('0');
        zoneID.append(hr);
        if (min < 10) zoneID.append('0');
        zoneID.append(min);
        if (sec < 10) zoneID.append('0');
        zoneID.append(sec);

        TimeZone syntheticTZ = new SimpleTimeZone(rawOffset, zoneID.toString());
        calCache = new GregorianCalendar(syntheticTZ);
        calCacheZone = rawOffset;
        return calCache;
    }

    static class ParsedTimestamp { // #modified: Removed private modifier
        boolean hasDate = false;
        int era = GregorianCalendar.AD;
        int year = 1970;
        int month = 1;

        boolean hasTime = false;
        int day = 1;
        int hour = 0;
        int minute = 0;
        int second = 0;
        int nanos = 0;

        Calendar tz = null;
    }

    /**
     * Load date/time information into the provided calendar
     * returning the fractional seconds.
     */
    ParsedTimestamp loadCalendar(Calendar defaultTz, String str, String type) throws SQLException { // #modified: Removed private modifier
        char []s = str.toCharArray();
        int slen = s.length;

        // This is pretty gross..
        ParsedTimestamp result = new ParsedTimestamp();

        // We try to parse these fields in order; all are optional
        // (but some combinations don't make sense, e.g. if you have
        //  both date and time then they must be whitespace-separated).
        // At least one of date and time must be present.

        //   leading whitespace
        //   yyyy-mm-dd
        //   whitespace
        //   hh:mm:ss
        //   whitespace
        //   timezone in one of the formats:  +hh, -hh, +hh:mm, -hh:mm
        //   whitespace
        //   if date is present, an era specifier: AD or BC
        //   trailing whitespace

        try {
            int start = skipWhitespace(s, 0);   // Skip leading whitespace
            int end = firstNonDigit(s, start);
            int num;
            char sep;

            // Possibly read date.
            if (charAt(s, end) == '-') {
                //
                // Date
                //
                result.hasDate = true;

                // year
                result.year = number(s, start, end);
                start = end + 1; // Skip '-'

                // month
                end = firstNonDigit(s, start);
                result.month = number(s, start, end);

                sep = charAt(s, end);
                if (sep != '-')
                    throw new NumberFormatException("Expected date to be dash-separated, got '" + sep + "'");

                start = end + 1; // Skip '-'

                // day of month
                end = firstNonDigit(s, start);
                result.day = number(s, start, end);

                start = skipWhitespace(s, end); // Skip trailing whitespace
            }

            // Possibly read time.
            if (Character.isDigit(charAt(s, start))) {
                //
                // Time.
                //

                result.hasTime = true;

                // Hours

                end = firstNonDigit(s, start);
                result.hour = number(s, start, end);

                sep = charAt(s, end);
                if (sep != ':')
                    throw new NumberFormatException("Expected time to be colon-separated, got '" + sep + "'");

                start = end + 1; // Skip ':'

                // minutes

                end = firstNonDigit(s, start);
                result.minute = number(s, start, end);

                sep = charAt(s, end);
                if (sep != ':')
                    throw new NumberFormatException("Expected time to be colon-separated, got '" + sep + "'");

                start = end + 1; // Skip ':'

                // seconds

                end = firstNonDigit(s, start);
                result.second = number(s, start, end);
                start = end;

                // Fractional seconds.
                if (charAt(s, start) == '.') {
                    end = firstNonDigit(s, start+1); // Skip '.'
                    num = number(s, start+1, end);

                    for (int numlength = (end - (start+1)); numlength < 9; ++numlength)
                        num *= 10;

                    result.nanos = num;
                    start = end;
                }

                start = skipWhitespace(s, start); // Skip trailing whitespace
            }

            // Possibly read timezone.
            sep = charAt(s, start);
            if (sep == '-' || sep == '+') {
                int tzsign = (sep == '-') ? -1 : 1;
                int tzhr, tzmin, tzsec;

                end = firstNonDigit(s, start+1);    // Skip +/-
                tzhr = number(s, start+1, end);
                start = end;

                sep = charAt(s, start);
                if (sep == ':') {
                    end = firstNonDigit(s, start+1);  // Skip ':'
                    tzmin = number(s, start+1, end);
                    start = end;
                } else {
                    tzmin = 0;
                }

                tzsec = 0;
                if (min82) {
                    sep = charAt(s, start);
                    if (sep == ':') {
                        end = firstNonDigit(s, start+1);  // Skip ':'
                        tzsec = number(s, start+1, end);
                        start = end;
                    }
                }

                // Setting offset does not seem to work correctly in all
                // cases.. So get a fresh calendar for a synthetic timezone
                // instead
                result.tz = getCalendar(tzsign, tzhr, tzmin, tzsec);

                start = skipWhitespace(s, start);  // Skip trailing whitespace
            }

            if (result.hasDate && start < slen) {
                String eraString = new String(s, start, slen - start) ;
                if (eraString.startsWith("AD")) {
                    result.era = GregorianCalendar.AD;
                    start += 2;
                } else if (eraString.startsWith("BC")) {
                    result.era = GregorianCalendar.BC;
                    start += 2;
                }
            }

            if (start < slen)
                throw new NumberFormatException("Trailing junk on timestamp: '" + new String(s, start, slen - start) + "'");

            if (!result.hasTime && !result.hasDate)
                throw new NumberFormatException("Timestamp has neither date nor time");

        } catch (NumberFormatException nfe) {
            throw new PSQLException(GT.tr("Bad value for type {0} : {1}", new Object[]{type,str}), PSQLState.BAD_DATETIME_FORMAT, nfe);
        }

        return result;
    }

    private static int skipWhitespace(char []s, int start)
    {
        int slen = s.length;
        for (int i=start; i<slen; i++) {
            if (!Character.isSpaceChar(s[i]))
                return i;
        }
        return slen;
    }

    private static int firstNonDigit(char []s, int start)
    {
        int slen = s.length;
        for (int i=start; i<slen; i++) {
            if (!Character.isDigit(s[i])) {
                return i;
            }
        }
        return slen;
    }

    private static int number(char []s, int start, int end) {
        if (start >= end) {
            throw new NumberFormatException();
        }
        int n=0;
        for ( int i=start; i < end; i++)
        {
            n = 10 * n + (s[i]-'0');
        }
        return n;
    }

    private static char charAt(char []s, int pos) {
        if (pos >= 0 && pos < s.length) {
            return s[pos];
        }
        return '\0';
    }
}
