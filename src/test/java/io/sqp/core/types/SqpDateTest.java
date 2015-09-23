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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.sqp.core.DataFormat;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import io.sqp.core.jackson.JacksonObjectMapperFactory;

import java.time.LocalDate;
import java.time.Month;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * @author Stefan Burnicki
 */
public class SqpDateTest {
    @Test(dataProvider = "dateValues")
    public void CanDecodeFromJson(byte[] content, DataFormat format) throws Exception{
        ObjectMapper mapper = JacksonObjectMapperFactory.objectMapper(format);

        Object dateObj = mapper.readValue(content, Object.class);
        SqpDate sqpDate = SqpDate.fromJsonFormatValue(dateObj);

        LocalDate localDate = sqpDate.asLocalDate();
        assertThat(localDate.getYear(), is(2015));
        assertThat(localDate.getMonth(), is(Month.JUNE));
        assertThat(localDate.getDayOfMonth(), is(28));
    }

    @DataProvider(name="dateValues")
    public Object[][] dateValues() throws Exception {
        return new Object[][] {
            {"[2015, 6, 28]".getBytes("UTF-8"), DataFormat.Text},
            {new byte[] {(byte) 0x93, (byte) 0xcd, 0x07, (byte) 0xdf, 0x06, 0x1c}, DataFormat.Binary}
        };
    }
}
