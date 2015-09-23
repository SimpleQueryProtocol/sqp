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
import io.sqp.core.jackson.JacksonObjectMapperFactory;
import org.testng.annotations.Test;

import java.time.OffsetTime;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * @author Stefan Burnicki
 */
public class SqpTimeTest {

    @Test
    public void CanDecodeFromJson() throws Exception{
        String content = "[[13, 52, 5, 123456],7260]";
        ObjectMapper mapper = JacksonObjectMapperFactory.objectMapper(DataFormat.Text);

        Object timeObj = mapper.readValue(content, Object.class);
        SqpTime sqpTime = SqpTime.fromJsonFormatValue(timeObj);

        assertThat(sqpTime.hasOffset(), is(true));
        OffsetTime offsetTime = sqpTime.asOffsetTime();
        assertThat(offsetTime.getHour(), is(13));
        assertThat(offsetTime.getMinute(), is(52));
        assertThat(offsetTime.getSecond(), is(5));
        assertThat(offsetTime.getNano(), is(123456));
        assertThat(offsetTime.getOffset().getTotalSeconds(), is(7260));
    }

    @Test
    public void CanDecodeFromJsonWithoutOffset() throws Exception{
        String content = "[[13, 52, 5, 123456]]";
        ObjectMapper mapper = JacksonObjectMapperFactory.objectMapper(DataFormat.Text);

        Object timeObj = mapper.readValue(content, Object.class);
        SqpTime sqpTime = SqpTime.fromJsonFormatValue(timeObj);

        assertThat(sqpTime.hasOffset(), is(false));
        OffsetTime offsetTime = sqpTime.asOffsetTime();
        assertThat(offsetTime.getHour(), is(13));
        assertThat(offsetTime.getMinute(), is(52));
        assertThat(offsetTime.getSecond(), is(5));
        assertThat(offsetTime.getNano(), is(123456));
        assertThat(offsetTime.getOffset().getTotalSeconds(), is(0));
    }

}
