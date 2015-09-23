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

package io.sqp.schemamatcher.typematchers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.testng.annotations.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Stefan Burnicki
 */
public class StringTypeMatcherTest {
    static private ObjectMapper _mapper = new ObjectMapper();

     static ObjectNode newString() {
        return _mapper.createObjectNode().put("type", "string");
     }

    @Test
    public void StringTypesMatch() {
        assertThat(new StringTypeMatcher(newString()).isCompatibleTo(newString()), is(true));
    }

    @Test
    public void StringOneWithMinLengthMatch() {
        JsonNode orig = newString();
        JsonNode sub = newString().put("minLength", 10);
        assertThat(new StringTypeMatcher(sub).isCompatibleTo(orig), is(true));
    }

    @Test
    public void StringWithSameMinLengthMatch() {
        JsonNode orig = newString().put("minLength", 10);
        JsonNode sub = newString().put("minLength", 10);
        assertThat(new StringTypeMatcher(sub).isCompatibleTo(orig), is(true));
    }

    @Test
    public void StringWithGreaterMinLengthMatch() {
        JsonNode orig = newString().put("minLength", 10);
        JsonNode sub = newString().put("minLength", 15);
        assertThat(new StringTypeMatcher(sub).isCompatibleTo(orig), is(true));
    }

    @Test
    public void StringWithSmallerMinLengthMisMatch() {
        JsonNode orig = newString().put("minLength", 15);
        JsonNode sub = newString().put("minLength", 10);
        assertThat(new StringTypeMatcher(sub).isCompatibleTo(orig), is(false));
    }

    @Test
    public void StringOneWithMinLengthMisMatch() {
        JsonNode orig = newString().put("minLength", 10);
        JsonNode sub = newString();
        assertThat(new StringTypeMatcher(sub).isCompatibleTo(orig), is(false));
    }

    @Test
    public void StringOneWithMaxLengthMatch() {
        JsonNode orig = newString();
        JsonNode sub = newString().put("maxLength", 10);
        assertThat(new StringTypeMatcher(sub).isCompatibleTo(orig), is(true));
    }

    @Test
    public void StringWithSameMaxLengthMatch() {
        JsonNode orig = newString().put("maxLength", 10);
        JsonNode sub = newString().put("maxLength", 10);
        assertThat(new StringTypeMatcher(sub).isCompatibleTo(orig), is(true));
    }

    @Test
    public void StringWithGreaterMaxLengthMisMatch() {
        JsonNode orig = newString().put("maxLength", 10);
        JsonNode sub = newString().put("maxLength", 15);
        assertThat(new StringTypeMatcher(sub).isCompatibleTo(orig), is(false));
    }

    @Test
    public void StringWithSmallerMaxLengthMatch() {
        JsonNode orig = newString().put("maxLength", 15);
        JsonNode sub = newString().put("maxLength", 10);
        assertThat(new StringTypeMatcher(sub).isCompatibleTo(orig), is(true));
    }

    @Test
    public void StringOneWithMaxLengthMisMatch() {
        JsonNode orig = newString().put("maxLength", 10);
        JsonNode sub = newString();
        assertThat(new StringTypeMatcher(sub).isCompatibleTo(orig), is(false));
    }

    @Test
    public void StringOneFormatMisMatch() {
        JsonNode orig = newString().put("format", "\\d+");
        JsonNode sub = newString();
        assertThat(new StringTypeMatcher(sub).isCompatibleTo(orig), is(false));
    }

    @Test
    public void StringOneFormatMatch() {
        JsonNode orig = newString();
        JsonNode sub = newString().put("format", "\\d+");
        assertThat(new StringTypeMatcher(sub).isCompatibleTo(orig), is(true));
    }

    @Test
    public void StringSameFormatMatch() {
        JsonNode orig = newString().put("format", "\\d+");
        JsonNode sub = newString().put("format", "\\d+");
        assertThat(new StringTypeMatcher(sub).isCompatibleTo(orig), is(true));
    }
}
