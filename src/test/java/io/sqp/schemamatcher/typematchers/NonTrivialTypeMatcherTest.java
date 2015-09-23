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
public class NonTrivialTypeMatcherTest {
    static private ObjectMapper _mapper = new ObjectMapper();

    static private ObjectNode newString() {
        return _mapper.createObjectNode().put("type", "string");
    }

    @Test
    public void FormatOneDefinedMatch() {
        JsonNode orig = newString();
        JsonNode sub = newString().put("format", "email");
        assertThat(new StringTypeMatcher(sub).isCompatibleTo(orig), is(true));
    }

    @Test
    public void FormatOneDefinedMisMatch() {
        JsonNode orig = newString().put("format", "email");
        JsonNode sub = newString();
        assertThat(new StringTypeMatcher(sub).isCompatibleTo(orig), is(false));
    }

    @Test
    public void FormatDifferentMisMatch() {
        JsonNode orig = newString().put("format", "email");
        JsonNode sub = newString().put("format", "date");
        assertThat(new StringTypeMatcher(sub).isCompatibleTo(orig), is(false));
    }

    @Test
    public void FormatSameMatch() {
        JsonNode orig = newString().put("format", "email");
        JsonNode sub = newString().put("format", "email");
        assertThat(new StringTypeMatcher(sub).isCompatibleTo(orig), is(true));
    }
}
