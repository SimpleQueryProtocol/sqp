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
import io.sqp.schemamatcher.JsonType;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Stefan Burnicki
 */
public class NumberTypeMatcherTest {
    private static ObjectMapper _mapper = new ObjectMapper();

    private static ObjectNode newObject() {
        return _mapper.createObjectNode();
    }

    private static ObjectNode newNumber() {
        return newObject().put("type", "number");
    }

    private static ObjectNode newInt() {
        return newObject().put("type", "integer");
    }

    @Test
    public void SimpleIntegerMatch() {
        assertThat(new NumberTypeMatcher(JsonType.Integer, newInt()).isCompatibleTo(newInt()), is(true));
    }

    @Test
    public void SimpleNumberMatch() {
        assertThat(new NumberTypeMatcher(JsonType.Number, newNumber()).isCompatibleTo(newNumber()), is(true));
    }

    @Test
    public void IntMatchesNumber() {
        assertThat(new NumberTypeMatcher(JsonType.Integer, newInt()).isCompatibleTo(newNumber()), is(true));
    }

    @Test
    public void NumberDoesNotMatchInt() {
        assertThat(new NumberTypeMatcher(JsonType.Number, newNumber()).isCompatibleTo(newInt()), is(false));
    }

    @Test
    public void NumberMultipleOfMatch() {
        JsonNode orig = newNumber().put("multipleOf", 7.8);
        JsonNode sub = newNumber().put("multipleOf", 3.9);
        assertThat(new NumberTypeMatcher(JsonType.Number, sub).isCompatibleTo(orig), is(true));
    }

    @Test
    public void NumberMultipleOfIsSameMatch() {
        JsonNode orig = newNumber().put("multipleOf", 3.9);
        JsonNode sub = newNumber().put("multipleOf", 3.9);
        assertThat(new NumberTypeMatcher(JsonType.Number, sub).isCompatibleTo(orig), is(true));
    }

    @Test
    public void NumberMultipleOfMisMatch() {
        JsonNode orig = newNumber().put("multipleOf", 3.9);
        JsonNode sub = newNumber().put("multipleOf", 3.8);
        assertThat(new NumberTypeMatcher(JsonType.Number, sub).isCompatibleTo(orig), is(false));
    }

    @Test
    public void NumberMultipleOfMissing() {
        JsonNode orig = newNumber();
        JsonNode sub = newNumber().put("multipleOf", 3.8);
        assertThat(new NumberTypeMatcher(JsonType.Number, sub).isCompatibleTo(orig), is(true));
    }

    @Test
    public void NumberMultipleOfMissingMisMatch() {
        JsonNode orig = newNumber().put("multipleOf", 3.8);
        JsonNode sub = newNumber();
        assertThat(new NumberTypeMatcher(JsonType.Number, sub).isCompatibleTo(orig), is(false));
    }

    @Test
    public void NumberMinSameMatch() {
        JsonNode orig = newNumber().put("minimum", -1.1);
        JsonNode sub = newNumber().put("minimum", -1.1);
        assertThat(new NumberTypeMatcher(JsonType.Number, sub).isCompatibleTo(orig), is(true));
    }

    @Test
    public void NumberMinSameExclusiveMatch() {
        JsonNode orig = newNumber().put("minimum", -1.1);
        JsonNode sub = newNumber().put("minimum", -1.1).put("exclusiveMinimum", true);
        assertThat(new NumberTypeMatcher(JsonType.Number, sub).isCompatibleTo(orig), is(true));
    }

    @Test
    public void NumberMinSameExclusiveMisMatch() {
        JsonNode orig = newNumber().put("minimum", -1.1).put("exclusiveMinimum", true);
        JsonNode sub = newNumber().put("minimum", -1.1);
        assertThat(new NumberTypeMatcher(JsonType.Number, sub).isCompatibleTo(orig), is(false));
    }

    @Test
    public void NumberMinExclusiveMatch() {
        JsonNode orig = newNumber().put("minimum", -1.1);
        JsonNode sub = newNumber().put("minimum", -0.9).put("exclusiveMinimum", true);
        assertThat(new NumberTypeMatcher(JsonType.Number, sub).isCompatibleTo(orig), is(true));
    }

    @Test
    public void NumberMinExclusiveMatch2() {
        JsonNode orig = newNumber().put("minimum", -1.1).put("exclusiveMinimum", true);
        JsonNode sub = newNumber().put("minimum", -0.9);
        assertThat(new NumberTypeMatcher(JsonType.Number, sub).isCompatibleTo(orig), is(true));
    }

    @Test
    public void NumberMinMatch() {
        JsonNode orig = newNumber().put("minimum", -1.1);
        JsonNode sub = newNumber().put("minimum", -0.9);
        assertThat(new NumberTypeMatcher(JsonType.Number, sub).isCompatibleTo(orig), is(true));
    }

    @Test
    public void NumberMinMisMatch() {
        JsonNode orig = newNumber().put("minimum", 1.1);
        JsonNode sub = newNumber().put("minimum", -0.9);
        assertThat(new NumberTypeMatcher(JsonType.Number, sub).isCompatibleTo(orig), is(false));
    }

    @Test
    public void NumberMinMissingMatch() {
        JsonNode orig = newNumber();
        JsonNode sub = newNumber().put("minimum", -0.9);
        assertThat(new NumberTypeMatcher(JsonType.Number, sub).isCompatibleTo(orig), is(true));
    }

    @Test
    public void NumberMinMissingMisMatch() {
        JsonNode orig = newNumber().put("minimum", -0.9);
        JsonNode sub = newNumber();
        assertThat(new NumberTypeMatcher(JsonType.Number, sub).isCompatibleTo(orig), is(false));
    }

    @Test
    public void NumberMaxSameMatch() {
        JsonNode orig = newNumber().put("maximum", -1.1);
        JsonNode sub = newNumber().put("maximum", -1.1);
        assertThat(new NumberTypeMatcher(JsonType.Number, sub).isCompatibleTo(orig), is(true));
    }

    @Test
    public void NumberMaxSameExclusiveMatch() {
        JsonNode orig = newNumber().put("maximum", -1.1);
        JsonNode sub = newNumber().put("maximum", -1.1).put("exclusiveMaximum", true);
        assertThat(new NumberTypeMatcher(JsonType.Number, sub).isCompatibleTo(orig), is(true));
    }

    @Test
    public void NumberMaxSameExclusiveMisMatch() {
        JsonNode orig = newNumber().put("maximum", -1.1).put("exclusiveMaximum", true);
        JsonNode sub = newNumber().put("maximum", -1.1);
        assertThat(new NumberTypeMatcher(JsonType.Number, sub).isCompatibleTo(orig), is(false));
    }

    @Test
    public void NumberMaxExclusiveMatch() {
        JsonNode orig = newNumber().put("maximum", -0.9);
        JsonNode sub = newNumber().put("maximum", -1.1).put("exclusiveMaximum", true);
        assertThat(new NumberTypeMatcher(JsonType.Number, sub).isCompatibleTo(orig), is(true));
    }

    @Test
    public void NumberMaxExclusiveMatch2() {
        JsonNode orig = newNumber().put("maximum", -0.9).put("exclusiveMaximum", true);
        JsonNode sub = newNumber().put("maximum", -1.1);
        assertThat(new NumberTypeMatcher(JsonType.Number, sub).isCompatibleTo(orig), is(true));
    }

    @Test
    public void NumberMaxMatch() {
        JsonNode orig = newNumber().put("maximum", -0.9);
        JsonNode sub = newNumber().put("maximum", -1.1);
        assertThat(new NumberTypeMatcher(JsonType.Number, sub).isCompatibleTo(orig), is(true));
    }

    @Test
    public void NumberMaxMisMatch() {
        JsonNode orig = newNumber().put("maximum", -0.1);
        JsonNode sub = newNumber().put("maximum", 0.9);
        assertThat(new NumberTypeMatcher(JsonType.Number, sub).isCompatibleTo(orig), is(false));
    }

    @Test
    public void NumberMaxMissingMatch() {
        JsonNode orig = newNumber();
        JsonNode sub = newNumber().put("maximum", -0.9);
        assertThat(new NumberTypeMatcher(JsonType.Number, sub).isCompatibleTo(orig), is(true));
    }

    @Test
    public void NumberMaxMissingMisMatch() {
        JsonNode orig = newNumber().put("maximum", -0.9);
        JsonNode sub = newNumber();
        assertThat(new NumberTypeMatcher(JsonType.Number, sub).isCompatibleTo(orig), is(false));
    }
}
