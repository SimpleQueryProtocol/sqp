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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Stefan Burnicki
 */
public class ArrayTypeMatcherTest {
    static private ObjectMapper _mapper = new ObjectMapper();

    static private ObjectNode newInt() {
        return _mapper.createObjectNode().put("type", "integer");
    }

    static private ObjectNode newNumber() {
        return _mapper.createObjectNode().put("type", "number");
    }

    static private ObjectNode newString() {
        return _mapper.createObjectNode().put("type", "string");
    }

    static private ObjectNode newArray(ObjectNode... items) {
        ObjectNode array = _mapper.createObjectNode().put("type", "array");
        if (items.length == 1) {
            array.set("items", items[0]);
        } else if (items.length > 1) {
            array.set("items", _mapper.createArrayNode().addAll(Arrays.asList(items)));
        }
        return array;
    }

    static private ObjectNode Int3String = newArray(newInt(), newNumber(), newInt(), newString());

    @Test(dataProvider = "compatibleToInt3Array")
    public void int3SchemaCompatibility(JsonNode matchingSchema) {
        ObjectNode int3Schema = newArray(newInt()).put("maxItems", 3);
        assertThat(new ArrayTypeMatcher(int3Schema).isCompatibleTo(matchingSchema), is(true));
    }

    @Test(dataProvider = "compatibleToInt3Array")
    public void int3ArraySchemaCompatibility(JsonNode matchingSchema) {
        ObjectNode int3Schema = newArray(newInt(), newInt(), newInt()).put("additionalItems", false);
        assertThat(new ArrayTypeMatcher(int3Schema).isCompatibleTo(matchingSchema), is(true));
    }

    @Test
    public void int3additionalIntsArrayNotCompatibleWithInt3String() {
        ObjectNode int3Schema = newArray(newInt(), newInt(), newInt());
        assertThat(new ArrayTypeMatcher(int3Schema).isCompatibleTo(Int3String), is(false));
    }

    @Test
    public void int3StringNotCompatibleWithIntSchemaArray() {
        ObjectNode intSchema = newArray(newInt());
        assertThat(new ArrayTypeMatcher(Int3String).isCompatibleTo(intSchema), is(false));
    }

    @Test(dataProvider = "compatibleToInt3Array")
    public void CompatibilityWithIntNumberAdditionalSchema(JsonNode schema) {
        ObjectNode intNumberAdditional = newArray(newInt(), newNumber()).put("additionalItems", true);
        assertThat(new ArrayTypeMatcher(schema).isCompatibleTo(intNumberAdditional), is(true));
    }

    @DataProvider(name = "compatibleToInt3Array")
    public static Object[][] compatibleToInt3Array() {
        return new Object[][] {
            {newArray(newInt())}, // just set items schema to number
            {newArray(newInt(), newNumber(), newInt())},  // set items schema array
            {newArray(newInt(), newNumber())}, // one element less
            {Int3String}, // string as 4th the end
            {newArray(newInt(), newInt()).put("additionalItems", true)}, // additional items are okay
            {newArray(newInt(), newNumber()).set("additionalItems", newInt())} // additional items are ints
        };
    }
}
