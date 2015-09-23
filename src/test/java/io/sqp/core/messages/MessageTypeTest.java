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

package io.sqp.core.messages;

import org.testng.annotations.Test;
import io.sqp.proxy.testhelpers.ClassFinder;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;

/**
 * @author Stefan Burnicki
 * These tests make sure that the MessageType enums are used consistently with their linked types
 */
public class MessageTypeTest {

    @Test
    public void allTypeEnumsHaveDifferentIds() {
        List<Character> ids = new ArrayList<>();
        for (MessageType t : MessageType.values()) {
            Character id = t.getId();

            assertThat("The id '" + id + "'  is used multiple times.", ids, not(hasItem(id)));
            ids.add(id);
        }
    }

    @Test
    public void allTypeEnumsLinkDifferentTypes() {
        List<Class<? extends SqpMessage>> types = new ArrayList<>();
        for (MessageType t : MessageType.values()) {
            Class<? extends SqpMessage> type = t.getType();

            assertThat("The type '" + type + "' is used multiple times.", types, not(hasItem(type)));
            types.add(type);
        }
    }

    @Test
    public void allTypeEnumsLinkTypesWithSameName() {
        for (MessageType t : MessageType.values()) {
            assertThat(t.toString(), is(t.getType().getSimpleName()));
        }
    }

    @Test
    public void allMessageTypesHaveEnumValues() {
        List<Class<? extends SqpMessage>> msgClasses = ClassFinder.find("io.sqp.core.messages", SqpMessage.class);
        for (Class<? extends SqpMessage> clazz : msgClasses) {
            MessageType t = MessageType.valueOf(clazz.getSimpleName()); // would throw if enum doesn't exist
            assertThat(t.getType(), equalTo(clazz)); // make sure the enums value corresponds to the class
        }
    }
}
