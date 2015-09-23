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

package io.sqp.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.sqp.client.exceptions.UnexpectedResultTypeException;
import io.sqp.core.InformationSubject;
import io.sqp.core.types.SqpTypeCode;
import io.sqp.testhelpers.TestUtils;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.CompletionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.testng.Assert.fail;

/**
 * @author Stefan Burnicki
 */
public class InformationRequestTest extends AutoConnectTestBase {

    @Test
    public void InformationRequestForProxyInformation() {
        Boolean answer = connection.getInformation(Boolean.class, InformationSubject.SupportsBinaryProtocol).join();
        assertThat(answer, is(true));
    }

    @Test
    public void InformationRequestWithWrongResultType() {
        try {
            connection.getInformation(Integer.class, InformationSubject.SupportsBinaryProtocol).join();
            fail();
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            assertThat(cause, is(not(nullValue())));
            assertThat(cause, is(instanceOf(UnexpectedResultTypeException.class)));
            UnexpectedResultTypeException unexpectedResult = (UnexpectedResultTypeException) cause;
            assertThat(unexpectedResult.getExpectedClass().getName(), is(Integer.class.getName()));
            assertThat(unexpectedResult.getResult(), instanceOf(Boolean.class));
            assertThat(unexpectedResult.getResult(), is(true));
        }
    }

    @Test
    public void InformationRequestForBackendInformation() {
        String answer = connection.getInformation(String.class, InformationSubject.DBMSName).join();
        assertThat(answer, is(either(equalTo("Transbase")).or(equalTo("PostgreSQL"))));
    }

    @Test
    public void RequestSqpTypeSchema() throws IOException {
        String origSchema = TestUtils.readResourceAsString("schemas/Date.json");
        String schema = connection.getTypeSchema(SqpTypeCode.Date.toString()).join();
        assertThat(schema, is(origSchema));
    }

    @Test(groups={"native-postgres-only"})
    public void RequestPostgresSpecificTypeSchema() throws IOException {
        String origSchema = TestUtils.readResourceAsString("postgres-backend/schemas/point.json");
        String schema = connection.getTypeSchema("PG_POINT").join();
        assertThat(schema, is(origSchema));
    }

    @Test(groups={"native-transbase-only"})
    public void RequestTransbaseRangedDateTimeSchema() throws IOException {
        String schema = connection.getTypeSchema("tb_datetime[mo:hh]").join();
        JsonNode schemaNode = new ObjectMapper().readTree(schema);
        assertThat(schemaNode.get("minItems").asInt(), is(3));
        assertThat(schemaNode.get("additionalItems").asBoolean(), is(false));
        JsonNode itemsNode = schemaNode.get("items");
        assertThat(itemsNode.size(), is(3));
        checkDateFieldSchema(itemsNode.get(0), "MO", 1, 12, "Months");
        checkDateFieldSchema(itemsNode.get(1), "DD", 1, 31, "Days");
        checkDateFieldSchema(itemsNode.get(2), "HH", 0, 23, "Hours");
    }

    private void checkDateFieldSchema(JsonNode typeNode, String id, int min, int max, String title) {
        assertThat(typeNode.get("id").asText(), is(id));
        assertThat(typeNode.get("minimum").asInt(), is(min));
        assertThat(typeNode.get("maximum").asInt(), is(max));
        assertThat(typeNode.get("title").asText(), is(title));
    }

}
