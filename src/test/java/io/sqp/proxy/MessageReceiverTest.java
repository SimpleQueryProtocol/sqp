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

package io.sqp.proxy;

import io.sqp.core.DataFormat;
import io.sqp.core.messages.DummyMessage;
import io.sqp.core.messages.SqpMessage;
import io.sqp.proxy.vertx.VertxByteBuffer;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import io.sqp.core.MessageDecoder;
import io.sqp.core.exceptions.DecodingException;
import io.sqp.core.messages.MessageType;
import io.sqp.proxy.exceptions.InvalidFrameException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.InputStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.*;

/**
 * @author Stefan Burnicki
 */
public class MessageReceiverTest {

    private final String dummyMsgID = "" + MessageType.DummyMessage.getId();
    private SqpMessage dummyMessage;
    private ClientSession session;
    private MessageReceiver handler;
    private MessageDecoder msgdecoder;


    @BeforeMethod
    public void setUp() throws Exception {
        session = mock(ClientSession.class);
        msgdecoder = mock(MessageDecoder.class);
        handler = new MessageReceiver(session, msgdecoder);
        dummyMessage = new DummyMessage("test");
        when(msgdecoder.decode(anyObject(), anyObject(), anyObject())).thenReturn(dummyMessage);
    }

    @Test
    public void newMessageFinalGetsHandled() throws Exception {
        handler.newMessage(DataFormat.Binary, new VertxByteBuffer().append(dummyMsgID), true);
        verify(session).processMessage(dummyMessage);
    }

    @Test(expectedExceptions = InvalidFrameException.class)
    public void newMessageEmptyFails() throws Exception {
        handler.newMessage(DataFormat.Binary, new VertxByteBuffer(), true);
    }

    @Test
    public void newMessageNotFinalDoesntGetHandled() throws Exception {
        // make sure the partial message doesn't get decoded, yet
        handler.newMessage(DataFormat.Binary, new VertxByteBuffer().append(dummyMsgID), false);
        verify(msgdecoder, never()).decode(anyObject(), anyObject(), anyObject());
        verify(session, never()).processMessage(anyObject());
    }

    @Test(expectedExceptions = InvalidFrameException.class)
    public void newUnfinishedMessageTwiceFails() throws Exception {
        ByteBuffer buf = new VertxByteBuffer().append(dummyMsgID);
        handler.newMessage(DataFormat.Binary, buf, false);
        handler.newMessage(DataFormat.Binary, buf, true);
    }

    @Test
    public void twoMessages() throws Exception {
        ByteBuffer buf = new VertxByteBuffer().append(dummyMsgID);
        handler.newMessage(DataFormat.Binary, buf, false);
        handler.continueMessage(new VertxByteBuffer(), true);
        handler.newMessage(DataFormat.Binary, buf, true);

        verify(msgdecoder, Mockito.times(2)).decode(anyObject(), anyObject(), anyObject());
        verify(session, Mockito.times(2)).processMessage(dummyMessage);
    }

    @Test
    public void handlerWorksAfterResetAfterError() throws Exception {
        ByteBuffer buf = new VertxByteBuffer().append(dummyMsgID);
        InvalidFrameException exception = null;
        try {
            handler.continueMessage(new VertxByteBuffer(), true);
        } catch (InvalidFrameException e) {
            exception = e;
        }
        assertThat(exception, is(notNullValue()));

        handler.reset();
        handler.newMessage(DataFormat.Binary, buf, true);

        verify(msgdecoder).decode(anyObject(), anyObject(), anyObject());
        verify(session).processMessage(dummyMessage);
    }

    @Test(expectedExceptions = DecodingException.class)
    public void newMessageInvalidTypeFails() throws Exception {
        // ' is an invalid message type identifier
        handler.newMessage(DataFormat.Binary, new VertxByteBuffer().append("'"), false);
    }

    @Test(expectedExceptions = InvalidFrameException.class)
    public void continueMessageWithoutNewFails() throws Exception {
        handler.continueMessage(new VertxByteBuffer(), false);
    }

    @Test(dataProvider = "messageFormats")
    public void continueMessageGetsHandled(DataFormat dataFormat) throws Exception {
        ArgumentCaptor<InputStream> argCap = ArgumentCaptor.forClass(InputStream.class);

        handler.newMessage(dataFormat, new VertxByteBuffer().append(dummyMsgID +"buffer"), false);
        handler.continueMessage(new VertxByteBuffer().append("content"), true);

        verify(session).processMessage(dummyMessage);
        verify(msgdecoder).decode(eq(MessageType.DummyMessage), eq(dataFormat), argCap.capture());

        InputStream captured = argCap.getValue();
        byte[] buffer = new byte[13];
        captured.read(buffer);
        String value = new String(buffer, "UTF-8");
        assertThat(value, is("buffercontent"));
    }

     @DataProvider
     public static Object[][] messageFormats() {
         return new Object[][] {{DataFormat.Binary}, {DataFormat.Text}};
     }
}
