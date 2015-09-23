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
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.ServerWebSocket;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import io.sqp.proxy.vertx.WebsocketWriteStream;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static io.sqp.proxy.testhelpers.WebSocketFrameMatcher.binaryFrameEq;
import static io.sqp.proxy.testhelpers.WebSocketFrameMatcher.continuationFrameEq;
import static io.sqp.proxy.testhelpers.WebSocketFrameMatcher.textFrameEq;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;

/**
 * @author Stefan Burnicki
 */

public class WebsocketWriteStreamTest {
    final private int MAX_FRAMESIZE = 5;
    private ServerWebSocket _socket;
    private WebsocketWriteStream _wsStream;

    @BeforeMethod
    public void setUp() throws Exception {
        _socket = mock(ServerWebSocket.class);
        _wsStream = new WebsocketWriteStream(_socket, MAX_FRAMESIZE);
        _wsStream.setDataFormat(DataFormat.Binary);
    }

    @Test
    public void writeOneByteAndClose() throws Exception {
        byte b = 0x13; // arbitrary byte
        _wsStream.write(Buffer.buffer(new byte[]{b}));
        _wsStream.finishCurrentMessage(); // should flush and send websocket frame

        verify(_socket).writeFrame(binaryFrameEq(true, new byte[]{b}));
    }

    @Test
    public void closeUnusedStreamDoesntSend() throws Exception {
        _wsStream.finishCurrentMessage();
        verify(_socket, never()).writeFrame(anyObject());
    }

    @Test
    public void writeOneByteWithoutFinishedDoesntSend() throws Exception {
        _wsStream.write(Buffer.buffer(new byte[]{0x13}));
        verify(_socket, never()).writeFrame(anyObject());
    }

    @Test
    public void writeMultipleBytesAndClose() throws Exception {
        byte[] bytes = {0x1, 0x3, 0x5, 0x7};
        _wsStream.write(Buffer.buffer(bytes));
        _wsStream.finishCurrentMessage();

        verify(_socket).writeFrame(binaryFrameEq(true, bytes));
    }

    @Test
    public void writeTwoDifferentMessages() throws Exception {
        byte[] bytes = {0x1, 0x3, 0x5, 0x7};
        String strContent = "foo";

        _wsStream.write(Buffer.buffer(bytes));
        _wsStream.finishCurrentMessage();

        _wsStream.setDataFormat(DataFormat.Text);
        _wsStream.write(Buffer.buffer(strContent.getBytes(StandardCharsets.UTF_8)));
        _wsStream.finishCurrentMessage();

        verify(_socket).writeFrame(binaryFrameEq(true, bytes));
        verify(_socket).writeFrame(textFrameEq(true, strContent));
    }

    @Test
    public void writeUnicodeFrameAndClose() throws Exception {
        String unicodeStr = "\u0628\u00de";

        _wsStream.setDataFormat(DataFormat.Text);
        _wsStream.write(Buffer.buffer(unicodeStr.getBytes(StandardCharsets.UTF_8)));
        _wsStream.finishCurrentMessage();

        verify(_socket).writeFrame(textFrameEq(true, unicodeStr));
    }

    @Test
    public void streamAutoFlushesFrames() throws Exception {
        byte[] bytes = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};

        _wsStream.write(Buffer.buffer(bytes));

        verify(_socket).writeFrame(binaryFrameEq(false, Arrays.copyOfRange(bytes, 0, 5)));
        verify(_socket).writeFrame(continuationFrameEq(false, Arrays.copyOfRange(bytes, 5, 10)));

        _wsStream.finishCurrentMessage();
        verify(_socket).writeFrame(continuationFrameEq(true, Arrays.copyOfRange(bytes, 10, 11)));
    }
}
