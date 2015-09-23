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

package io.sqp.proxy.testhelpers;

import io.vertx.core.http.WebSocketFrame;
import org.hamcrest.Description;
import org.mockito.ArgumentMatcher;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.mockito.Matchers.argThat;

/**
 * @author Stefan Burnicki
 */
public class WebSocketFrameMatcher extends ArgumentMatcher<WebSocketFrame> {
    enum FrameType {
        Binary,
        Text,
        Continuation
    }

    private boolean _isFinal;
    private FrameType _frameType;
    private byte[] _expectedContent;

    public WebSocketFrameMatcher(boolean isFinal, FrameType type, byte[] content) {
        _isFinal = isFinal;
        _frameType = type;
        _expectedContent = content;
    }

    @Override
    public boolean matches(Object argument) {
        if (!(argument instanceof WebSocketFrame)) {
            return false;
        }
        WebSocketFrame frame = (WebSocketFrame) argument;
        if (_isFinal != frame.isFinal()) {
            return false;
        }

        if (_frameType.equals(FrameType.Binary) && !frame.isBinary()) {
            return false;
        } else if (_frameType.equals(FrameType.Text) && !frame.isText()) {
            return false;
        } else if (_frameType.equals(FrameType.Continuation) && !frame.isContinuation()) {
            return false;
        }

        byte[] frameContent = frame.binaryData().getBytes();
        return Arrays.equals(_expectedContent, frameContent);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText(toString());
    }

    @Override
    public String toString() {
        return "WebSocketFrameMatcher{" +
                "final=" + _isFinal +
                ", type=" + _frameType +
                ", content=" + Arrays.toString(_expectedContent) +
                '}';
    }

    public static WebSocketFrame binaryFrameEq(boolean isFinal, byte[] content) {
        return argThat(new WebSocketFrameMatcher(isFinal, WebSocketFrameMatcher.FrameType.Binary, content));
    }

    public static WebSocketFrame textFrameEq(boolean isFinal, String content) {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        return argThat(new WebSocketFrameMatcher(isFinal, WebSocketFrameMatcher.FrameType.Text, bytes));
    }

    public static WebSocketFrame continuationFrameEq(boolean isFinal, byte[] content) {
        return argThat(new WebSocketFrameMatcher(isFinal, WebSocketFrameMatcher.FrameType.Continuation, content));
    }
}
