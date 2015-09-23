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

package io.sqp.proxy.vertx;

import io.sqp.core.DataFormat;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.mockito.Mockito.spy;

/**
 * @author Stefan Burnicki
 */
public class WebsocketSendQueueStreamTest {
    private WebsocketSendQueueStream _queueStream;
    private List<DataFormat> _formats;
    private List<String> _handledMessages;
    private int _numEndCalled;

    @BeforeMethod
    public void initStream() {
        _numEndCalled = 0;
        _formats = new ArrayList<>();
        _handledMessages = new ArrayList<>();
        _queueStream = spy(new WebsocketSendQueueStream());
        _queueStream.streamEndedHandler(v -> _numEndCalled++);
        _queueStream.streamStartedHandler(_formats::add);
        _queueStream.handler(buf -> _handledMessages.add(buf.toString("UTF-8")));
    }

    @Test
    public void canSendTwoConsecutiveStreams() {
        _queueStream.addStream(new DummyReadStream("bar", "foo"), DataFormat.Text);
        _queueStream.addStream(new DummyReadStream("bytes"), DataFormat.Binary);

        assertThat(_formats, contains(DataFormat.Text, DataFormat.Binary));
        assertThat(_numEndCalled, is(2));
        assertThat(_handledMessages, contains("bar", "foo", "bytes"));
    }

    class DummyReadStream implements ReadStream<Buffer> {
        private Handler<Buffer> _handler;
        private Handler<Void> _endHandler;
        private String[] _messages;

        public DummyReadStream(String... messages) {
            _messages = messages;
        }

        @Override
        public ReadStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
            return this;
        }

        @Override
        public ReadStream<Buffer> handler(Handler<Buffer> handler) {
            _handler = handler;
            return this;
        }

        @Override
        public ReadStream<Buffer> pause() {
            return this;
        }

        @Override
        public ReadStream<Buffer> resume() {
            Arrays.stream(_messages).map(m -> Buffer.buffer(m, "UTF-8")).forEach(_handler::handle);
            _endHandler.handle(null);
            return this;
        }

        @Override
        public ReadStream<Buffer> endHandler(Handler<Void> endHandler) {
            _endHandler = endHandler;
            return this;
        }
    }
}
