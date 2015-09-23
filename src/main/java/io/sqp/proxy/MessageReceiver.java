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
import io.sqp.core.MessageDecoder;
import io.sqp.core.exceptions.DecodingException;
import io.sqp.core.exceptions.SqpException;
import io.sqp.core.messages.MessageType;
import io.sqp.core.messages.SqpMessage;
import io.sqp.proxy.exceptions.InvalidFrameException;

import java.io.ByteArrayInputStream;

/**
 * @author Stefan Burnicki
 */
public class MessageReceiver {
    private ClientSession _session;
    private ByteBuffer _currentMsg;
    private DataFormat _currentFormat;
    private MessageType _currentType;
    private MessageDecoder _messageDecoder;
    private boolean _waitingForLob;

    public MessageReceiver(ClientSession session, MessageDecoder decoder) {
        _messageDecoder = decoder;
        _session = session;
        _currentMsg = null;
        _currentType = null;
    }

    public void continueMessage(ByteBuffer buf, boolean finish) throws SqpException {
        verifyOpenMessage();

        // TODO: implement maximum size, e.g. 10MB and refer others to use BLOBs

        _currentMsg.append(buf);

        if (finish) {
            processMessageOrLob();
        }
    }

    public void newMessage(DataFormat format, ByteBuffer buf, boolean finish) throws SqpException {
        verifyNoOpenMessage();

        int offset = 0;
        _currentFormat = format;

        if (!_waitingForLob) {
            // normal messages
            if (buf.length() < 1) {
                throw new InvalidFrameException("The message buffer is empty. At least a type id is required.");
            }
            _currentType = MessageType.fromId((char) buf.getByte(0));
            offset = 1;
        }

        _currentMsg = buf.getBuffer(offset);

        if (finish) {
            processMessageOrLob();
        }
    }

    public void reset() {
        // so a new message can be handled
        _currentMsg = null;
        _currentType = null;
        _waitingForLob = false;
    }

    private void processMessageOrLob() throws SqpException {
        if (_waitingForLob) {
            _session.registerLob(_currentFormat, new ByteArrayInputStream(_currentMsg.getBytes()));
            reset();
        } else {
            processMessage();
        }
    }

    private void processMessage() throws DecodingException {
        SqpMessage msg;
        msg = _messageDecoder.decode(_currentType, _currentFormat, new ByteArrayInputStream(_currentMsg.getBytes()));
        msg.setMessageFormat(_currentFormat);
        reset();
        _waitingForLob = msg.isA(MessageType.LobAnnouncementMessage); // check if next message should be a lob
        _session.processMessage(msg); // actually process the message
    }


    private void verifyOpenMessage() throws InvalidFrameException {
        if (_currentMsg == null) {
            throw new InvalidFrameException("There is no existing message to be continued");
        }
    }

    private void verifyNoOpenMessage() throws InvalidFrameException {
        if (_currentMsg != null) {
            throw new InvalidFrameException("There is already a message which was not finished");
        }
    }

}
