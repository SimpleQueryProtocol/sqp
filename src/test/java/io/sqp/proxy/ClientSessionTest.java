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


import io.sqp.backend.BackendConnection;
import io.sqp.core.DataFormat;
import io.sqp.core.ErrorType;
import io.sqp.core.messages.*;
import io.sqp.proxy.testhelpers.DummyBackend;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import io.sqp.backend.ResultHandler;
import io.sqp.core.ErrorAction;
import io.sqp.core.exceptions.SqpException;
import io.sqp.proxy.exceptions.ServerErrorException;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;

/**
 * @author Stefan Burnicki
 */
public class ClientSessionTest {
    @Mock
    BackendConnectionPool backendPool;

    @Mock
    ClientConnection connection;

    ClientSession session;

    @BeforeMethod
    public void initMocks(){
        MockitoAnnotations.initMocks(this);
        when(backendPool.getBackend()).thenReturn(new DummyBackend());
        session = new ClientSession(backendPool, connection);
    }


    @Test
    public void firstClientSessionStateIsUninitialized() {
        assertThat(session.getState(), is(ClientSessionState.Uninitialised));
    }

    @Test
    public void handleAbortErrorSendsErrorMessageAndClosesConnection() {
        session.handleError(new SqpException(ErrorType.ServerError, "test error", ErrorAction.Abort));

        assertThat(session.getState(), is(ClientSessionState.Dead));
        verify(connection).sendMessage(any(ErrorMessage.class));
        verify(connection, never()).reset();
        verify(connection).close();
    }

    @Test
    public void uninitializedClientStateCantRecover() {
        session.recover();

        assertThat(session.getState(), is(ClientSessionState.Dead));
        verify(connection, never()).reset();
        verify(connection).sendMessage(any(ErrorMessage.class));
        verify(connection).close();
    }

    @Test
    public void helloMessageConnectsSession() throws SqpException
    {
        sendHelloMessage(session);

        assertThat(session.getState(), is(ClientSessionState.Ready));
        verify(connection).setMessageFormat(Matchers.eq(DataFormat.Text));
        verify(connection).sendMessage(any(ReadyMessage.class));
    }


    @Test
    public void binaryHelloMessageSetsBinaryFormat() throws SqpException
    {
        sendHelloMessage(session, DataFormat.Binary);

        assertThat(session.getState(), is(ClientSessionState.Ready));
        verify(connection).setMessageFormat(eq(DataFormat.Binary));
        verify(connection).sendMessage(any(ReadyMessage.class));
    }

    @Test
    public void failedConnectionSendsErrorAndAborts() throws SqpException
    {
        when(backendPool.createConnection(eq("test"), anyObject(), anyObject())).then(invocation -> {
            ((ResultHandler<BackendConnection>) invocation.getArguments()[2]).fail(new ServerErrorException("error"));
            return 0l;
        });

        session.processMessage(new HelloMessage("test"));
        assertThat(session.getState(), is(ClientSessionState.Dead));
        verify(connection).sendMessage(any(ErrorMessage.class));
        verify(connection).close();
    }


    @Test
    public void handleRecoverErrorSendsErrorMessageAndResetsConnection() {
        ArgumentCaptor<SqpMessage> argCap = ArgumentCaptor.forClass(SqpMessage.class);

        sendHelloMessage(session);
        session.handleError(new SqpException(ErrorType.ServerError, "test error", ErrorAction.Recover));

        assertThat(session.getState(), is(ClientSessionState.Ready));
        verify(connection, times(3)).sendMessage(argCap.capture()); // ready, error, ready
        verify(connection).reset();
        verify(connection, never()).close();

        List<SqpMessage> capturedMessages = argCap.getAllValues();
        assertThat(capturedMessages.get(0).getType(), is(MessageType.ReadyMessage));
        assertThat(capturedMessages.get(1).getType(), is(MessageType.ErrorMessage));
        assertThat(capturedMessages.get(2).getType(), is(MessageType.ReadyMessage));
    }

    @Test
    public void otherMessageWithoutHelloCausesAbortError() throws Exception {
        session.processMessage(new SimpleQueryMessage("foo", false, null, -1));

        assertThat(session.getState(), is(ClientSessionState.Dead));
        verify(connection).sendMessage(any(ErrorMessage.class)); // ready, error, ready
        verify(connection).close();
    }

    @Test
    public void twoHelloMessagesCauseRecoverableError() throws Exception {
        sendHelloMessage(session);
        sendHelloMessage(session);

        assertThat(session.getState(), is(ClientSessionState.Ready));
        verify(connection, times(3)).sendMessage(any(SqpMessage.class)); // ready, error, ready
        verify(connection).reset();
    }

    @Test
    public void sendingReadyMessageToServerFails() throws Exception {
        sendHelloMessage(session);
        session.processMessage(new ReadyMessage());

        assertThat(session.getState(), is(ClientSessionState.Ready));
        verify(connection, times(3)).sendMessage(any(SqpMessage.class)); // ready, error, ready
        verify(connection).reset();
    }

    private void sendHelloMessage(ClientSession session) {
        sendHelloMessage(session, DataFormat.Text);
    }

    private void sendHelloMessage(ClientSession session, DataFormat format) {
        BackendConnection backendConnection = mock(BackendConnection.class);
        when(backendPool.createConnection(eq("test"), anyObject(), anyObject())).then(invocation -> {
            ((ResultHandler<BackendConnection>) invocation.getArguments()[2]).handle(backendConnection);
            return 0l;
        });
        HelloMessage msg = new HelloMessage("test");
        msg.setMessageFormat(format);
        session.processMessage(msg);
    }

}
