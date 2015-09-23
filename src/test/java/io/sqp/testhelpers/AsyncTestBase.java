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

package io.sqp.testhelpers;

import org.testng.Assert;
import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import io.sqp.backend.SuccessHandler;
import io.sqp.core.exceptions.SqpException;
import io.sqp.backend.ErrorHandler;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author Stefan Burnicki
 * Utility to wait for some threads to complete. Taken from the vertx project:
 * https://github.com/eclipse/vert.x/blob/master/src/test/java/io/vertx/test/core/AsyncTestBase.java
 */
public class AsyncTestBase implements ErrorHandler {
    private CountDownLatch latch;
    private volatile boolean testCompleteCalled;
    private volatile boolean awaitCalled;
    private volatile boolean tearingDown;

    @BeforeMethod
    public synchronized void setUp() {
        tearingDown = false;
        waitFor(1);
        testCompleteCalled = false;
        awaitCalled = false;
    }

    @AfterMethod
    public synchronized void tearDown(ITestResult result) {
        tearingDown = true;
        if (result.isSuccess() && (!awaitCalled || !testCompleteCalled)) {
            throw new IllegalStateException("await() or complete() wasn't called");
        }
    }

    protected synchronized void waitFor(int count) {
        latch = new CountDownLatch(count);
    }

    protected synchronized void fail() {
        fail(null);
    }

    protected synchronized void fail(String msg) {
        complete();
        Assert.fail(msg);
    }

    protected synchronized void complete() {
        if (tearingDown) {
            throw new IllegalStateException("testComplete called after test has completed");
        }
        if (testCompleteCalled) {
            throw new IllegalStateException("already complete");
        }
        latch.countDown();
        if (latch.getCount() == 0) {
            testCompleteCalled = true;
        }
    }

    protected void testComplete() {
        if (tearingDown) {
            throw new IllegalStateException("testComplete called after test has completed");
        }
        if (testCompleteCalled) {
            throw new IllegalStateException("testComplete() already called");
        }
        testCompleteCalled = true;
        latch.countDown();
    }

    protected void await() {
        await(1, TimeUnit.MINUTES);
    }

    public void await(long delay, TimeUnit timeUnit) {
        if (awaitCalled) {
            throw new IllegalStateException("await() already called");
        }
        awaitCalled = true;
        try {
            boolean ok = latch.await(delay, timeUnit);
            if (!ok) {
                // timed out
                throw new IllegalStateException("Timed out in waiting for test complete");
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException("Test thread was interrupted!");
        }
    }

    @Override
    public void handleError(SqpException error) {
        fail(error.getMessage());
    }

    protected SuccessHandler completionHandler() {
        return new SuccessHandler(this, this::complete);
    }
}
