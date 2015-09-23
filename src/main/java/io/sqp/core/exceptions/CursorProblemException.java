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

package io.sqp.core.exceptions;

import io.sqp.core.ErrorType;
import io.sqp.core.ErrorAction;

/**
 * @author Stefan Burnicki
 */
public class CursorProblemException extends SqpException {
    public enum Problem {
        NotScrollable("is not scrollable"),
        Closed("is already closed"),
        NotPositionable("could not be positioned"),
        FetchDirectionFailed("could not be set to the correct fetch direction"),
        DoesNotExist("does not exist");

        private String _reason;

        Problem(String reason) {
            _reason = reason;
        }

        public String getReason() {
            return _reason;
        }
    }

    private Problem _problem;

    public CursorProblemException(String id, Problem problem, Throwable cause) {
        super(ErrorType.CursorProblem, "The cursor " + id + " " + problem.getReason(), ErrorAction.Recover, cause);
        _problem = problem;

    }

    public CursorProblemException(String id, Problem problem) {
        this(id, problem, null);
    }

    public Problem getProblem() {
        return _problem;
    }
}
