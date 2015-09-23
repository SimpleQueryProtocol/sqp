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

package io.sqp.core;

import io.sqp.core.messages.ExecuteQueryMessage;
import io.sqp.core.messages.FetchDataMessage;
import io.sqp.core.messages.PrepareQueryMessage;
import io.sqp.core.messages.SimpleQueryMessage;

/**
 * Some default values that are used for example on message decoding to have valid values.
 * @author StefanBurnicki
 */
public class Defaults {
    /**
     * Default ID used for cursors.
     * @see ExecuteQueryMessage
     * @see FetchDataMessage
     * @see SimpleQueryMessage
     */
    public static final String DefaultCursorID = "Default";

    /**
     * Default ID used for statements.
     * @see PrepareQueryMessage
     * @see ExecuteQueryMessage
     */
    public static final String DefaultStatementID = "Default";

    private Defaults() {}
}
