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

import io.sqp.core.messages.InformationRequestMessage;
import io.sqp.core.messages.SetFeatureMessage;

/**
 * Valid subjects for information requests.
 * @author Stefan Burnicki
 * @see InformationRequestMessage
 */
public enum InformationSubject {
    /**
     * If the server supports the binary protocol.
     */
    SupportsBinaryProtocol,

    /**
     * The native types supported by the current backend.
     * @see SetFeatureMessage#setAllowedNativeTypes
     */
    SupportedNativeTypes,

    /**
     * The name of the database management system that is in use.
     */
    DBMSName,

    /**
     * The maximum precision of a data type. In an {@link InformationRequestMessage}
     * the affected data type needs to be passed as the "detail".
     */
    MaxPrecision,

    /**
     * The maximum scale of a data type. In an {@link InformationRequestMessage}
     * the affected data type needs to be passed as the "detail".
     */
    MaxScale,

    /**
     * The JSON schema of a data type. In an {@link InformationRequestMessage}
     * the affected data type needs to be passed as the "detail".
     */
    TypeSchema
}
