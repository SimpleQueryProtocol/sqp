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

package io.sqp.proxy.util;

import io.sqp.core.ErrorAction;
import io.sqp.core.ErrorType;
import io.sqp.core.exceptions.SqpException;

import java.io.IOException;
import java.io.InputStream;
import java.util.MissingResourceException;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Stefan Burnicki
 */
public class ResourceUtil {
    private ResourceUtil() {}

    public static String readResourceAsString(String resourceName, Logger logger) throws SqpException {
        return readResourceAsString(resourceName, "UTF-8", logger);
    }

    public static String readResourceAsString(String resourceName, String charset, Logger logger) throws SqpException {
        InputStream resourceStream = ClassLoader.getSystemClassLoader().getResourceAsStream(resourceName);
        if (resourceStream == null) {
            SqpException error = new SqpException(ErrorType.ServerError,
                    "The resource file couldn't be found on the server. This is likely to be a server issue.",
                    ErrorAction.Recover, new MissingResourceException("Resource '" + resourceName + "' does not exist.",
                    "schema", resourceName));
            throw error;
        }
        // This uses a "Scanner trick, since \A means "beginning of input boundary". Also see here:
        // http://stackoverflow.com/questions/309424/read-convert-an-inputstream-to-a-string
        try (Scanner s = new java.util.Scanner(resourceStream, charset).useDelimiter("\\A")) {
            return s.hasNext() ? s.next() : "";
        } finally {
            try {
                resourceStream.close();
            } catch (IOException e) {
                logger.log(Level.WARNING, "Could not close resource stream for resource '" + resourceName + "'");
            }
        }
    }
}
