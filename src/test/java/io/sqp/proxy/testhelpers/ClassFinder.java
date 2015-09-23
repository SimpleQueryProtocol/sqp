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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * Taken from http://stackoverflow.com/questions/15519626/how-to-get-all-classes-names-in-a-package
 * Modified by Stefan Burnicki <stefan.burnicki@burnicki.net>.
 * Used to check the message classes to be consistent with the registered MessageTypes
 */
public class ClassFinder {
    private static final char DOT = '.';
    private static final char SLASH = '/';
    private static final String CLASS_SUFFIX = ".class";
    private static final String BAD_PACKAGE_ERROR = "Unable to get resources from path '%s'. Are you sure the package '%s' exists?";

    public static <T> List<Class<? extends T>> find(String scannedPackage, Class<T> superClass) {
        String scannedPath = scannedPackage.replace(DOT, SLASH);
        Enumeration<URL> scannedUrls = null;
        try {
            scannedUrls = Thread.currentThread().getContextClassLoader().getResources(scannedPath);
        } catch (IOException e) {
            throw new IllegalArgumentException(String.format(BAD_PACKAGE_ERROR, scannedPath, scannedPackage));
        }
        if (!scannedUrls.hasMoreElements()) {
            throw new IllegalArgumentException(String.format(BAD_PACKAGE_ERROR, scannedPath, scannedPackage));
        }
        List<Class<? extends T>> classes = new ArrayList<>();
        while (scannedUrls.hasMoreElements()) {
            URL scannedUrl = scannedUrls.nextElement();
            File scannedDir = new File(scannedUrl.getFile());
            for (File file : scannedDir.listFiles()) {
                classes.addAll(find(file, scannedPackage, superClass));
            }
        }
        return classes;
    }

    private static <T> List<Class<? extends T>> find(File file, String scannedPackage, Class<T> superClass) {
        List<Class<? extends T>> classes = new ArrayList<>();
        String resource = scannedPackage + DOT + file.getName();
        if (file.isDirectory()) {
            for (File child : file.listFiles()) {
                classes.addAll(find(child, resource, superClass));
            }
        } else if (resource.endsWith(CLASS_SUFFIX)) {
            int endIndex = resource.length() - CLASS_SUFFIX.length();
            String className = resource.substring(0, endIndex);
            try {
                Class<?> clazz = Class.forName(className);
                try {
                    Class<? extends T> subclass = clazz.asSubclass(superClass);
                    if (!clazz.equals(superClass)) {
                        classes.add(subclass);
                    }
                } catch (ClassCastException e) {
                    // just ignore classes other than we need
                }
            } catch (ClassNotFoundException ignore) {
            }
        }
        return classes;
    }

}
