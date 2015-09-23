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

package io.sqp.transbase;

import io.sqp.core.exceptions.BackendErrorException;
import io.sqp.core.types.SqpCustom;
import io.sqp.core.types.SqpValue;
import transbase.tbx.TBConst;
import transbase.tbx.types.TBDatetime;
import transbase.tbx.types.helpers.TBObject;
import transbase.tbx.types.helpers.TSpec;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Stefan Burnicki
 */
public class NativeResultConverter {
    Map<DateRangeSpecifier, Set<DateRangeSpecifier>> _allowedDateTimes;

    public NativeResultConverter() {
        // we currently only support ranged datetime values as native types
        _allowedDateTimes = new HashMap<>();
    }

    public void addAllowedTypes(List<String> allowedTypes) {
        for (String type : allowedTypes) {
            CustomType customType = TBTypeRepository.getCustomType(type);
            try {
                registerAllowedType(customType, type);
            } catch (IllegalArgumentException e) {
                // TODO: send some warning
            }
        }
    }

    private void registerAllowedType(CustomType customType, String typeName) {
        switch (customType) {
            case DateTime:
                DateRangeSpecifier[] specifiers = TBTypeRepository.parseRangeSpecifiers(typeName);
                registerDateTimeType(specifiers[0], specifiers[1]);

            default:
                throw new IllegalArgumentException("The type is unknown");
        }
    }

    private void registerDateTimeType(DateRangeSpecifier highField, DateRangeSpecifier lowField) {
        Set<DateRangeSpecifier> allowedHigh = _allowedDateTimes.get(highField);
        if (allowedHigh == null) {
            allowedHigh = new HashSet<>();
        }
        allowedHigh.add(lowField);
        _allowedDateTimes.put(highField, allowedHigh);
    }

    public boolean canMapToNativeType(TSpec tSpec) {
        switch (tSpec.getType()) {
            case TBConst.TB__DATETIME:
                return isDateTimeAllowed(DateRangeSpecifier.fromTbValue(tSpec.getHighf()),
                        DateRangeSpecifier.fromTbValue(tSpec.getLowf()));
        }
        return false;
    }

    private boolean isDateTimeAllowed(DateRangeSpecifier high, DateRangeSpecifier low) {
        Set<DateRangeSpecifier> allowedHigh = _allowedDateTimes.get(high);
        return allowedHigh != null && allowedHigh.contains(low);
    }

    public SqpValue mapNative(TBObject value) throws BackendErrorException {
        if (value instanceof TBDatetime) {
            return mapDateTime((TBDatetime) value);
        }
        throw new BackendErrorException("Native format of " + value.getClass() + " requested, but not supported");
    }

    private SqpValue mapDateTime(TBDatetime value) {
        DateRangeSpecifier highField = DateRangeSpecifier.fromTbValue(value.getHighField());
        DateRangeSpecifier lowField = DateRangeSpecifier.fromTbValue(value.getLowField());
        List<Integer> fieldValues = DateRangeSpecifier.range(highField, lowField).stream()
                .map(s -> value.getField(s.getTbValue()))
                .collect(Collectors.toList());
        return new SqpCustom(fieldValues);
    }
}
