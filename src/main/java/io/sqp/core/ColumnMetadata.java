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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import io.sqp.core.messages.SetFeatureMessage;
import io.sqp.core.types.SqpSmallInt;
import io.sqp.core.types.SqpTypeCode;

/**
 * Metadata representing one column of a database table.
 * Can be used for encoding/decoding via Jackson
 * @author Stefan Burnicki
 */
public class ColumnMetadata {
    private String _name;
    private TypeDescription _type;
    private String _nativeType;

    /**
     * Same as {@link ColumnMetadata#ColumnMetadata(String, TypeDescription, String)}, but with separate values
     * for type code, precision, and scale. Used for Jackson's object creation.
     * @param name Name of the column
     * @param type Type code of the column's type
     * @param nativeType Native type name of the column, i.e. depending on the actual DBMS in use.
     * @param precision Precision of the column
     * @param scale Scale of the column
     */
    @JsonCreator
    public ColumnMetadata(
            @JsonProperty("name") String name,
            @JsonProperty("type") SqpTypeCode type,
            @JsonProperty("nativeType") String nativeType,
            @JsonProperty("precision") Integer precision,
            @JsonProperty("scale") Integer scale) {
        this(name, new TypeDescription(type, precision, scale), nativeType);
    }

    /**
     * Constructs column metadata.
     * @param name The name of the column
     * @param type The type description with SQP type code, prcision, and scale
     * @param nativeType The native type name, i.e. depending on the actual DBMS in use.
     */
    public ColumnMetadata(String name, TypeDescription type, String nativeType) {
        _name = name;
        _type = type;
        _nativeType = nativeType;
    }

    /**
     * Returns the name of the column.
     * @return Name of the column
     */
    public String getName() {
        return _name;
    }

    /**
     * TypeDescription of the column's type. Gets unwrapped when used by a Jackson object encoder.
     * @return The column's type description
     */
    @JsonUnwrapped
    public TypeDescription getType() {
        return _type;
    }

    /**
     * The native type name of the column.
     * SQP can be used to "wrap" another database which has its own, native types.
     * E.g. PostgreSQL calls the {@link SqpSmallInt} "int2" and when the proxy uses a PostgreSQL
     * backend, this name would be wrapped as "pg_int2". This field can be used to uniquely identify an underlying type,
     * e.g. when using {@link SetFeatureMessage}.
     * @return The native type name
     */
    public String getNativeType() {
        return _nativeType;
    }
}
