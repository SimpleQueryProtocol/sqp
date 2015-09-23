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

package io.sqp.client.impl;

import io.sqp.core.types.*;
import io.sqp.client.PreparedStatement;
import io.sqp.client.QueryResult;
import io.sqp.core.exceptions.SqpException;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Stefan Burnicki
 */
// TODO: we could somehow add some simple SQL parsing that finds placeholders ("?") this way we could verify
// the number of parameters on the client side
public class PreparedStatementImpl extends CloseableServerResource implements PreparedStatement {
    private CompletableFuture<?> _startFuture;
    private Map<Integer, SqpTypeCode> _parameterTypes;
    private Map<Integer, String> _customTypes;
    private List<List<SqpValue>> _parameterBatches;
    private Map<Integer, SqpValue> _currentParameterBatch;
    private LobManager _lobManager;

    public PreparedStatementImpl(SqpConnectionImpl connection, LobManager lobManager, String id, CompletableFuture startFuture) {
        super(connection, id, "prepared statement");
        _startFuture = startFuture;
        _parameterTypes = new HashMap<>();
        _customTypes = new HashMap<>();
        _parameterBatches = new LinkedList<>();
        _currentParameterBatch = new HashMap<>();
        _lobManager = lobManager;
    }

    @Override
    public <T extends QueryResult> CompletableFuture<T> execute(Class<T> resultClass, boolean scrollable) {
        try {
            validateOpen();
        } catch (SqpException e) {
            CompletableFuture<T> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(e);
            return failedFuture;
        }

        List<SqpTypeCode> parameterTypes = mapWithPositionsToList(_parameterTypes, SqpTypeCode.Unknown);

        if (_currentParameterBatch.size() > 0) {
            addBatch();
        }
        List<List<Object>> jsonFormatParameters = _parameterBatches.stream().map(
                paramList -> paramList.stream().map(SqpValue::getJsonFormatValue).collect(Collectors.toList())
        ).collect(Collectors.toList());
        List<String> customTypes = _customTypes.entrySet().stream()
                .sorted((left, right) -> Integer.compare(left.getKey(), right.getKey()))
                .map(Map.Entry::getValue).collect(Collectors.toList());

        _parameterBatches.clear();
        _parameterTypes.clear();
        _customTypes.clear();


        // compose with start future in order to throw previous errors first
        // TODO: this future should be created in thenCompose, but then it's maybe then another message is sent before
        // the execution messages, which must be avoided. Maybe the connection should depend on other futures to send
        // new messages
        CompletableFuture<T> future = getConnection().execute(resultClass, getId(), parameterTypes, customTypes, jsonFormatParameters, scrollable);
        return _startFuture.thenCompose(v -> future);
    }

    @Override
    public PreparedStatement addBatch() {
        List<SqpValue> parameterList = mapWithPositionsToList(_currentParameterBatch, new SqpNull(SqpTypeCode.Unknown));
        _parameterBatches.add(parameterList);
        _currentParameterBatch.clear();
        return this;
    }

    @Override
    public PreparedStatement bind(int param, int value) throws SqpException {
        return bind(param, new SqpInteger(value));
    }

    @Override
    public PreparedStatement bind(int param, long value) throws SqpException {
        return bind(param, new SqpBigInt(value));
    }

    @Override
    public PreparedStatement bind(int param, BigDecimal value) throws SqpException {
        return bind(param, new SqpDecimal(value));
    }

    @Override
    public PreparedStatement bind(int param, float value) throws SqpException {
        return bind(param, new SqpReal(value));
    }

    @Override
    public PreparedStatement bind(int param, double value) throws SqpException {
        return bind(param, new SqpDouble(value));
    }

    @Override
    public PreparedStatement bind(int param, LocalDate value) throws SqpException {
        return bind(param, new SqpDate(value));
    }

    @Override
    public PreparedStatement bind(int param, OffsetTime value) throws SqpException {
        return bind(param, new SqpTime(value));
    }

    @Override
    public PreparedStatement bind(int param, LocalTime value) throws SqpException {
        return bind(param, new SqpTime(value));
    }

    @Override
    public PreparedStatement bind(int param, OffsetDateTime value) throws SqpException {
        return bind(param, new SqpTimestamp(value));
    }

    @Override
    public PreparedStatement bind(int param, String value) throws SqpException {
        return bind(param, new SqpVarChar(value));
    }

    @Override
    public PreparedStatement bind(int param, byte[] bytes) throws SqpException {
        return bind(param, new SqpBinary(bytes));
    }

    @Override
    public PreparedStatement bind(int param, InputStream binStream) throws SqpException {
        String id = _lobManager.createBlobId();
        bind(param, new SqpBlob(id, -1));
        CompletableFuture<Void> future = _lobManager.create(id, binStream);
        // reflect success in start future which is used in execution
        _startFuture = CompletableFuture.allOf(_startFuture, future);
        return this;
    }

    @Override
    public PreparedStatement bind(int param, Reader charStream) throws SqpException {
        String id = _lobManager.createClobId();
        bind(param, new SqpClob(id, -1));
        CompletableFuture<Void> future = _lobManager.create(id, charStream);
        // reflect success in start future which is used in execution
        _startFuture = CompletableFuture.allOf(_startFuture, future);
        return this;
    }

    @Override
    public PreparedStatement bind(int param, SqpValue value) throws SqpException {
        if (_currentParameterBatch.containsKey(param)) {
            throw new IllegalArgumentException("Parameter " + param + " is already bound in the current batch.");
        }
        if (!checkParameterType(param, value)) {
            setParameterType(param, value);
        }
        _currentParameterBatch.put(param, value);
        return this;
    }

    @Override
    public PreparedStatement bind(int param, String customTypeName, Object value) throws SqpException {
        return bind(param, new SqpCustom(value, customTypeName));
    }

    @Override
    public PreparedStatement bindNull(int param, SqpTypeCode type) throws SqpException {
        return bind(param, new SqpNull(type));
    }

    private boolean checkParameterType(int param, SqpValue value) {
        SqpTypeCode paramType = value.getType();
        SqpTypeCode currentType = _parameterTypes.get(param);
        if (currentType == null) {
            return false;
        }
        // TODO: check for compatibility, not for exact fit
        if (!currentType.equals(paramType)) {
            throw new IllegalArgumentException("The parameter type '" + paramType +
                    "' is not compatible with the type '" + currentType +  "' which was used before.");
        }
        if (paramType == SqpTypeCode.Custom) {
            String customType = _customTypes.get(param);
            if (customType == null) { // well this shouldn't happen, but let's be prepared
                return false; // so it gets set
            }
            String newCustomType = ((SqpCustom) value).getCustomTypeName(); // safe as we checked the type code
            if (!customType.equals(newCustomType)) {
                throw new IllegalArgumentException("The parameter is already bound to custom parameter type '" +
                        customType + "', cannot bind to custom type '" + customType + "'");
            }
        }
        return true;
    }

    public void setParameterType(int param, SqpValue value) {
        SqpTypeCode paramType = value.getType();
        if (paramType == SqpTypeCode.Custom) {
            _customTypes.put(param, ((SqpCustom) value).getCustomTypeName());
        }
        _parameterTypes.put(param, paramType);
    }

    private <T> List<T> mapWithPositionsToList(Map<Integer, T> map, T defaultValue) {
        if (map.isEmpty()) {
            return new ArrayList<>(0);
        }
        int maxIdx = map.keySet().stream().max(Integer::compare).get();
        return IntStream.range(0, maxIdx + 1).mapToObj(
                i -> map.containsKey(i) ? map.get(i) : defaultValue
        ).collect(Collectors.toList());
    }
}
