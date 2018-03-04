/*
 * MIT License
 *
 * Copyright (c) 2018 objecttrouve.org <un.object.trouve@gmail.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.objecttrouce.fourtytwo.graphs.aggregations.logic;

import com.google.common.collect.Maps;
import org.objecttrouce.fourtytwo.graphs.aggregations.api.Aggregator;
import org.objecttrouce.fourtytwo.graphs.aggregations.api.NeighbourWithCount;
import org.objecttrouce.fourtytwo.graphs.aggregations.api.ValueWithAggregatedPositions;
import org.objecttrouce.fourtytwo.graphs.aggregations.api.ValueWithPosition;
import org.objecttrouve.fourtytwo.graphs.api.Value;

import java.util.Map;
import java.util.stream.Stream;

class Aggr implements Aggregator{

    @Override
    public <T> Stream<ValueWithAggregatedPositions<T>> aggregate(final Stream<ValueWithPosition<T>> values){
        final Map<T, Map<Integer, Long>> positionCountsByValue = Maps.newHashMap();
        values.forEach(nwp -> {
            final T val = nwp.getValue().getIdentifier();
            final Map<Integer, Long> positionCounts = positionCountsByValue.computeIfAbsent(val, k -> Maps.newHashMap());
            final int position = nwp.getPosition();
            final Long positionCount = positionCounts.getOrDefault(position, 0L);
            positionCounts.put(position, positionCount + 1);

        });
        //noinspection unchecked
        return positionCountsByValue.entrySet().stream().map(e -> {
            //noinspection unchecked
            final Map<Integer, Long> value = e.getValue();
            //noinspection unchecked
            return new AggrPosValue<>(e.getKey(), value.values().stream().mapToLong(v -> v).sum(), value);
        });
    }

    @Override
    public <T> Stream<NeighbourWithCount<T>> countNeighbours(final Stream<Value<T>> neighbours, final Value<T> self, final int vicinity){
        final Map<Object, NeighbourCounter> aggrNeighbours = Maps.newHashMap();
        neighbours.forEach(n -> {
            //noinspection unchecked
            aggrNeighbours.computeIfAbsent(n.getIdentifier(), k -> new NeighbourCounter<T>(self, n, vicinity)).count();
        });
        //noinspection unchecked
        return aggrNeighbours.values().stream().map(NeighbourCounter::build);
    }


}
