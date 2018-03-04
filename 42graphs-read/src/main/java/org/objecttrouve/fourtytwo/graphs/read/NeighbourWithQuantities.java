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

package org.objecttrouve.fourtytwo.graphs.read;

import com.google.common.collect.ImmutableMap;
import org.objecttrouve.fourtytwo.graphs.api.*;

import java.util.Map;
import java.util.Set;

class NeighbourWithQuantities<T> implements AggregatedNeighbour<T> {

    private  final Value<T> self;
    private  final Value<T> neighbour;
    private  final Map<QuantityMetric, Number> metrics;

    NeighbourWithQuantities(final Value<T> self, final Value<T> neighbour, final Map<QuantityMetric, Number> metrics) {
        this.self = self;
        this.neighbour = neighbour;
        this.metrics = ImmutableMap.copyOf(metrics);
    }

    @Override
    public Value<T> getSelf() {
        return self;
    }

    @Override
    public Value<T> getNeighbour() {
        return neighbour;
    }

    @Override
    public Set<QuantityMetric> getAvailableQuantities() {
        return metrics.keySet();
    }

    @Override
    public Map<QuantityMetric, Number> getQuantities() {
        return metrics;
    }
}
