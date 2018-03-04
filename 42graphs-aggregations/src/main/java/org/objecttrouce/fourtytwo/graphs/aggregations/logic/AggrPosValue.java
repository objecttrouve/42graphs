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

import org.objecttrouce.fourtytwo.graphs.aggregations.api.ValueWithAggregatedPositions;

import java.util.Map;

import static com.google.common.collect.ImmutableMap.copyOf;

class AggrPosValue<T> implements ValueWithAggregatedPositions{

    private final T t;
    private final long count;
    private final Map<Integer, Long> countsPerPosition;

    AggrPosValue(final T t, final long count, final Map<Integer, Long> countsPerPosition) {
        this.t = t;
        this.count = count;
        this.countsPerPosition = copyOf(countsPerPosition);
    }

    @Override
    public T getIdentifier() {
        return t;
    }

    @Override
    public long getCount() {
        return count;
    }

    @Override
    public Map<Integer, Long> getCountsPerPosition() {
        return countsPerPosition;
    }

    @Override
    public String toString() {
        return "AggrPosValue{" +
            "t=" + t +
            ", count=" + count +
            ", countsPerPosition=" + countsPerPosition +
            '}';
    }
}
