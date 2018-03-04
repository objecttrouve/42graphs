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

package org.objecttrouve.fourtytwo.graphs.matchers;


import org.objecttrouve.fourtytwo.graphs.api.AggregatedNeighbour;
import org.objecttrouve.fourtytwo.graphs.api.NeighbourQuantityMetric;
import org.objecttrouve.testing.matchers.fluentatts.Attribute;
import org.objecttrouve.testing.matchers.fluentatts.FluentAttributeMatcher;

import static org.objecttrouve.testing.matchers.ConvenientMatchers.an;
import static org.objecttrouve.testing.matchers.fluentatts.Attribute.attribute;

public class AggregatedNeighbourMatcher extends AbstractMatcherBuilder<AggregatedNeighbour> {

    private static final Attribute<AggregatedNeighbour, Object> selfIdVal = attribute("identifier", n -> n.getSelf().getIdentifier());
    private static final Attribute<AggregatedNeighbour, Object> neighbourIdVal = attribute("identifier", n -> n.getNeighbour().getIdentifier());
    private static final Attribute<AggregatedNeighbour, Integer> vicinity = attribute("vicinity", n -> NeighbourQuantityMetric.vicinity.get(n.getQuantities(), Integer.class));
    private static final Attribute<AggregatedNeighbour, Long> count = attribute("count", n -> NeighbourQuantityMetric.count.get(n.getQuantities(), Long.class));

    public static AggregatedNeighbourMatcher anAggregatedNeighbour() {
        return new AggregatedNeighbourMatcher(an(AggregatedNeighbour.class));
    }

    private AggregatedNeighbourMatcher(final FluentAttributeMatcher<AggregatedNeighbour> matcher) {
        super(matcher);
    }

    public AggregatedNeighbourMatcher withSelfId(final String id) {
        super.matcher.with(selfIdVal, id);
        return this;
    }

    public AggregatedNeighbourMatcher withNeighbourId(final String id) {
        super.matcher.with(neighbourIdVal, id);
        return this;
    }

    public AggregatedNeighbourMatcher withVicinity(final Integer v) {
        super.matcher.with(vicinity, v);
        return this;
    }

    public AggregatedNeighbourMatcher withCount(final Long c) {
        super.matcher.with(count, c);
        return this;
    }
}
