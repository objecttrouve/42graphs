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

import com.google.common.collect.Sets;
import org.objecttrouve.fourtytwo.graphs.api.AggregatedValue;
import org.objecttrouve.testing.matchers.fluentatts.Attribute;
import org.objecttrouve.testing.matchers.fluentatts.FluentAttributeMatcher;

import java.util.Set;
import java.util.stream.IntStream;

import static org.objecttrouve.fourtytwo.graphs.api.ValueQuantityMetric.count;
import static org.objecttrouve.testing.matchers.ConvenientMatchers.an;
import static org.objecttrouve.testing.matchers.fluentatts.Attribute.attribute;

public class AggregatedValueMatcher extends AbstractMatcherBuilder<AggregatedValue> {

    private static final Attribute<AggregatedValue, Object> identifier = attribute("identifier", AggregatedValue::getIdentifier);
    private static final Attribute<AggregatedValue, Long> attrCount = attribute("attrCount", v -> count.get(v.getQuantities(), Long.class));
    private static final int maxExpectedPositions = 20;

    public static AggregatedValueMatcher anAggrValue() {
        return new AggregatedValueMatcher(an(AggregatedValue.class));
    }

    private AggregatedValueMatcher(final FluentAttributeMatcher<AggregatedValue> matcher) {
        super(matcher);
    }

    public AggregatedValueMatcher withCountInPosition(final int position, final long count) {
        //noinspection unchecked
        super.matcher.with(attribute("attrCount at position " + position, v -> v.getCountsPerPosition().getOrDefault(position, 0))
            , count);
        return this;
    }

    public AggregatedValueMatcher occurringOnlyAt(final Integer... positions) {
        final Set<Integer> skip = Sets.newHashSet(positions);
        IntStream.range(0, maxExpectedPositions).forEach(i -> {
            if (!skip.contains(i)) {
                //noinspection unchecked
                super.matcher.with(attribute("(absent) attrCount at position " + i, v -> v.getCountsPerPosition().getOrDefault(i, 0))
                    , 0);
            }
        });
        return this;
    }


    public AggregatedValueMatcher withIdentifier(final String id) {
        super.matcher.with(identifier, id);
        return this;
    }

    public AggregatedValueMatcher withCount(final Long c) {
        super.matcher.with(attrCount, c);
        return this;
    }
}
