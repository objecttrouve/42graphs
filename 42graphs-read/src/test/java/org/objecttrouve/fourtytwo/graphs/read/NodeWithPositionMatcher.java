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

import org.objecttrouce.fourtytwo.graphs.aggregations.api.ValueWithPosition;
import org.objecttrouve.fourtytwo.graphs.matchers.AbstractMatcherBuilder;
import org.objecttrouve.testing.matchers.fluentatts.Attribute;
import org.objecttrouve.testing.matchers.fluentatts.FluentAttributeMatcher;

import static org.objecttrouve.testing.matchers.ConvenientMatchers.a;
import static org.objecttrouve.testing.matchers.fluentatts.Attribute.attribute;

class ValueWithPositionMatcher<T> extends AbstractMatcherBuilder<ValueWithPosition<T>> {

    static <T> ValueWithPositionMatcher aVwp(final Class<T> tClass) {
        //noinspection unchecked
        return new ValueWithPositionMatcher(a(ValueWithPosition.class), tClass);
    }

    private ValueWithPositionMatcher(final FluentAttributeMatcher<ValueWithPosition<T>> matcher, final Class<T> tClass) {
        super(matcher);
        super.matcher.with(attribute("class", n -> n.getValue().getIdentifier().getClass()), tClass);

    }

    ValueWithPositionMatcher withIdentifier(final Object t) {
        super.matcher.with(attribute("identifier", n -> n.getValue().getIdentifier()), t);
        return this;
    }

    ValueWithPositionMatcher withPosition(final int position) {
        super.matcher.with(attribute("position", ValueWithPosition::getPosition), position);
        return this;
    }
}
