/*
 * MIT License
 *
 * Copyright (c) 2019 objecttrouve.org <un.object.trouve@gmail.com>
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

import org.objecttrouve.fourtytwo.graphs.api.Value;
import org.objecttrouve.testing.matchers.fluentatts.FluentAttributeMatcher;

import static org.objecttrouve.testing.matchers.ConvenientMatchers.a;
import static org.objecttrouve.testing.matchers.fluentatts.Attribute.attribute;

public class ValueMatcher<T> extends AbstractMatcherBuilder<Value<T>> {

    public static ValueMatcher<String> aStringValue() {
        //noinspection unchecked
        return new ValueMatcher(a(Value.class), String.class);
    }

    public static <T> ValueMatcher aValueAs(final Class<T> tClass) {
        //noinspection unchecked
        return new ValueMatcher(a(Value.class), tClass);
    }

    private ValueMatcher(final FluentAttributeMatcher<Value<T>> matcher, final Class<T> tClass) {
        super(matcher);
        this.matcher.with(attribute("class", v -> v.getIdentifier().getClass()), tClass);
    }

    public ValueMatcher<T> withIdentifier(final T identifyingValue) {
        super.matcher.with(attribute("identifier", Value::getIdentifier), identifyingValue);
        return this;
    }
}
