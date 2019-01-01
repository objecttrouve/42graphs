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

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.objecttrouve.testing.matchers.fluentatts.FluentAttributeMatcher;

public abstract class AbstractMatcherBuilder<M> implements Matcher<M> {

    protected final FluentAttributeMatcher<M> matcher;

    protected AbstractMatcherBuilder(final FluentAttributeMatcher<M> matcher) {
        this.matcher = matcher;
    }



    @Override
    public boolean matches(final Object item) {
        return matcher.matches(item);
    }

    @Override
    public void describeMismatch(final Object item, final Description mismatchDescription) {
        matcher.describeMismatch(item, mismatchDescription);
    }

    @Override
    public void _dont_implement_Matcher___instead_extend_BaseMatcher_() {
        throw new UnsupportedOperationException("Don't call this!");
    }

    @Override
    public void describeTo(final Description description) {
        matcher.describeTo(description);
    }




}
