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

import org.objecttrouve.fourtytwo.graphs.api.SequenceTree;
import org.objecttrouve.fourtytwo.graphs.api.Value;
import org.objecttrouve.testing.matchers.fluentatts.Attribute;
import org.objecttrouve.testing.matchers.fluentatts.FluentAttributeMatcher;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.objecttrouve.testing.matchers.ConvenientMatchers.a;
import static org.objecttrouve.testing.matchers.fluentatts.Attribute.attribute;

public class SequenceTreeMatcher<R, V> extends AbstractMatcherBuilder<SequenceTree<R, V>> {


    private final Attribute<SequenceTree<R, V>, Object> identifier = attribute("identifier", st -> st.getRoot().getIdentifier());
    private final Attribute<SequenceTree<R, V>, String> parentDimensionName = attribute("root dimension name", st -> st.getParentDimension().getName());
    private final Attribute<SequenceTree<R, V>, String> childDimensionName = attribute("leaf dimension name", st -> st.getChildDimension().getName());
    private final Attribute<SequenceTree<R, V>, List<Object>> leaves = attribute("leaves", st -> st.getValues().stream().map(Value::getIdentifier).collect(toList()));


    @SuppressWarnings("unused")
    public static <R, V> SequenceTreeMatcher<R, V> aSequenceTree(final Class<R> rClass, final Class<V> vClass){
        //noinspection unchecked
        final FluentAttributeMatcher<SequenceTree<R,V>> m = (FluentAttributeMatcher) a(SequenceTree.class);
        return new SequenceTreeMatcher<>(m);
    }

    private SequenceTreeMatcher(final FluentAttributeMatcher<SequenceTree<R, V>> matcher) {
        super(matcher);
    }

    public SequenceTreeMatcher withRootId(final V root) {
        matcher.with(identifier, root);
        return this;
    }

    public SequenceTreeMatcher withParentDimension(final String parentDimension) {
        matcher.with(parentDimensionName, parentDimension);
        return this;
    }

    public SequenceTreeMatcher withChildDimension(final String childDimension) {
        matcher.with(childDimensionName, childDimension);
        return this;
    }

    @SafeVarargs
    public final SequenceTreeMatcher withLeaves(final V... values) {
        matcher.with(leaves, asList(values));
        return this;
    }
}
