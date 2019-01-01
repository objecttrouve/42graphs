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

import org.neo4j.driver.v1.types.Node;
import org.objecttrouve.testing.matchers.ConvenientMatchers;
import org.objecttrouve.testing.matchers.fluentatts.Attribute;
import org.objecttrouve.testing.matchers.fluentatts.FluentAttributeMatcher;

import static org.objecttrouve.fourtytwo.graphs.api.Value.idKey;

public class NeoNode2Matcher extends AbstractMatcherBuilder<org.neo4j.driver.v1.types.Node> {

    private static final Attribute<Node, String> idVal = Attribute.attribute("identifier", n -> n.get(idKey).asString());

    public static NeoNode2Matcher aNode() {
        final FluentAttributeMatcher<Node> m = ConvenientMatchers.a(Node.class);
        return new NeoNode2Matcher(m);
    }

    private NeoNode2Matcher(final FluentAttributeMatcher<Node> matcher) {
        super(matcher);
    }

    public NeoNode2Matcher withId(final String id) {
        super.matcher.with(idVal, id);
        return this;
    }
}
