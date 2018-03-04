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


import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.objecttrouve.fourtytwo.graphs.api.Dimension;
import org.objecttrouve.testing.matchers.ConvenientMatchers;
import org.objecttrouve.testing.matchers.fluentatts.Attribute;
import org.objecttrouve.testing.matchers.fluentatts.FluentAttributeMatcher;

public class NeoRelationMatcher extends AbstractMatcherBuilder<Relationship> {

    private static final Attribute<Relationship, Node> endNode = Attribute.attribute("end node", Relationship::getEndNode);
    private static final Attribute<Relationship, Node> startNode = Attribute.attribute("start node", Relationship::getStartNode);
    private static final Attribute<Relationship, String> leafDimension = Attribute.attribute("leaf dimension", r -> r.getType().name());
    private static final Attribute<Relationship, Object> positionKey = Attribute.attribute("position key", r -> r.getProperty(Dimension.positionKey));


    public static NeoRelationMatcher aRelation() {
        return new NeoRelationMatcher(ConvenientMatchers.a(Relationship.class));
    }

    private NeoRelationMatcher(final FluentAttributeMatcher<Relationship> matcher) {
        super(matcher);
    }

    public NeoRelationMatcher to(final NeoNodeMatcher neoNodeMatcher) {
        matcher.with(endNode, neoNodeMatcher);
        return this;
    }

    public NeoRelationMatcher from(final NeoNodeMatcher neoNodeMatcher) {
        matcher.with(startNode, neoNodeMatcher);
        return this;
    }


    public NeoRelationMatcher inDimension(final String leafdim) {
        matcher.with(leafDimension, leafdim);
        return this;
    }

    public NeoRelationMatcher atPosition(final int position) {
        matcher.with(positionKey, position);
        return this;
    }

}
