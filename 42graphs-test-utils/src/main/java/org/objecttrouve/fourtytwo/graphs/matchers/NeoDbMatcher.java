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


import org.hamcrest.CoreMatchers;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.objecttrouve.testing.matchers.ConvenientMatchers;
import org.objecttrouve.testing.matchers.fluentatts.Attribute;
import org.objecttrouve.testing.matchers.fluentatts.FluentAttributeMatcher;

import static org.objecttrouve.testing.matchers.fluentatts.Attribute.attribute;


public class NeoDbMatcher extends AbstractMatcherBuilder<GraphDatabaseService> {

    private final Attribute<GraphDatabaseService, Boolean> existingNodes = attribute(//
        "graph having any nodes", //
        (db) -> db.execute("MATCH (n) RETURN n").hasNext()//
    );
    private final Attribute<GraphDatabaseService, Long> graphOrder = attribute(//
        "order", //
        db -> (Long) //
            db.execute("MATCH (n) RETURN count(*)") //
                .next()//
                .get("count(*)") //
    );
    private final Attribute<GraphDatabaseService, Long> graphSize = attribute(//
        "size", //
        db -> (Long)db.execute("MATCH (n)-[r]->() RETURN COUNT(r)")//
            .next()//
            .get("COUNT(r)")//
    );
    private final Attribute<GraphDatabaseService, Iterable<Node>> allNodes = attribute(//
        "all nodes", //
        GraphDatabaseService::getAllNodes //
    );


    public static NeoDbMatcher theEmptyGraph() {
        return new NeoDbMatcher(ConvenientMatchers.a(GraphDatabaseService.class)).withoutNodes();
    }

    public static NeoDbMatcher aGraph() {
        return new NeoDbMatcher(ConvenientMatchers.a(GraphDatabaseService.class));
    }

    private NeoDbMatcher(final FluentAttributeMatcher<GraphDatabaseService> matcher) {
        super(matcher);
    }

    @SuppressWarnings("WeakerAccess")
    public NeoDbMatcher withoutNodes() {
        matcher.with(existingNodes, false);
        return this;
    }

    public NeoDbMatcher containing(final NeoNodeMatcher... neoNodeMatcher) {
        matcher.with(allNodes, //
            CoreMatchers.hasItems(neoNodeMatcher));
        return this;
    }

    public NeoDbMatcher ofOrder(final long order) {
        matcher.with(graphOrder, order);
        return this;
    }

    public NeoDbMatcher ofSize(final long size) {
        matcher.with(graphSize, size);
        return this;
    }
}
