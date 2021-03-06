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

import org.hamcrest.CoreMatchers;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.objecttrouve.fourtytwo.graphs.api.Value;
import org.objecttrouve.testing.matchers.fluentatts.Attribute;
import org.objecttrouve.testing.matchers.fluentatts.FluentAttributeMatcher;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertEquals;
import static org.objecttrouve.fourtytwo.graphs.matchers.Matchers.an;
import static org.objecttrouve.testing.matchers.fluentatts.Attribute.attribute;

@SuppressWarnings({"SpellCheckingInspection", "unused"})
public class NeoNodeMatcher extends AbstractMatcherBuilder<Node> {


    private static final Function<String, Attribute<Node, Boolean>> nodeDimension = (labelName) -> attribute("dimension '" + labelName + "'", node -> hasLabel(labelName, node));
    private static final Function<Direction, Attribute<Node, Integer>> nodeDegree = (direction) -> attribute("degree", node -> node.getDegree(direction));
    private final Function<String, Attribute<Node, Boolean>> nodeId = (id) -> attribute("id '" + id + "'", node -> hasIdProperty(id, node));

    private static final Attribute<Node, Iterable<Relationship>> nodeRelations = attribute("relations", Node::getRelationships);
    private final Attribute<Node, Iterable<Node>> nodeRelatedNodes = attribute("related nodes", this::getRelatedNodes);
    private final Attribute<Node, Iterable<Node>> nodeIncomingRelatedNodes = attribute("incoming related nodes", this::getIncomingRelatedNodes);
    private final Attribute<Node, Iterable<Node>> nodeOutgoingRelatedNodes = attribute("outgoing related nodes", this::getOutgoingRelatedNodes);
    private final Attribute<Node, Iterable<Relationship>> nodeIncomingRelationships = attribute("incoming relationships", node -> node.getRelationships(Direction.INCOMING));
    private final Attribute<Node, Iterable<Relationship>> nodeOutgoingRelationships = attribute("outgoing relationships", node -> node.getRelationships(Direction.OUTGOING));

    private static Attribute<Node, Long> nodePropDirectNeighbourCount(final String dimension) {
        return longAttributeForKey("directNeighbourCount_"+dimension);
    }
    private static Attribute<Node, Long> nodePropLength(final String dimension) {
        return longAttributeForKey("length_" + dimension);
    }

    private static Attribute<Node, Long> nodePropLongest(final String parentDimension, final String dimension) {
        return longAttributeForKey("longest_" + parentDimension + "_" + dimension);
    }

    private static Attribute<Node, Long> longAttributeForKey(final String key) {
        return attribute(key, node -> node.hasProperty(key) ? (Long) node.getProperty(key) : 0L);
    }

    private static Attribute<Node, List<Integer>> nodePropAggrPosCount(final String parentDimension, final String dimension) {
        final String key = "positionCounts_" + parentDimension + "_" + dimension;
        return attribute(key, node -> node.hasProperty(key) ? intList(key, node) :Collections.emptyList());

    }

    private static List<Integer> intList(final String key, final Node node) {
        return Arrays.stream((int[]) node.getProperty(key)).boxed().collect(Collectors.toList());
    }


    private Long matchedId = null;
    private boolean unique;

    public static NeoNodeMatcher aNode() {
        final FluentAttributeMatcher<Node> m = an.instanceOf(Node.class);
        return new NeoNodeMatcher(m);
    }

    private NeoNodeMatcher(final FluentAttributeMatcher<Node> matcher) {
        super(matcher);
    }

    @Override
    public boolean matches(final Object item) {
        final boolean matches = super.matches(item);
        if (matches && unique) {
            final Node n = (Node) item;
            final long id = n.getId();
            if (matchedId == null) {
                matchedId = id;
            } else {
                assertEquals("Expected node always to be unique and matched only with same ID.", id, (long) matchedId);
            }
        }
        return matches;
    }

    public NeoNodeMatcher unique() {
        unique = true;
        return this;
    }

    public NeoNodeMatcher inDimension(final String dim) {
        matcher.with(nodeDimension.apply(dim), true);
        return this;
    }

    public NeoNodeMatcher withIdentifier(final String id) {
        matcher.with(nodeId.apply(id), true);
        return this;
    }


    public NeoNodeMatcher with(final NeoRelationMatcher... relationMatchers) {
        matcher.with(nodeRelations, CoreMatchers.hasItems(relationMatchers));
        return this;
    }

    public NeoNodeMatcher withAllRelationsBeing(final NeoRelationMatcher relationMatcher) {
        matcher.with(nodeRelations, CoreMatchers.everyItem(relationMatcher));
        return this;
    }

    private boolean hasIdProperty(final Object id, final Node node) {
        return node.getProperties(Value.idKey).values().stream()//
            .anyMatch(v -> Objects.equals(v, id));
    }

    private static boolean hasLabel(final String root, final Node node) {
        return StreamSupport.stream(node.getLabels().spliterator(), false)//
            .map(Label::name)//
            .anyMatch(l -> Objects.equals(l, root));
    }

    public NeoNodeMatcher ofDegree(final int deg) {
        return ofDirectedDegree(deg, Direction.BOTH);
    }

    public NeoNodeMatcher ofOutgoingDegree(final int deg) {
        return ofDirectedDegree(deg, Direction.OUTGOING);
    }

    public NeoNodeMatcher ofIncomingDegree(final int deg) {
        return ofDirectedDegree(deg, Direction.INCOMING);
    }

    private NeoNodeMatcher ofDirectedDegree(final int deg, final Direction direction) {
        matcher.with(nodeDegree.apply(direction), deg);
        return this;
    }

    public NeoNodeMatcher withAllRelatedNodesBeing(final NeoNodeMatcher nodeMatcher) {
        matcher.with(nodeRelatedNodes, CoreMatchers.everyItem(nodeMatcher));
        return this;
    }

    public NeoNodeMatcher withAllIncomingRelatedNodesBeing(final NeoNodeMatcher nodeMatcher) {
        matcher.with(nodeIncomingRelatedNodes, CoreMatchers.everyItem(nodeMatcher));
        return this;
    }

    public NeoNodeMatcher withAllOutgoingRelatedNodesBeing(final NeoNodeMatcher nodeMatcher) {
        matcher.with(nodeOutgoingRelatedNodes, CoreMatchers.everyItem(nodeMatcher));
        return this;
    }

    private List<Node> getRelatedNodes(final Node node) {
        return getNodesFromBothDirections(node);
    }

    private List<Node> getNodesFromBothDirections(final Node node) {
        final List<Node> result = new LinkedList<>();
        result.addAll(getIncomingRelatedNodes(node));
        result.addAll(getOutgoingRelatedNodes(node));
        return result;
    }

    private List<Node> getIncomingRelatedNodes(final Node node) {
        final List<Node> result = new LinkedList<>();
        node.getRelationships(Direction.INCOMING).forEach(r -> result.add(r.getStartNode()));
        return result;
    }

    private List<Node> getOutgoingRelatedNodes(final Node node) {
        final List<Node> result = new LinkedList<>();
        node.getRelationships(Direction.OUTGOING).forEach(r -> result.add(r.getEndNode()));
        return result;
    }

    public NeoNodeMatcher withAllIncomingRelationsBeing(final NeoRelationMatcher rel) {
        matcher.with(nodeIncomingRelationships, CoreMatchers.everyItem(rel));
        return this;
    }

    public NeoNodeMatcher withAllOutgoingRelationsBeing(final NeoRelationMatcher rel) {
        matcher.with(nodeOutgoingRelationships, CoreMatchers.everyItem(rel));
        return this;
    }

    public NeoNodeMatcher withPropDirectNeighbourCount(final String dimension, final long expectedCount) {
        matcher.with(nodePropDirectNeighbourCount(dimension), expectedCount);
        return this;
    }

    public NeoNodeMatcher withPropLength(final String dimension, final long expected) {
        matcher.with(nodePropLength(dimension), expected);
        return this;
    }

    public NeoNodeMatcher withPropLongest(final String parentDimension, final String dimension, final long expected) {
        matcher.with(nodePropLongest(parentDimension, dimension), expected);
        return this;
    }

    public NeoNodeMatcher withPropAggrPositionCount(final String parentDimension, final String childDimension, final List<Integer> expected) {
        matcher.with(nodePropAggrPosCount(parentDimension, childDimension), expected);
        return this;
    }
}
