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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;
import org.objecttrouce.fourtytwo.graphs.aggregations.api.Aggregator;
import org.objecttrouce.fourtytwo.graphs.aggregations.api.NeighbourWithCount;
import org.objecttrouce.fourtytwo.graphs.aggregations.api.ValueWithPosition;
import org.objecttrouve.fourtytwo.graphs.api.*;
import org.objecttrouve.fourtytwo.graphs.api.Value;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.neo4j.driver.v1.Values.parameters;
import static org.objecttrouve.fourtytwo.graphs.api.Value.idKey;

class DriverReadOnlyBackend implements GraphReader {

    private static final List<Class<?>> supportedValueClasses = ImmutableList.of(//
        String.class, //
        Integer.class, //
        Object.class //
    );
    private final Driver driver;
    private final Aggregator aggregator;

    DriverReadOnlyBackend(final Driver driver, final Aggregator aggregator) {
        this.driver = driver;
        this.aggregator = aggregator;
    }

    @Override
    public long countNodes(final Dimension dimension) {
        if (dimension == null) {
            return 0L;
        }
        return inTx((tx -> {
            final StatementResult result = tx.run("MATCH (n:" + dimension.getName() + ") RETURN count(n)");
            return result.hasNext() ? result.next().get("count(n)").asLong() : 0L;
        }));
    }

    @Override
    public <T> Stream<Value<T>> streamNodeValues(final Dimension dimension, final Class<T> klass) {
        checkValueTypeSupported(klass);
        return streamNodes(dimension).map(n -> (Value<T>) () -> {
            final org.neo4j.driver.v1.Value value = n.get(idKey);
            return casT(value, klass);
        });
    }

    private <T> T casT(final org.neo4j.driver.v1.Value value, final Class<T> klass) {
        return klass.cast(toObj(value, klass));
    }

    private Object toObj(final org.neo4j.driver.v1.Value value, final Class<?> klass) {
        if (String.class.isAssignableFrom(klass)) {
            return value.asString();
        }
        if (Integer.class.isAssignableFrom(klass)) {
            return value.asInt();
        }
        return value.asObject();
    }

    private void checkValueTypeSupported(final Class<?> klass) {
        if (klass == null) {
            throw new IllegalArgumentException("Sorry, class 'null' is not allowed. Currently, you can read the followng types: " + supportedValueClasses);
        }
        for (final Class<?> supportedValueClass : supportedValueClasses) {
            if (klass.isAssignableFrom(supportedValueClass)) {
                return;
            }
        }
        throw new IllegalArgumentException("Sorry, class " + klass.getName()
            + " is not supported (yet). Currently, you can read the followng types: " + supportedValueClasses);
    }

    @Override
    public long countAllValues(final Dimension parentDimension, final Dimension leafDimension) {
        if (parentDimension == null || leafDimension == null) {
            return 0L;
        }
        return inTx((tx -> {
            final StatementResult result = tx.run("MATCH (:" + leafDimension.getName() + ")-[r]->(:" + parentDimension.getName() + ") RETURN count(r)");
            return result.hasNext() ? result.next().get("count(r)").asLong() : 0L;
        }));
    }

    @Override
    public <T> long countOccurrences(final Value<T> value, final Dimension parentDimension, final Dimension leafDimension) {
        if (value == null || parentDimension == null || leafDimension == null) {
            return 0L;
        }
        return inTx((tx -> {
            final StatementResult result = tx.run(//
                "MATCH (:" + leafDimension.getName() + "{" + idKey + ": $id })-[r]->(:" + parentDimension.getName() + ") RETURN count(r)", //
                parameters("id", value.getIdentifier()) //
            );
            return result.hasNext() ? result.next().get("count(r)").asLong() : 0L;
        }));
    }


    Stream<Node> streamNodes(final Dimension dimension) {
        if (dimension == null) {
            return Stream.empty();
        }
        return inTx((tx -> nodeStream(tx.run("MATCH (n:" + dimension.getName() + ") RETURN n"), "n")));
    }

    private Stream<Node> nodeStream(final StatementResult result, final String colKey) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<Node>() {
                @Override
                public boolean hasNext() {
                    return result.hasNext();
                }

                @Override
                public Node next() {
                    //noinspection unchecked
                    return (Node) result.next().get(colKey).asObject();
                }
            }, Spliterator.ORDERED),
            false);
    }

    static class TValue<T> implements Value<T> {

        private final T t;

        TValue(final T t) {
            this.t = t;
        }

        @Override
        public T getIdentifier() {
            return t;
        }

    }

    @Override
    public <T> Stream<AggregatedNeighbour<T>> streamAggregatedNeighbours(final Value<T> self, final Dimension parentDimension, final Dimension leafDimension, final int vicinity) {

        //noinspection unchecked
        return aggregator.countNeighbours(streamNeighbours(self, parentDimension, leafDimension, vicinity).map(
            node -> new TValue(node.get(idKey).asObject())),
            self, vicinity).map(nwc -> {
            //noinspection unchecked
            final NeighbourWithCount<T> neighbourWithCount = (NeighbourWithCount<T>) nwc;
            final Map<QuantityMetric, Number> metrics = Maps.newHashMap();
            metrics.put(NeighbourQuantityMetric.vicinity, vicinity);
            metrics.put(NeighbourQuantityMetric.count, neighbourWithCount.getCount());
            return new NeighbourWithQuantities<>(self, neighbourWithCount.getNeighbour(), metrics);
        });
    }

    <T> Stream<Node> streamNeighbours(final Value<T> self, final Dimension parentDimension, final Dimension leafDimension, final int vicinity) {
        if (self == null || parentDimension == null || leafDimension == null) {
            return Stream.empty();
        }
        return inTx((tx -> {
            final String query = "MATCH (self:" + leafDimension.getName() + "{ " + idKey + ": '" + self.getIdentifier() + "' }" + ") -[spos]->(:" + parentDimension.getName() + ")<-[vpos]-(neighbour:" + leafDimension.getName() + ") WHERE vpos." + Dimension.positionKey + "=spos." + Dimension.positionKey + "+" + vicinity + " RETURN neighbour";
            //System.out.println(query);
            return nodeStream(tx.run(query), "neighbour");
        }));
    }

    <T> Stream<ValueWithPosition<T>> streamNodesWithPosition(final Dimension parentDimension, final Dimension leafDimension, final Class<T> tClass) {
        if (parentDimension == null || leafDimension == null) {
            return Stream.empty();
        }
        checkValueTypeSupported(tClass);
        return inTx(tx -> {
            final String query = "MATCH (n:" + leafDimension.getName() + ") -[r]->(:" + parentDimension.getName() + ") RETURN n, r";
            //System.out.println(query);

            final StatementResult result = tx.run(query);
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<NodeWithPosition<T>>() {
                    @Override
                    public boolean hasNext() {
                        return result.hasNext();
                    }

                    @Override
                    public NodeWithPosition<T> next() {
                        //noinspection unchecked
                        final Record next = result.next();
                        final Node n = next.get("n").asNode();
                        final Relationship r = next.get("r").asRelationship();
                        return new NodeWithPosition<>(casT(n.get(Value.idKey), tClass), r.get(Dimension.positionKey).asInt());
                    }
                }, Spliterator.ORDERED),
                false);
        });
    }

    private <T> T inTx(final Function<Transaction, T> query) {
        return driver.session(AccessMode.READ).readTransaction(query::apply);
    }

    @Override
    public <T> Stream<org.objecttrouve.fourtytwo.graphs.api.AggregatedValue<T>> streamFullyAggregatedValues(final Dimension parentDimension, final Dimension leafDimension, final Class<T> tClass) {
       return aggregator.aggregate(streamNodesWithPosition(parentDimension, leafDimension, tClass)).map(vwap -> {
            final Map<ValueQuantityMetric, Number> metrics = Maps.newHashMap();
            metrics.put(ValueQuantityMetric.count, vwap.getCount());
            return new ValueWithQuantities<>(vwap.getIdentifier(), leafDimension, metrics, vwap.getCountsPerPosition());
        });
    }
}
