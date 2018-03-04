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


package org.objecttrouve.fourtytwo.graphs.backend.init;

import com.google.common.collect.Maps;
import org.neo4j.graphdb.GraphDatabaseService;
import org.objecttrouve.fourtytwo.graphs.api.Dimension;
import org.objecttrouve.fourtytwo.graphs.api.GraphWriter;
import org.objecttrouve.fourtytwo.graphs.api.SequenceTree;
import org.objecttrouve.fourtytwo.graphs.api.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.joining;
import static org.neo4j.helpers.collection.MapUtil.map;

public class TransactionalWriter implements GraphWriter {
    private static final Logger logger = LoggerFactory.getLogger(TransactionalWriter.class);

    private final GraphDatabaseService db;
    private final org.neo4j.graphdb.Transaction t;

    TransactionalWriter(final GraphDatabaseService db, final org.neo4j.graphdb.Transaction t) {
        this.db = db;
        this.t = t;
    }

    @Override
    public void commit() {
        logger.trace("Committing...");
        t.success();
        t.close();
    }

    @Override
    public <T, U> GraphWriter add(final SequenceTree<T, U> sequenceTree) {
        logger.trace("Adding {} {}...", SequenceTree.class.getName(), sequenceTree.toString());
        final Value<T> parent = sequenceTree.getRoot();
        final Dimension rootDimension = sequenceTree.getRootDimension();
        final String rootDimensionName = rootDimension.getName();
        final Map<String, Object> parentProps = Maps.newHashMap();
        final Dimension leafDimension = sequenceTree.getLeafDimension();
        final List<Value<U>> values = sequenceTree.getValues();
        parentProps.put(leafDimension.childrenSizeKey(), values.size());
        addNode(parent.getIdentifier(), rootDimensionName, parentProps);
        for (int i = 0; i < values.size(); i++) {
            final Value<U> child = values.get(i);
            addNode(child.getIdentifier(), leafDimension.getName(), Maps.newHashMap());
            addRelation(//
                child.getIdentifier(), //
                leafDimension.getName(),//
                parent.getIdentifier(), //
                rootDimensionName, //
                i//
            );
        }

        return this;
    }

    private <U, V> void addRelation(//
                                    final V childId, //
                                    final String childDimension, //
                                    final U parentId, //
                                    final String parentDimension, //
                                    final int position //
    ) {
        final Map<String, Object> parameters = map(
            "cid", childId,
            "pid", parentId,
            "pos", position
        );
        final String query = "MATCH (c:" + childDimension + " { " + Value.idKey + ": $cid }),(p:" + parentDimension + " { " + Value.idKey + ": $pid })\n"//
            + "MERGE (c)-[:" + childDimension + " {" + Dimension.positionKey + ":$pos}]->(p)\n"//
            ;
        db.execute(query, parameters);
    }

    private <V> void addNode(final V id, final String dimension, final Map<String, Object> props) {
        final Map<String, Object> parameters = map(
            "id", id
        );
        db.execute("MERGE (:" + dimension + " { " //
            //+ props.entrySet().stream().map(e -> e.getKey() + " : " + e.getValue().toString()).collect(joining(",")) + (props.isEmpty() ? "" : ", ")//
            + Value.idKey + ": $id})", parameters);
        if (!props.isEmpty()) {
            db.execute("MATCH (n:" + dimension + "{" + Value.idKey + ": $id})\n" //
                    + props.entrySet().stream().map(e -> "SET n." + e.getKey() + "=" + e.getValue().toString()).collect(joining("\n"))
                , parameters);
        }
    }

    @Override
    public void abort() {
        logger.info("Aborting transaction...");
        t.failure();
        t.terminate();
    }


}
