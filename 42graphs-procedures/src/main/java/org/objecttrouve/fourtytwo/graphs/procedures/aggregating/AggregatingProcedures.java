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

package org.objecttrouve.fourtytwo.graphs.procedures.aggregating;

import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static org.neo4j.procedure.Mode.WRITE;

public class AggregatingProcedures {

    @SuppressWarnings("WeakerAccess")
    public static final String procAggregateDirectNeighbourCounts = "org.objecttrouve.fourtytwo.aggregateDirectNeighbourCounts";

    private static final String neighbourCountTemplate = "" +
        "MATCH (n:%s)-[spos]->(:%s)<-[vpos]-(ne:%s) " +
        " WHERE vpos.position=spos.position+1 OR vpos.position=spos.position-1" +
        " WITH n AS node, count(distinct ne) AS cf " +
        " SET node.directNeighbourCount_%s = cf " +
        "";

    @SuppressWarnings("WeakerAccess")
    public static final String procAggregateLength = "org.objecttrouve.fourtytwo.aggregateLength";

    private static final String lengthTemplate = "" +
        "MATCH (c:%s)-->(p:%s) " +
        " WITH p AS parent, count(c) AS l " +
        " SET parent.length_%s = l " +
        "";

    @SuppressWarnings("WeakerAccess")
    public static final String procAggregateLongest = "org.objecttrouve.fourtytwo.aggregateLongest";

    private static final String longestXTemplate = "" +
        "MATCH (p:%s)-->(g:%s) " +
        " WITH g AS grandParent, max(p.length_%s) AS longest " +
        " SET grandParent.longest_%s_%s = longest " +
        "";

    @Context
    public GraphDatabaseService db;
    @SuppressWarnings("WeakerAccess")
    @Context
    public Log log;

    @SuppressWarnings("unused")
    @Procedure(name = procAggregateDirectNeighbourCounts, mode = WRITE)
    @Description("Aggregates the counts of directly preceding and following node in the childDimension")
    public void aggregateDirectNeighbourCount(
        @Name("parentDimension") final String parentDimension,
        @Name("childDimension") final String childDimension
    ) {
        if (StringUtils.isBlank(parentDimension)) {
            log.warn("Procedure '%s' called with null or empty 'parentDimension' parameter. Won't aggregate anything meaningful.", procAggregateDirectNeighbourCounts);
            return;
        }
        if (StringUtils.isBlank(childDimension)) {
            log.warn("Procedure '%s' called with null or empty 'childDimension' parameter. Won't aggregate anything meaningful.", procAggregateDirectNeighbourCounts);
            return;
        }
        final String query = String.format(neighbourCountTemplate, childDimension, parentDimension, childDimension, childDimension);
        db.execute(query);
    }

    @SuppressWarnings("unused")
    @Procedure(name = procAggregateLength, mode = WRITE)
    @Description("Aggregates the length of a parent item = the number of child items in a child dimension.")
    public void aggregateLength(
        @Name("parentDimension") final String parentDimension,
        @Name("childDimension") final String childDimension
    ) {
        if (StringUtils.isBlank(parentDimension)) {
            log.warn("Procedure '%s' called with null or empty 'parentDimension' parameter. Won't aggregate anything meaningful.", procAggregateDirectNeighbourCounts);
            return;
        }
        if (StringUtils.isBlank(childDimension)) {
            log.warn("Procedure '%s' called with null or empty 'childDimension' parameter. Won't aggregate anything meaningful.", procAggregateDirectNeighbourCounts);
            return;
        }
        final String query = String.format(lengthTemplate, childDimension, parentDimension, childDimension);
        db.execute(query);
    }

    @SuppressWarnings("unused")
    @Procedure(name = procAggregateLongest, mode = WRITE)
    @Description("Finds and stores the longest length of a child item on the parent item.")
    public void aggregateLongest(
        @Name("grandParentDimension") final String grandParentDimension,
        @Name("parentDimension") final String parentDimension,
        @Name("childDimension") final String childDimension
    ) {
        if (StringUtils.isBlank(parentDimension)) {
            log.warn("Procedure '%s' called with null or empty 'parentDimension' parameter. Won't aggregate anything meaningful.", procAggregateDirectNeighbourCounts);
            return;
        }
        if (StringUtils.isBlank(childDimension)) {
            log.warn("Procedure '%s' called with null or empty 'childDimension' parameter. Won't aggregate anything meaningful.", procAggregateDirectNeighbourCounts);
            return;
        }
        final String query = String.format(longestXTemplate,  parentDimension, grandParentDimension, childDimension, parentDimension, childDimension);
        db.execute(query);
    }

}
