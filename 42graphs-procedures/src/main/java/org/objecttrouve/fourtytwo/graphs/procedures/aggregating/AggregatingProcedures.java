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
    public static final String procAggregateDirectNeighbourCounts = "aggregate.direct.neighbour.counts";

    private static final String neighbourCountTemplate = "" +
        "MATCH (n:%s)-[spos]->(:%s)<-[vpos]-(ne:%s) " +
        " WHERE vpos.position=spos.position+1 OR vpos.position=spos.position-1" +
        " WITH n AS node, count(distinct ne) AS cf " +
        " SET node.directNeighbourCount = cf " +
        "";

    @SuppressWarnings("WeakerAccess")
    @Context
    public GraphDatabaseService db;
    @SuppressWarnings("WeakerAccess")
    @Context
    public Log log;

    @SuppressWarnings("unused")
    @Procedure(name = procAggregateDirectNeighbourCounts, mode = WRITE)
    @Description("Aggregates the counts of directly preceding and following node in the leafDimension")
    public void aggregateDirectNeighbourCount(
        @Name("parentDimension") final String parentDimension,
        @Name("leafDimension") final String leafDimension
    ) {
        if (StringUtils.isBlank(parentDimension)) {
            log.warn("Procedure '%s' called with null or empty 'parentDimension' parameter. Won't aggregate anything meaningful.", procAggregateDirectNeighbourCounts);
            return;
        }
        if (StringUtils.isBlank(leafDimension)) {
            log.warn("Procedure '%s' called with null or empty 'leafDimension' parameter. Won't aggregate anything meaningful.", procAggregateDirectNeighbourCounts);
            return;
        }
        final String query = String.format(neighbourCountTemplate, leafDimension, parentDimension, leafDimension);
        db.execute(query);
    }

}