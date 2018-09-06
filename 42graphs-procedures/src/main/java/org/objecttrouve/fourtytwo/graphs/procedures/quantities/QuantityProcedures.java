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

package org.objecttrouve.fourtytwo.graphs.procedures.quantities;

import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import javax.annotation.Nonnull;
import java.util.stream.Stream;

import static java.lang.String.*;
import static java.util.stream.Stream.empty;
import static org.neo4j.procedure.Mode.READ;

public class QuantityProcedures {

    @SuppressWarnings("WeakerAccess")
    public static final String procCountAllValues = "org.objecttrouve.fourtytwo.countAllValues";
    @SuppressWarnings("WeakerAccess")
    public static final String procCountAllOccurrences = "org.objecttrouve.fourtytwo.countAllOccurrences";
    @SuppressWarnings("WeakerAccess")
    public static final String procCountOccurrences = "org.objecttrouve.fourtytwo.countOccurrences";
    @SuppressWarnings("WeakerAccess")
    public static final String procCountNeighbours = "org.objecttrouve.fourtytwo.countNeighbours";

    private enum Query {
        countAllValues("MATCH (n:%s) RETURN count(n)", "count(n)"),
        countAllOccurrences("MATCH (l:%s)-[r]->(p:%s) RETURN count(r)", "count(r)"),
        countOccurrences("MATCH (l:%s{identifier:'%s'})-[r]->(p:%s) RETURN count(r)", "count(r)"),
        countNeighbours("MATCH (:%s{identifier:'%s'})-[spos]->(:%s)<-[vpos]-(neighbour:%s) " +
                               " WHERE vpos.position=spos.position+%s RETURN count(distinct neighbour)", "count(distinct neighbour)")
        ;
        final String template;
        final String resultKey;

        Query(final String template, final String result) {
            this.template = template;
            this.resultKey = result;
        }

        String str(final Object... snippets) {
            //noinspection ConfusingArgumentToVarargsMethod
            return format(template, snippets);
        }


    }

    @SuppressWarnings("WeakerAccess")
    @Context
    public GraphDatabaseService db;
    @SuppressWarnings("WeakerAccess")
    @Context
    public Log log;

    @SuppressWarnings("unused")
    @Procedure(name = procCountAllValues, mode = READ)
    @Description("Counts the value nodes in the given dimension.")
    public Stream<LongQuantityRecord> countAllValues(@Name("dimension") @Nonnull final String dimensionName) {
        if (StringUtils.isBlank(dimensionName)) {
            log.warn("Procedure '%s' called with null or empty 'dimension' parameter. Won't work.", procCountAllValues);
            return empty();
        }
        return execute(Query.countAllValues, dimensionName);
    }

    @SuppressWarnings("unused")
    @Procedure(name = procCountAllOccurrences, mode = READ)
    @Description("Counts all value occurrences in the given leaf dimension with the given parent dimension.")
    public Stream<LongQuantityRecord> countAllOccurrences(
        @Name("parentDimension") final String parentDimensionName,
        @Name("leafDimension")final String leafDimensionName
    ) {
        if (StringUtils.isBlank(parentDimensionName)) {
            log.warn("Procedure '%s' called with null or empty 'parentDimensionName' parameter. Won't work.", procCountAllOccurrences);
            return empty();
        }
        if (StringUtils.isBlank(leafDimensionName)) {
            log.warn("Procedure '%s' called with null or empty 'leafDimensionName' parameter. Won't work.", procCountAllOccurrences);
            return empty();
        }
        return execute(Query.countAllOccurrences, leafDimensionName, parentDimensionName);
    }


    @SuppressWarnings("unused")
    @Procedure(name = procCountOccurrences, mode = READ)
    @Description("Counts all occurrences of a particular value in the given leaf dimension with the given parent dimension.")
    public Stream<LongQuantityRecord> countOccurrences(
        @Name("value") final String value,
        @Name("parentDimension") final String parentDimensionName,
        @Name("leafDimension")final String leafDimensionName

    ){
        if (StringUtils.isBlank(value)) {
            log.warn("Procedure '%s' called with null or empty 'value' parameter. Won't work.", procCountOccurrences);
            return empty();
        }
        if (StringUtils.isBlank(parentDimensionName)) {
            log.warn("Procedure '%s' called with null or empty 'parentDimensionName' parameter. Won't work.", procCountOccurrences);
            return empty();
        }
        if (StringUtils.isBlank(leafDimensionName)) {
            log.warn("Procedure '%s' called with null or empty 'leafDimensionName' parameter. Won't work.", procCountOccurrences);
            return empty();
        }
        return execute(Query.countOccurrences, leafDimensionName, value, parentDimensionName);

    }

    @SuppressWarnings("unused")
    @Procedure(name = procCountNeighbours, mode = READ)
    @Description("Returns all neighbours of the given value in the given dimension with the given parent dimension and the given vicinity.")
    public Stream<LongQuantityRecord> retrieveAllNeighbours(
        @Name("self") final String self,
        @Name("parentDimension") final String parentDimension,
        @Name("leafDimension") final String leafDimension,
        @Name("vicinity") final long vicinity
    ) {
        if (StringUtils.isBlank(self)) {
            log.warn("Procedure '%s' called with null or empty 'self' parameter. Won't return anything meaningful.", procCountNeighbours);
            return empty();
        }
        if (StringUtils.isBlank(parentDimension)) {
            log.warn("Procedure '%s' called with null or empty 'parentDimension' parameter. Won't return anything meaningful.", procCountNeighbours);
            return empty();
        }
        if (StringUtils.isBlank(leafDimension)) {
            log.warn("Procedure '%s' called with null or empty 'leafDimension' parameter. Won't return anything meaningful.", procCountNeighbours);
            return empty();
        }
        return execute(Query.countNeighbours, leafDimension, self, parentDimension, leafDimension, vicinity);
    }


    private Stream<LongQuantityRecord> execute(final Query q, final Object... args){
        return executeLongQuery(q.str(args), q.resultKey);
    }

    private Stream<LongQuantityRecord> executeLongQuery(final String query, final String resultKey) {
        log.debug("Executing query '%s'...", query);
        final Result result = db.execute(query);
        if (!result.hasNext()) {
            return empty();
        }
        return getLongQuantity(result, resultKey);
    }

    private Stream<LongQuantityRecord> getLongQuantity(final Result result, final String resultKey) {
        return Stream.of(//
            new LongQuantityRecord(//
                (long) result.next()//
                    .getOrDefault(resultKey, 0L)//
            ));
    }




}
