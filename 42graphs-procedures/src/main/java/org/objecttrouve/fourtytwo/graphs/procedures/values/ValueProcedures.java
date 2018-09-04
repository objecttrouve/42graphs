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

package org.objecttrouve.fourtytwo.graphs.procedures.values;

import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.lang.String.format;
import static java.util.stream.Stream.empty;
import static org.neo4j.procedure.Mode.READ;

public class ValueProcedures {

    @SuppressWarnings("WeakerAccess")
    public static final String procRetrieveAllValues = "org.objecttrouve.fourtytwo.retrieveAllValues";
    @SuppressWarnings("WeakerAccess")
    public static final String procRetrieveNeighbours = "org.objecttrouve.fourtytwo.retrieveNeighbours";

    private enum Query {
        retrieveAllValues("MATCH (n:%s) RETURN n.identifier", "n.identifier"),
        retrieveNeighbours("" +
            "MATCH (:%s{identifier:'%s'})-[spos]->(:%s)<-[vpos]-(neighbour:%s) " +
            " WHERE vpos.position=spos.position+%s RETURN DISTINCT neighbour", "neighbour"),;
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
    @Procedure(name = procRetrieveAllValues, mode = READ)
    @Description("Returns all values in the given dimension.")
    public Stream<StringValueRecord> retrieveAllValues(@Name("dimension") final String dimension) {
        if (StringUtils.isBlank(dimension)) {
            log.warn("Procedure '%s' called with null or empty 'dimension' parameter. Won't return anything meaningful.", procRetrieveAllValues);
            return empty();
        }
        return execute(Query.retrieveAllValues, dimension);
    }

    @SuppressWarnings("unused")
    @Procedure(name = procRetrieveNeighbours, mode = READ)
    @Description("Returns all neighbours of the given value in the given dimension with the given parent dimension and the given vicinity.")
    public Stream<StringValueRecord> retrieveAllNeighbours(
        @Name("self") final String self,
        @Name("parentDimension") final String parentDimension,
        @Name("leafDimension") final String leafDimension,
        @Name("vicinity") final long vicinity
    ) {
        if (StringUtils.isBlank(self)) {
            log.warn("Procedure '%s' called with null or empty 'self' parameter. Won't return anything meaningful.", procRetrieveAllValues);
            return empty();
        }
        if (StringUtils.isBlank(parentDimension)) {
            log.warn("Procedure '%s' called with null or empty 'parentDimension' parameter. Won't return anything meaningful.", procRetrieveAllValues);
            return empty();
        }
        if (StringUtils.isBlank(leafDimension)) {
            log.warn("Procedure '%s' called with null or empty 'leafDimension' parameter. Won't return anything meaningful.", procRetrieveAllValues);
            return empty();
        }
        return execute(Query.retrieveNeighbours, leafDimension, self, parentDimension, leafDimension, vicinity);
    }

    private Stream<StringValueRecord> execute(final Query q, final Object... args) {
        return executeValueQuery(q.str(args), q.resultKey);
    }

    private Stream<StringValueRecord> executeValueQuery(final String query, final String resultKey) {
        log.debug("Executing query '%s'...", query);
        final Result result = db.execute(query);
        if (!result.hasNext()) {
            return empty();
        }
        return getValues(result, resultKey);
    }

    private Stream<StringValueRecord> getValues(final Result result, final String resultKey) {
        final Iterator<Map<String, Object>> itr = new Iterator<Map<String, Object>>() {
            @Override
            public boolean hasNext() {
                return result.hasNext();
            }

            @Override
            public Map<String, Object> next() {
                return result.next();
            }
        };

        return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(itr, Spliterator.ORDERED),
            false).map(m -> {
            final Object resultObject = m.getOrDefault(resultKey, "");
            if (resultObject instanceof String){
                return new StringValueRecord((String) resultObject);
            } else if (resultObject instanceof Node) {
                final Object identifier = ((Node) resultObject).getProperty("identifier");
                return new StringValueRecord((String) identifier);
            }

            return new StringValueRecord("");
        });
    }


}

