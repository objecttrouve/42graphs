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

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.objecttrouve.fourtytwo.graphs.api.Dimension;
import org.objecttrouve.fourtytwo.graphs.pojo.DimensionPojo;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Stream.empty;
import static org.neo4j.procedure.Mode.READ;

public class QuantityProcedures {

    public static final String procCountAllValues = "count.all.values";

    private enum Query {
        countAllValues("MATCH (n:%s) RETURN count(n)") //
        ;
        final String template;

        Query(final String template) {
            this.template = template;
        }

        String str(final String... snippets){
            //noinspection ConfusingArgumentToVarargsMethod
            return String.format(template,  snippets);
        }


    }

    @SuppressWarnings("WeakerAccess")
    @Context
    public GraphDatabaseService db;

    @SuppressWarnings("WeakerAccess")
    @Context
    public Log log;

    @Procedure( name = procCountAllValues, mode = READ )
    public Stream<LongQuantityRecord> countAllValues(@Name("dimension") final String dimensionName)
    {
        if (dimensionName == null) {
            return empty();
        }
        final Dimension dimension = DimensionPojo.toDimension(dimensionName);
        final Result result = db.execute(Query.countAllValues.str(dimension.getName()));
        if (!result.hasNext()){
            return empty();
        }
        return Stream.of(new LongQuantityRecord((long) result.next()//
            .getOrDefault("count(n)", 0L)));

    }

}
