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

package org.objecttrouve.fourtytwo.graphs.examples.x002.retrieve;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.objecttrouve.fourtytwo.graphs.api.Value;
import org.objecttrouve.fourtytwo.graphs.examples.common.GraphDatabaseServiceFactory;
import org.objecttrouve.fourtytwo.graphs.examples.common.StringValue;
import org.objecttrouve.fourtytwo.graphs.examples.common.cmd.Args;
import org.objecttrouve.fourtytwo.graphs.examples.common.cmd.CmdLine;
import org.objecttrouve.fourtytwo.graphs.examples.x000.warmup.WarmUpMain;
import org.objecttrouve.fourtytwo.graphs.procedures.quantities.QuantityProcedures;
import org.objecttrouve.fourtytwo.graphs.procedures.values.StringValueRecord;
import org.objecttrouve.fourtytwo.graphs.procedures.values.ValueProcedures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.objecttrouve.fourtytwo.graphs.examples.x001.count.CountStuffMain.*;

public class RetrieveStuffMain {
    private static final Logger log = LoggerFactory.getLogger(RetrieveStuffMain.class);

    public static void main(final String[] cmdLineArgs) {
        final Args args = CmdLine.get(cmdLineArgs);
        try {
            run(args);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void run(final Args args) throws IOException, KernelException {
        log.info("Running example " + RetrieveStuffMain.class.getSimpleName() + "...");
        final Path store = args.outputDirectory().resolve(WarmUpMain.warmUpDbDir);
        if (args.isClean()) {
            WarmUpMain.run(args);
        }

        log.info("Using graph database at " + store + ". Starting up DB service...");
        final GraphDatabaseService db = GraphDatabaseServiceFactory.dbService(store);
        final Procedures procedures = ((GraphDatabaseAPI) db).getDependencyResolver()
            .resolveDependency(Procedures.class);
        procedures.registerProcedure(ValueProcedures.class);
        procedures.registerProcedure(QuantityProcedures.class);

        log.info("Retrieving all token value nodes...");
        final Result tokenCountResult = db.execute("CALL org.objecttrouve.fourtytwo.retrieveAllValues('Token')");
        final List<Value<String>> tokens = list(tokenCountResult);
        log.info("Distinct tokens: ");
        tokens.forEach(t -> log.info("Next token: " + t.getIdentifier()));

        log.info("Running sanity check...");
        assertThat(tokens.size(), is(22999));

        log.info("Retrieving all followers of 'Jesus'...");
        final Result jesusNeighboursResult = db.execute("CALL org.objecttrouve.fourtytwo.retrieveNeighbours('Jesus', 'Sentence', 'Token', 1)");
        final List<Value<String>> neighbours = list(jesusNeighboursResult);
        log.info("Neighbours: ");
        neighbours.forEach(n -> log.info("Next neighbour: " + n.getIdentifier()));

        log.info("Counting all occurrences of token 'Jesus'...");
        final Result jesusOccurrencesResult = db.execute("CALL org.objecttrouve.fourtytwo.countOccurrences('Jesus', 'Sentence', 'Token')");
        final Long jesusOccurrences = getLongQuantity(jesusOccurrencesResult);
        log.info("Of course, 'Jesus' himself occurs often: " + jesusOccurrences);

        log.info("Running sanity check...");
        assertThat(neighbours.size(), is(131));

        log.info("Shutting down...");
        db.shutdown();

        log.info("Done!");
    }

    private static List<Value<String>> list(final Result tokenCountResult) {
        return tokenCountResult.stream()
            .map(m -> new StringValue((String) m.getOrDefault(StringValueRecord.idKey, "")))
            .collect(toList());
    }


}
