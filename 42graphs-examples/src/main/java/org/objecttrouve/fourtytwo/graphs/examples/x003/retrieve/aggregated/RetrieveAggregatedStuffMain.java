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

package org.objecttrouve.fourtytwo.graphs.examples.x003.retrieve.aggregated;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.objecttrouve.fourtytwo.graphs.examples.common.GraphDatabaseServiceFactory;
import org.objecttrouve.fourtytwo.graphs.examples.common.cmd.Args;
import org.objecttrouve.fourtytwo.graphs.examples.common.cmd.CmdLine;
import org.objecttrouve.fourtytwo.graphs.examples.x000.warmup.WarmUpMain;
import org.objecttrouve.fourtytwo.graphs.procedures.aggregating.AggregatingProcedures;
import org.objecttrouve.fourtytwo.graphs.procedures.quantities.QuantityProcedures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.objecttrouve.fourtytwo.graphs.api.Value.idKey;

public class RetrieveAggregatedStuffMain {
    private static final Logger log = LoggerFactory.getLogger(RetrieveAggregatedStuffMain.class);

    public static void main(final String[] cmdLineArgs) throws KernelException, IOException {
        final Args args = CmdLine.get(cmdLineArgs);
        run(args);
    }

    private static void run(final Args args) throws IOException, KernelException {
        log.info("Running example " + RetrieveAggregatedStuffMain.class.getSimpleName() + "...");
        final Path store = args.outputDirectory().resolve(WarmUpMain.warmUpDbDir);
        if (args.isClean()) {
            WarmUpMain.run(args);
        }
        log.info("Using graph database at " + store + ". Starting up DB service...");
        final GraphDatabaseService db = GraphDatabaseServiceFactory.dbService(store);
        final Procedures procedures = ((GraphDatabaseAPI) db).getDependencyResolver()
            .resolveDependency(Procedures.class);
        procedures.registerProcedure(QuantityProcedures.class);
        procedures.registerProcedure(AggregatingProcedures.class);
        if (args.isClean()) {
            log.info("Aggregating neighbour counts. This may take a while...");
            final Instant startAggr = Instant.now();
            db.execute("CALL org.objecttrouve.fourtytwo.aggregateDirectNeighbourCounts('Sentence','Token')");
            final Duration aggrDuration = Duration.between(startAggr, Instant.now());
            log.info("Aggregation actually had a duration of " + aggrDuration);
        }
        log.info("Retrieve tokens sorted by their number of neighbours. (Descending)...");
        final Result tokensWithNeighbourCounts = db.execute("MATCH (t:Token)-->(:Sentence) RETURN DISTINCT t.identifier, t.directNeighbourCount ORDER BY t.directNeighbourCount DESC");
        final long totalNeighbourCount = tokensWithNeighbourCounts.stream()
            .mapToLong(m -> {
                long count = (long) Optional.ofNullable(m.getOrDefault("t.directNeighbourCount", -1)).orElse(-1L);
                log.info("'" + m.getOrDefault("t." + idKey, "") + "': " + count);
                return count;
            })
            .sum();

        log.info("Running sanity check...");
        assertThat(totalNeighbourCount, is(393175L));

        log.info("Done!");
    }


}
