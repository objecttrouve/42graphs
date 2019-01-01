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

package org.objecttrouve.fourtytwo.graphs.examples.x001.count;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.objecttrouve.fourtytwo.graphs.examples.common.GraphDatabaseServiceFactory;
import org.objecttrouve.fourtytwo.graphs.examples.common.cmd.Args;
import org.objecttrouve.fourtytwo.graphs.examples.common.cmd.CmdLine;
import org.objecttrouve.fourtytwo.graphs.examples.x000.warmup.WarmUpMain;
import org.objecttrouve.fourtytwo.graphs.procedures.quantities.LongQuantityRecord;
import org.objecttrouve.fourtytwo.graphs.procedures.quantities.QuantityProcedures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;

import static java.util.Optional.ofNullable;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class CountStuffMain {
    private static final Logger log = LoggerFactory.getLogger(CountStuffMain.class);

    public static void main(final String[] cmdLineArgs)  {
        final Args args = CmdLine.get(cmdLineArgs);
        try {
            run(args);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void run(final Args args) throws IOException, KernelException {
        log.info("Running example " + CountStuffMain.class.getSimpleName() + "...");
        final Path store = args.outputDirectory().resolve(WarmUpMain.warmUpDbDir);
        if (args.isClean()){
            WarmUpMain.run(args);
        }

        log.info("Using graph database at " + store + ". Starting up DB service...");
        final GraphDatabaseService db = GraphDatabaseServiceFactory.dbService(store);
        ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(Procedures.class).registerProcedure(QuantityProcedures.class);

        log.info("Counting all token value nodes...");
        final Result tokenCountResult = db.execute("CALL org.objecttrouve.fourtytwo.countAllValues('Token')");
        final Long tokenCount = getLongQuantity(tokenCountResult);
        log.info("Number of distinct tokens: " + tokenCount);

        log.info("Counting all sentence value nodes...");
        final Result sentenceCountResult = db.execute("CALL org.objecttrouve.fourtytwo.countAllValues('Sentence')");
        final Long sentenceCount = getLongQuantity(sentenceCountResult);
        log.info("Number of sentences: " + sentenceCount);

        log.info("Counting all token occurrences...");
        final Result tokenOccurrencesResult = db.execute("CALL org.objecttrouve.fourtytwo.countAllOccurrences('Sentence', 'Token')");
        final Long tokenOccurrences = getLongQuantity(tokenOccurrencesResult);
        log.info("Token occurrences: " + tokenOccurrences);


        log.info("Counting all occurrences of token 'Freude'...");
        final Result freudOccurrencesResult = db.execute("CALL org.objecttrouve.fourtytwo.countOccurrences('Freude', 'Sentence', 'Token')");
        final Long freudOccurrences = getLongQuantity(freudOccurrencesResult);
        log.info("'Freude' occurrences: " + freudOccurrences);

        log.info("Running sanity check...");
        assertThat(tokenCount, is(22999L));
        assertThat(sentenceCount, is(32451L));
        assertThat(tokenOccurrences, is(813319L));
        assertThat(freudOccurrences, is(124L));

        log.info("Shutting down...");
        db.shutdown();

        log.info("Done!");
    }

    public static Long getLongQuantity(final Result result) {
        return (Long) ofNullable(result)//
            .filter(Result::hasNext)//
            .map(Result::next)//
            .map(resultMap -> resultMap.get(LongQuantityRecord.keyQuantity))//
            .orElse(0L);
    }
}
