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

package org.objecttrouve.fourtytwo.graphs.examples.x001.count;

import org.neo4j.driver.v1.StatementResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.objecttrouve.fourtytwo.graphs.examples.common.GraphDatabaseServiceFactory;
import org.objecttrouve.fourtytwo.graphs.examples.common.Neo4jHomeDir;
import org.objecttrouve.fourtytwo.graphs.examples.x000.warmup.WarmUpMain;
import org.objecttrouve.fourtytwo.graphs.procedures.quantities.LongQuantityRecord;
import org.objecttrouve.fourtytwo.graphs.procedures.quantities.QuantityProcedures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static java.util.Optional.ofNullable;

public class CountStuffMain {
    private static final Logger logger = LoggerFactory.getLogger(CountStuffMain.class);

    public static void main(final String[] args) throws KernelException {
        final Path store = Neo4jHomeDir.get().resolve(WarmUpMain.warmUpDbDir);

        logger.info("Using graph database at " + store + ". Starting up DB service...");
        final GraphDatabaseService db = GraphDatabaseServiceFactory.dbService(store);
        ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(Procedures.class).registerProcedure(QuantityProcedures.class);

        logger.info("Counting all token value nodes...");
        final Result tokenCountResult = db.execute("CALL count.all.values('Token')");
        final Long tokenCount = getLongQuantity(tokenCountResult);
        logger.info("Number of distinct tokens: " + tokenCount);

        logger.info("Counting all sentence value nodes...");
        final Result sentenceCountResult = db.execute("CALL count.all.values('Sentence')");
        final Long sentenceCount = getLongQuantity(sentenceCountResult);
        logger.info("Number of sentences: " + sentenceCount);

    }

    private static Long getLongQuantity(final Result result) {
        return (Long) ofNullable(result)//
            .filter(Result::hasNext)//
            .map(Result::next)//
            .map(resultMap -> resultMap.get(LongQuantityRecord.keyQuantity))//
            .orElse(0L);
    }
}
