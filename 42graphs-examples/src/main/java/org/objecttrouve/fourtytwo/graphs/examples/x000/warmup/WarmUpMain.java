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

package org.objecttrouve.fourtytwo.graphs.examples.x000.warmup;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.objecttrouve.fourtytwo.graphs.api.Graph;
import org.objecttrouve.fourtytwo.graphs.api.GraphWriter;
import org.objecttrouve.fourtytwo.graphs.api.SequenceTree;
import org.objecttrouve.fourtytwo.graphs.backend.init.EmbeddedBackend;
import org.objecttrouve.fourtytwo.graphs.examples.common.*;
import org.objecttrouve.fourtytwo.graphs.examples.common.cmd.Args;
import org.objecttrouve.fourtytwo.graphs.examples.common.cmd.CmdLine;
import org.objecttrouve.fourtytwo.graphs.matchers.NeoDbMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.lang.String.valueOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.objecttrouve.fourtytwo.graphs.examples.common.GraphDatabaseServiceFactory.dbService;
import static org.objecttrouve.fourtytwo.graphs.examples.common.ResourceFile.file;
import static org.objecttrouve.fourtytwo.graphs.examples.common.io.Io.clean;

public class WarmUpMain {

    private static final Logger log = LoggerFactory.getLogger(WarmUpMain.class);
    public static final String warmUpDbDir = "x000.graphdb";

    public static void main(final String[] args) throws IOException {
        run(CmdLine.get(args));
    }

    public static void run(final Args args) throws IOException {
        log.info("Running example " + WarmUpMain.class.getSimpleName() + "...");
        log.info("Loading NLP components...");
        final SentenceDetector sentenceDetector = SentenceDetector.load("doc/x000/de-sent.bin");
        final Tokenizer tokenizer = Tokenizer.load("doc/x000/de-token.bin");

        log.info("Setting up storage directory.");
        final Path outputDirectory = args.outputDirectory();
        if (args.isClean()){
            clean(outputDirectory);
        }
        final Path store = outputDirectory.resolve(warmUpDbDir);
        Files.createDirectories(store);

        log.info("Set up graph access...");
        final GraphDatabaseService db = dbService(store);
        final Graph backend = new EmbeddedBackend(()-> db, () -> {
            try {
                return BatchInserters.inserter(store.toFile());
            } catch (final IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
        final GraphWriter graphWriter = backend.writer(true);

        log.info("Slurping text...");
        final String text = file("doc/x000/Martin_Luther_Uebersetzung_1912.cleanText.txt").read();
        final String[] sentences = sentenceDetector.process(text);
        int i = 0;
        for (final String s : sentences) {
            final String sentence = s.replaceAll("\"", "").replaceAll("'", "");
            i++;
            final String[] tokenize = tokenizer.process(sentence);
            final SequenceTree sequenceTree = new StringSequenceTree(valueOf(i),
                "Sentence", "Token", tokenize);
            //noinspection unchecked
            graphWriter.add(sequenceTree);
        }
        graphWriter.commit();

        log.info("Doing sanity check...");
        /* The GraphWriter closes the DB, so we have to reopen it again here. */
        final GraphDatabaseService reopenedDb = dbService(store);
        /* Let's double-check that the graph has the expected number of nodes. */
        assertThat(reopenedDb, is(NeoDbMatcher.aGraph().ofSize(815492L)));

        log.info("Shutting down...");
        reopenedDb.shutdown();
        log.info("Done.");
    }

}
