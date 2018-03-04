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

package org.objecttrouve.fourtytwo.graphs.read;

import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.objecttrouce.fourtytwo.graphs.aggregations.api.ValueWithPosition;
import org.objecttrouve.fourtytwo.graphs.api.*;
import org.objecttrouve.fourtytwo.graphs.backend.init.EmbeddedBackend;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.neo4j.graphdb.Label.label;
import static org.objecttrouce.fourtytwo.graphs.aggregations.logic.AggregatorFactory.*;
import static org.objecttrouve.fourtytwo.graphs.matchers.AggregatedValueMatcher.anAggrValue;
import static org.objecttrouve.fourtytwo.graphs.matchers.AggregatedNeighbourMatcher.anAggregatedNeighbour;
import static org.objecttrouve.fourtytwo.graphs.matchers.NeoDbMatcher.theEmptyGraph;
import static org.objecttrouve.fourtytwo.graphs.matchers.NeoNode2Matcher.aNode;
import static org.objecttrouve.fourtytwo.graphs.matchers.ValueMatcher.aValueAs;
import static org.objecttrouve.fourtytwo.graphs.mocks.DimensionMock.dim;
import static org.objecttrouve.fourtytwo.graphs.mocks.StringValue.str;
import static org.objecttrouve.fourtytwo.graphs.mocks.TestIntegerSequenceTree.anIntegerSequence;
import static org.objecttrouve.fourtytwo.graphs.mocks.TestStringSequenceTree.aStringSequence;
import static org.objecttrouve.fourtytwo.graphs.read.ValueWithPositionMatcher.aVwp;
import static org.objecttrouve.fourtytwo.graphs.tx.Tx.done;
import static org.objecttrouve.fourtytwo.graphs.tx.Tx.inTx;

public class DriverReadOnlyBackendTest {
    private static final String localhost_7687 = "localhost:7687";
    @ClassRule
    public static TemporaryFolder tmpFolder = new TemporaryFolder();
    private static GraphDatabaseService db;
    private static DriverReadOnlyBackend rob;
    private static Graph graph;
    private static final boolean noInit = false;

    @BeforeClass
    public static void init() throws IOException {
        final org.neo4j.kernel.configuration.BoltConnector bolt = new org.neo4j.kernel.configuration.BoltConnector();
        final File storeDir = tmpFolder.newFolder();
        db = new GraphDatabaseFactory()//
            .newEmbeddedDatabaseBuilder(storeDir)
            .setConfig(bolt.type, "BOLT")
            .setConfig(bolt.enabled, "true")
            .setConfig(bolt.listen_address, localhost_7687)
            .newGraphDatabase();
        graph = new EmbeddedBackend(//
            () -> db, () -> {
            try {
                return BatchInserters.inserter(storeDir);
            } catch (final IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
        rob = readOnlyBackend();
    }

    @AfterClass
    public static void destroy() {
        db.shutdown();
        tmpFolder.delete();
    }

    @Before
    public void clean() {

        final Transaction tx = db.beginTx();
        db.execute("MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r");
        assertThat(db, is(theEmptyGraph()));
        done(tx);
    }


    @Test
    public void countNodes__empty_DB() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();

        final long count = rob.countNodes(tokens);

        assertThat(count, is(0L));
    }

    @Test
    public void countNodes__1_matching_node() throws InterruptedException {
        inTx(db, () -> db.createNode(label("Token")));
        final Dimension tokens = dim().withName("Token").mock();

        final long count = rob.countNodes(tokens);

        assertThat(count, is(1L));
    }

    @Test
    public void countNodes__1_non_matching_node() throws InterruptedException {
        inTx(db, () -> db.createNode(label("Quark")));
        final Dimension tokens = dim().withName("Token").mock();

        final long count = rob.countNodes(tokens);

        assertThat(count, is(0L));
    }

    @Test
    public void countNodes__1_null_Dimension_arg() throws InterruptedException {
        inTx(db, () -> db.createNode(label("Quark")));

        final long count = rob.countNodes(null);

        assertThat(count, is(0L));
    }

    @Test
    public void countNodes__multiple_matching_nodes() throws InterruptedException {
        inTx(db, () -> db.createNode(label("Token")));
        inTx(db, () -> db.createNode(label("Token")));
        inTx(db, () -> db.createNode(label("Token")));
        final Dimension tokens = dim().withName("Token").mock();

        final long count = rob.countNodes(tokens);

        assertThat(count, is(3L));
    }

    @Test
    public void countNodes__multiple_matching_nodes_and_distractors() throws InterruptedException {
        inTx(db, () -> db.createNode(label("Token")));
        inTx(db, () -> db.createNode(label("Token")));
        inTx(db, () -> db.createNode(label("Pescado")));
        inTx(db, () -> db.createNode(label("La Barbe")));
        inTx(db, () -> db.createNode(label("Token")));
        final Dimension tokens = dim().withName("Token").mock();

        final long count = rob.countNodes(tokens);

        assertThat(count, is(3L));
    }


    @Test
    public void countNodes__multiple_matching_nodes_and_distractors_and_multiple_labels_per_node() throws InterruptedException {
        inTx(db, () -> db.createNode(label("Token"), label("Elphi")));
        inTx(db, () -> db.createNode(label("Token")));
        inTx(db, () -> db.createNode(label("Pescado"), label("Merci")));
        inTx(db, () -> db.createNode(label("La Barbe")));
        inTx(db, () -> db.createNode(label("Teenie"), label("Token"), label("Token")));
        final Dimension tokens = dim().withName("Token").mock();

        final long count = rob.countNodes(tokens);

        assertThat(count, is(3L));
    }

    @Test
    public void countNodes__no_matching_nodes() throws InterruptedException {
        inTx(db, () -> db.createNode(label("Token1"), label("Elphi")));
        inTx(db, () -> db.createNode(label("Token2")));
        inTx(db, () -> db.createNode(label("Pescado"), label("Merci")));
        inTx(db, () -> db.createNode(label("La Barbe")));
        inTx(db, () -> db.createNode(label("Teenie"), label("Token3"), label("Token4")));
        final Dimension tokens = dim().withName("Token5").mock();

        final long count = rob.countNodes(tokens);

        assertThat(count, is(0L));
    }

    @Test
    public void countAllOccurrences__empty_DB() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();


        final long count = rob.countAllValues(sentences, tokens);

        assertThat(count, is(0L));
    }

    @Test
    public void countAllOccurrences__one_occurrence() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Word") //
            ) //
            .commit();


        final long count = rob.countAllValues(sentences, tokens);

        assertThat(count, is(1L));
    }

    @Test
    public void countAllOccurrences__three_occurrences_in_same_sentence() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Three", "words", ".") //
            ) //
            .commit();


        final long count = rob.countAllValues(sentences, tokens);

        assertThat(count, is(3L));
    }

    @Test
    public void countAllOccurrences__multiple_sentences() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Three", "words", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("E") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Second", "sentence", "with", "words", ".") //
            ) //
            .commit();


        final long count = rob.countAllValues(sentences, tokens);

        assertThat(count, is(8L));
    }

    @Test
    public void countAllOccurrences__null_parent_Dimension() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Word") //
            ) //
            .commit();


        final long count = rob.countAllValues(null, tokens);

        assertThat(count, is(0L));
    }

    @Test
    public void countAllOccurrences__null_leaf_Dimension() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Word") //
            ) //
            .commit();


        final long count = rob.countAllValues(sentences, null);

        assertThat(count, is(0L));
    }

    @Test
    public void countAllOccurrences__null_null_Dimension() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Word") //
            ) //
            .commit();

        final long count = rob.countAllValues(null, null);

        assertThat(count, is(0L));
    }

    @Test
    public void countAllOccurrences__multiple_sentences_and_multiple_dimensions() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Three", "words", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("E") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Second", "sentence", "with", "words", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("LCToken") //
                    .withLeaves("three", "words", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("E") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("LCToken") //
                    .withLeaves("second", "sentence", "with", "words", ".") //
            ) //
            .commit();


        final long count = rob.countAllValues(sentences, tokens);

        assertThat(count, is(8L));
    }

    @Test
    public void countAllOccurrences__multiple_sentences_and_multiple_dimensions_2() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Three", "words", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("s") //
                    .withRootDimension("LCSentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Second", "sentence", "with", "words", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("s") //
                    .withRootDimension("LCSentence") //
                    .withLeafDimension("LCToken") //
                    .withLeaves("three", "words", ".") //
            ) //
            .commit();


        final long count = rob.countAllValues(sentences, tokens);

        assertThat(count, is(3L));
    }

    @Test
    public void countOccurrences__empty_DB() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();


        final long count = rob.countOccurrences(str("word"), sentences, tokens);

        assertThat(count, is(0L));
    }

    @Test
    public void countOccurrences__one_occurrence() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Word") //
            ) //
            .commit();


        final long count = rob.countOccurrences(str("Word"), sentences, tokens);

        assertThat(count, is(1L));
    }

    @Test
    public void countOccurrences__one_of_three_in_same_sentence() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Three", "words", ".") //
            ) //
            .commit();


        final long count = rob.countOccurrences(str("words"), sentences, tokens);

        assertThat(count, is(1L));
    }


    @Test
    public void countOccurrences__three_of_three_in_same_sentence() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("words", "words", "words") //
            ) //
            .commit();


        final long count = rob.countOccurrences(str("words"), sentences, tokens);

        assertThat(count, is(3L));
    }

    @Test
    public void countOccurrences__two_of_three_in_same_sentence_case_sensitive() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Words", "words", "words") //
            ) //
            .commit();


        final long count = rob.countOccurrences(str("words"), sentences, tokens);

        assertThat(count, is(2L));
    }

    @Test
    public void countOccurrences__multiple_sentences() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Three", "words", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("E") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Second", "sentence", "with", "words", ".") //
            ) //
            .commit();


        final long count = rob.countOccurrences(str("words"), sentences, tokens);

        assertThat(count, is(2L));
    }

    @Test
    public void countOccurrences__null_parent_Dimension() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Word") //
            ) //
            .commit();


        final long count = rob.countOccurrences(str("Word"), null, tokens);

        assertThat(count, is(0L));
    }

    @Test
    public void countOccurrences__null_leaf_Dimension() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Word") //
            ) //
            .commit();


        final long count = rob.countOccurrences(str("Word"), sentences, null);

        assertThat(count, is(0L));
    }

    @Test
    public void countOccurrences__null_null_Dimension() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Word") //
            ) //
            .commit();

        final long count = rob.countOccurrences(str("Words"), null, null);

        assertThat(count, is(0L));
    }

    @Test
    public void countOccurrences__null_Value() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Word") //
            ) //
            .commit();


        final long count = rob.countOccurrences(null, sentences, tokens);

        assertThat(count, is(0L));
    }

    @Test
    public void countOccurrences__multiple_sentences_and_multiple_dimensions() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Three", "words", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("E") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Second", "sentence", "with", "words", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("LCToken") //
                    .withLeaves("three", "words", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("E") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("LCToken") //
                    .withLeaves("second", "sentence", "with", "words", ".") //
            ) //
            .commit();


        final long count = rob.countOccurrences(str("words"), sentences, tokens);

        assertThat(count, is(2L));
    }

    @Test
    public void countOccurrences__multiple_sentences_and_multiple_dimensions_2() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Three", "words", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("s") //
                    .withRootDimension("LCSentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Second", "sentence", "with", "words", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("s") //
                    .withRootDimension("LCSentence") //
                    .withLeafDimension("LCToken") //
                    .withLeaves("three", "words", ".") //
            ) //
            .commit();


        final long count = rob.countOccurrences(str("words"), sentences, tokens);

        assertThat(count, is(1L));
    }

    @Test
    public void streamNodes__null_arg() {
        final DriverReadOnlyBackend rob = readOnlyBackend();


        final List<org.neo4j.driver.v1.types.Node> tokenNodes = rob.streamNodes(null).collect(toList());

        assertThat(tokenNodes.size(), is(0));
    }

    @Test
    public void streamNodes__no_Tokens() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();

        final List<org.neo4j.driver.v1.types.Node> tokenNodes = rob.streamNodes(tokens).collect(toList());

        assertThat(tokenNodes.size(), is(0));
    }

    @Test
    public void streamNodes__one_Token() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Word") //
            ) //
            .commit();


        final List<org.neo4j.driver.v1.types.Node> tokenNodes = rob.streamNodes(tokens).collect(toList());

        assertThat(tokenNodes.size(), is(1));
        assertThat(tokenNodes, hasItem(aNode().withId("Word")));
    }

    @Test
    public void streamNodes__multiple_Tokens_from_same_Sentence() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Some", "words", "come", "with", "other", "words", ".") //
            ) //
            .commit();

        final List<org.neo4j.driver.v1.types.Node> tokenNodes = rob.streamNodes(tokens).collect(toList());

        assertThat(tokenNodes.size(), is(6));
        assertThat(tokenNodes, hasItems(//
            aNode().withId("Some"), //
            aNode().withId("words"), //
            aNode().withId("come"), //
            aNode().withId("with"), //
            aNode().withId("other"), //
            aNode().withId(".") //
            )
        );
    }


    @Test
    public void streamNodes__multiple_Tokens_from_multiple_Sentences() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Some", "words", "come", "with", "other", "words", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S2") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("And", "words", "occur", "in", "sentences", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S3") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Words", "are", "case-sensitive", ".") //
            ) //
            .commit();

        final List<org.neo4j.driver.v1.types.Node> tokenNodes = rob.streamNodes(tokens).collect(toList());

        assertThat(tokenNodes.size(), is(13));
        assertThat(tokenNodes, hasItems(//
            aNode().withId("Some"), //
            aNode().withId("words"), //
            aNode().withId("Words"), //
            aNode().withId("come"), //
            aNode().withId("with"), //
            aNode().withId("other"), //
            aNode().withId("occur"), //
            aNode().withId("in"), //
            aNode().withId("sentences"), //
            aNode().withId("are"), //
            aNode().withId("case-sensitive"), //
            aNode().withId("And"), //
            aNode().withId(".") //
            )
        );
    }

    @Test
    public void streamNodes__multiple_Tokens_from_multiple_Sentences_and_distractors() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Some", "words", "come", "with", "other", "words", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("UCToken") //
                    .withLeaves("SOME", "WORDS", "COME", "IN", "UPPERCASE", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S2") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("And", "words", "occur", "in", "sentences", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S3") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Words", "are", "case-sensitive", ".") //
            ) //
            .commit();

        final List<org.neo4j.driver.v1.types.Node> tokenNodes = rob.streamNodes(tokens).collect(toList());

        assertThat(tokenNodes.size(), is(13));
        assertThat(tokenNodes, hasItems(//
            aNode().withId("Some"), //
            aNode().withId("words"), //
            aNode().withId("Words"), //
            aNode().withId("come"), //
            aNode().withId("with"), //
            aNode().withId("other"), //
            aNode().withId("occur"), //
            aNode().withId("in"), //
            aNode().withId("sentences"), //
            aNode().withId("are"), //
            aNode().withId("case-sensitive"), //
            aNode().withId("And"), //
            aNode().withId(".") //
            )
        );
    }

    @Test
    public void streamNeighbours__no_Tokens() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();

        final List<org.neo4j.driver.v1.types.Node> tokenNodes = rob.streamNeighbours(str("Word"), sentences, tokens, 1).collect(toList());

        assertThat(tokenNodes.size(), is(0));
    }

    @Test
    public void streamNeighbours__no_neighbours() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Word") //
            ) //
            .commit();


        final List<org.neo4j.driver.v1.types.Node> tokenNodes = rob.streamNeighbours(str("Word"), sentences, tokens, 1).collect(toList());

        assertThat(tokenNodes.size(), is(0));
    }

    @Test
    public void streamNeighbours__one_following_Token() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Word", "one", ".") //
            ) //
            .commit();


        final List<org.neo4j.driver.v1.types.Node> tokenNodes = rob.streamNeighbours(str("Word"), sentences, tokens, 1).collect(toList());

        assertThat(tokenNodes.size(), is(1));
        assertThat(tokenNodes, hasItem(aNode().withId("one")));
    }


    @Test
    public void streamNeighbours__null_self_arg() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Word", "one", ".") //
            ) //
            .commit();


        final List<org.neo4j.driver.v1.types.Node> tokenNodes = rob.streamNeighbours(null, sentences, tokens, 1).collect(toList());

        assertThat(tokenNodes.size(), is(0));
    }


    @Test
    public void streamNeighbours__null_parentDimension_arg() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Word", "one", ".") //
            ) //
            .commit();


        final List<org.neo4j.driver.v1.types.Node> tokenNodes = rob.streamNeighbours(str("Word"), null, tokens, 1).collect(toList());

        assertThat(tokenNodes.size(), is(0));
    }

    @Test
    public void streamNeighbours__null_leafDimension_arg() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Word", "one", ".") //
            ) //
            .commit();


        final List<org.neo4j.driver.v1.types.Node> tokenNodes = rob.streamNeighbours(str("Word"), sentences, null, 1).collect(toList());

        assertThat(tokenNodes.size(), is(0));
    }

    @Test
    public void streamNeighbours__one_preceding_Token() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Word", "one", ".") //
            ) //
            .commit();


        final List<org.neo4j.driver.v1.types.Node> tokenNodes = rob.streamNeighbours(str("one"), sentences, tokens, -1).collect(toList());

        assertThat(tokenNodes.size(), is(1));
        assertThat(tokenNodes, hasItem(aNode().withId("Word")));
    }

    @Test
    public void streamNeighbours__self_is_the_closest_neighbour_but_apparently_it_doesnt_match() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Word", "one", ".") //
            ) //
            .commit();


        final List<org.neo4j.driver.v1.types.Node> tokenNodes = rob.streamNeighbours(str("one"), sentences, tokens, 0).collect(toList());

        assertThat(tokenNodes.size(), is(0));
    }

    @Test
    public void streamNeighbours__one_neighbour_away() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Word", "one", ".") //
            ) //
            .commit();


        final List<org.neo4j.driver.v1.types.Node> tokenNodes = rob.streamNeighbours(str("Word"), sentences, tokens, 2).collect(toList());

        assertThat(tokenNodes.size(), is(1));
        assertThat(tokenNodes, hasItem(aNode().withId(".")));
    }


    @Test
    public void streamNeighbours__across_multiple_sentences() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Sentence", "with", "one", "word", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S2") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Not", "really", "just", "one", "word", ".") //
            ) //
            .commit();


        final List<org.neo4j.driver.v1.types.Node> tokenNodes = rob.streamNeighbours(str("word"), sentences, tokens, -2).collect(toList());

        assertThat(tokenNodes.size(), is(2));
        assertThat(tokenNodes, hasItem(aNode().withId("with")));
        assertThat(tokenNodes, hasItem(aNode().withId("just")));
    }

    @Test
    public void streamNeighbours__across_multiple_sentences_same_neighbour_occurs_twice() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Sentence", "with", "one", "word", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S2") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Not", "really", "just", "one", "word", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S3") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("But", "really", "just", "a", "word", ".") //
            ) //
            .commit();


        final List<org.neo4j.driver.v1.types.Node> tokenNodes = rob.streamNeighbours(str("word"), sentences, tokens, -2).collect(toList());

        assertThat(tokenNodes.size(), is(3));
        assertThat(tokenNodes, hasItems( //
            aNode().withId("with"),//
            aNode().withId("just"),//
            aNode().withId("just")//
        ));
    }


    @Test
    public void streamNeighbours__across_multiple_sentences_with_distractors() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Sentence", "with", "one", "word", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("LCSentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("sentence", "with", "one", "word", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S2") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Not", "really", "just", "one", "word", ".") //
            ) //
            .commit();


        final List<org.neo4j.driver.v1.types.Node> tokenNodes = rob.streamNeighbours(str("word"), sentences, tokens, -2).collect(toList());

        assertThat(tokenNodes.size(), is(2));
        assertThat(tokenNodes, hasItem(aNode().withId("with")));
        assertThat(tokenNodes, hasItem(aNode().withId("just")));
    }


    @Test
    public void streamAggregatedNeighbours__no_Tokens() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();

        final List<AggregatedNeighbour<String>> neighbours = rob.streamAggregatedNeighbours(str("Word"), sentences, tokens, 1).collect(toList());

        assertThat(neighbours.size(), is(0));
    }

    @Test
    public void streamAggregatedNeighbours__null_self_arg() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Word", "one", ".") //
            ) //
            .commit();


        final List<AggregatedNeighbour<Object>> neighbours = rob.streamAggregatedNeighbours(null, sentences, tokens, 1).collect(toList());

        assertThat(neighbours.size(), is(0));
    }


    @Test
    public void streamAggregatedNeighbours__null_parentDimension_arg() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Word", "one", ".") //
            ) //
            .commit();


        final List<AggregatedNeighbour<String>> neighbours = rob.streamAggregatedNeighbours(str("Word"), null, tokens, 1).collect(toList());

        assertThat(neighbours.size(), is(0));
    }

    @Test
    public void streamAggregatedNeighbours__null_leafDimension_arg() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Word", "one", ".") //
            ) //
            .commit();


        final List<AggregatedNeighbour<String>> neighbours = rob.streamAggregatedNeighbours(str("Word"), sentences, null, 1).collect(toList());

        assertThat(neighbours.size(), is(0));
    }

    @Test
    public void streamAggregatedNeighbours__one_preceding_Token() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Word", "one", ".") //
            ) //
            .commit();


        final List<AggregatedNeighbour<String>> neighbours = rob.streamAggregatedNeighbours(str("one"), sentences, tokens, -1).collect(toList());

        assertThat(neighbours, hasItem(//
            anAggregatedNeighbour().withNeighbourId("Word").withSelfId("one").withVicinity(-1).withCount(1L)//
        ));
        assertThat(neighbours.size(), is(1));
    }

    @Test
    public void streamAggregatedNeighbours__three_distinct_preceding_Tokens() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S1") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Word", "one", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S2") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Insight", "one", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S3") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("This", "is", "one", ".") //
            ) //
            .commit();


        final List<AggregatedNeighbour<String>> neighbours = rob.streamAggregatedNeighbours(str("one"), sentences, tokens, -1).collect(toList());

        assertThat(neighbours, hasItems(//
            anAggregatedNeighbour().withSelfId("one").withVicinity(-1).withCount(1L).withNeighbourId("Word"),//
            anAggregatedNeighbour().withSelfId("one").withVicinity(-1).withCount(1L).withNeighbourId("Insight"),//
            anAggregatedNeighbour().withSelfId("one").withVicinity(-1).withCount(1L).withNeighbourId("is")//
        ));
        assertThat(neighbours.size(), is(3));
    }


    @Test
    public void streamAggregatedNeighbours__three_distinct_preceding_Tokens_with_multiple_precedences() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S1") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Word", "one", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S2") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Insight", "one", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S3") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("This", "is", "one", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S4") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Word", "one", "occurs", "twice", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S5") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("This", "is", "one", ",", "too", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S6") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("There", "is", "one", "good", "insight", ".") //
            ) //
            .commit();


        final List<AggregatedNeighbour<String>> countedNeighbours = rob.streamAggregatedNeighbours(str("one"), sentences, tokens, -1).collect(toList());

        assertThat(countedNeighbours, hasItems(//
            anAggregatedNeighbour().withSelfId("one").withVicinity(-1).withCount(2L).withNeighbourId("Word"),//
            anAggregatedNeighbour().withSelfId("one").withVicinity(-1).withCount(1L).withNeighbourId("Insight"),//
            anAggregatedNeighbour().withSelfId("one").withVicinity(-1).withCount(3L).withNeighbourId("is")//
        ));
        assertThat(countedNeighbours.size(), is(3));
    }


    @Test
    public void streamNodeValues__no_Values() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();

        final List<Value<Object>> values = rob.streamNodeValues(tokens, Object.class).collect(toList());

        assertThat(values.size(), is(0));
    }

    @Test
    public void streamNodeValues__one_String_Value() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Word") //
            ) //
            .commit();


        final List<Value<String>> values = rob.streamNodeValues(tokens, String.class).collect(toList());

        //noinspection unchecked
        assertThat(values, hasItem(aValueAs(String.class).withIdentifier("Word")));
        assertThat(values.size(), is(1));
    }

    @Test
    public void streamNodeValues__multiple_String_Values() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("This", "is", "a", "sentence", ".") //
            ) //
            .commit();


        final List<Value<String>> values = rob.streamNodeValues(tokens, String.class).collect(toList());

        //noinspection unchecked
        assertThat(values, hasItems(//
            aValueAs(String.class).withIdentifier("This"),//
            aValueAs(String.class).withIdentifier("is"),//
            aValueAs(String.class).withIdentifier("a"),//
            aValueAs(String.class).withIdentifier("sentence"),//
            aValueAs(String.class).withIdentifier(".")//
        ));
        assertThat(values.size(), is(5));
    }

    @Test
    public void streamNodeValues__one_Integer_Value() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension numbers = dim().withName("Number").mock();
        graph.writer(noInit) //
            .add( //
                anIntegerSequence()//
                    .withRoot(0) //
                    .withRootDimension("ParentNumber") //
                    .withLeafDimension("Number") //
                    .withLeaves(1) //
            ) //
            .commit();


        final List<Value<Integer>> values = rob.streamNodeValues(numbers, Integer.class).collect(toList());

        //noinspection unchecked
        assertThat(values, hasItem(aValueAs(Integer.class).withIdentifier(1)));
        assertThat(values.size(), is(1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void streamNodeValues__rejects_unsupported_type_arg() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Word") //
            ) //
            .commit();

        rob.streamNodeValues(tokens, Value.class);
    }


    @Test
    public void streamNodesWithPosition__no_Values() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension sentences = dim().withName("Sentence").mock();
        final Dimension tokens = dim().withName("Token").mock();

        final List<ValueWithPosition> values = rob.streamNodesWithPosition(sentences, tokens, String.class).collect(toList());

        assertThat(values.size(), is(0));
    }

    @Test
    public void streamNodesWithPosition__one_Token() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension sentences = dim().withName("Sentence").mock();
        final Dimension tokens = dim().withName("Token").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("First") //
            ) //
            .commit();

        final List<ValueWithPosition> values = rob.streamNodesWithPosition(sentences, tokens, String.class).collect(toList());

        //noinspection unchecked
        assertThat(values, hasItem(aVwp(String.class).withIdentifier("First").withPosition(0)));
        assertThat(values.size(), is(1));
    }

    @Test
    public void streamNodesWithPosition__multiple_Tokens_same_Sentence() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension sentences = dim().withName("Sentence").mock();
        final Dimension tokens = dim().withName("Token").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("First", "ladies", "first", "!") //
            ) //
            .commit();

        final List<ValueWithPosition> values = rob.streamNodesWithPosition(sentences, tokens, String.class).collect(toList());

        //noinspection unchecked
        assertThat(values, hasItems(//
            aVwp(String.class).withIdentifier("First").withPosition(0),//
            aVwp(String.class).withIdentifier("ladies").withPosition(1),//
            aVwp(String.class).withIdentifier("first").withPosition(2),//
            aVwp(String.class).withIdentifier("!").withPosition(3)//
        ));
        assertThat(values.size(), is(4));
    }

    @Test
    public void streamNodesWithPosition__multiple_Tokens_reoccurring_in_multiple_sentences() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension sentences = dim().withName("Sentence").mock();
        final Dimension tokens = dim().withName("Token").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("First", "ladies", "first", "!") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S2") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Rather", "than", "America", "first", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S3") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("First", "ladies", "come", "before", "America", ".") //
            ) //
            .commit();

        final List<ValueWithPosition> values = rob.streamNodesWithPosition(sentences, tokens, String.class).collect(toList());

        //noinspection unchecked
        assertThat(values, hasItems(//
            aVwp(String.class).withIdentifier("First").withPosition(0),//
            aVwp(String.class).withIdentifier("First").withPosition(0),//
            aVwp(String.class).withIdentifier("Rather").withPosition(0),//
            aVwp(String.class).withIdentifier("ladies").withPosition(1),//
            aVwp(String.class).withIdentifier("ladies").withPosition(1),//
            aVwp(String.class).withIdentifier("than").withPosition(1),//
            aVwp(String.class).withIdentifier("come").withPosition(2),//
            aVwp(String.class).withIdentifier("first").withPosition(2),//
            aVwp(String.class).withIdentifier("America").withPosition(2),//
            aVwp(String.class).withIdentifier("!").withPosition(3),//
            aVwp(String.class).withIdentifier("before").withPosition(3),//
            aVwp(String.class).withIdentifier("first").withPosition(3),//
            aVwp(String.class).withIdentifier(".").withPosition(4),//
            aVwp(String.class).withIdentifier("America").withPosition(4),//
            aVwp(String.class).withIdentifier(".").withPosition(5)//
        ));
        assertThat(values.size(), is(15));
    }

    @Test
    public void streamNodesWithPosition__null_parentDimension_arg() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("First") //
            ) //
            .commit();

        final List<ValueWithPosition> values = rob.streamNodesWithPosition(null, tokens, String.class).collect(toList());

        //noinspection unchecked
        assertThat(values.size(), is(0));
    }

    @Test
    public void streamNodesWithPosition__null_leafDimension_arg() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("First") //
            ) //
            .commit();

        final List<ValueWithPosition> values = rob.streamNodesWithPosition(sentences, null, String.class).collect(toList());

        //noinspection unchecked
        assertThat(values.size(), is(0));
    }

    @Test(expected = IllegalArgumentException.class)
    public void streamNodesWithPosition__null_type_arg() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension sentences = dim().withName("Sentence").mock();
        final Dimension tokens = dim().withName("Token").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("First") //
            ) //
            .commit();

        rob.streamNodesWithPosition(sentences, tokens, null);
    }

    @Test
    public void streamAggregatedValues__no_Values() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension sentences = dim().withName("Sentence").mock();
        final Dimension tokens = dim().withName("Token").mock();

        final List<AggregatedValue<String>> values = rob.streamFullyAggregatedValues(sentences, tokens, String.class).collect(toList());

        assertThat(values.size(), is(0));
    }

    @Test
    public void streamAggregatedValues__one_Value() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension sentences = dim().withName("Sentence").mock();
        final Dimension tokens = dim().withName("Token").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Chat") //
            ) //
            .commit();

        final List<AggregatedValue<String>> values = rob.streamFullyAggregatedValues(sentences, tokens, String.class).collect(toList());

        //noinspection unchecked
        assertThat(values, hasItem(//
            anAggrValue().withIdentifier("Chat").withCount(1L).withCountInPosition(0, 1L).occurringOnlyAt(0)//
        ));
        assertThat(values.size(), is(1));
    }

    @Test
    public void streamAggregatedValues__multiple_distinct_Values_same_sequence() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension sentences = dim().withName("Sentence").mock();
        final Dimension tokens = dim().withName("Token").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("J", "'", "aime", "le", "Chat", "Noir", "!") //
            ) //
            .commit();

        final List<AggregatedValue<String>> values = rob.streamFullyAggregatedValues(sentences, tokens, String.class).collect(toList());

        //noinspection unchecked
        assertThat(values, hasItems(//
            anAggrValue().withCount(1L).withCountInPosition(0, 1L).withIdentifier("J").occurringOnlyAt(0),//
            anAggrValue().withCount(1L).withCountInPosition(1, 1L).withIdentifier("'").occurringOnlyAt(1),//
            anAggrValue().withCount(1L).withCountInPosition(2, 1L).withIdentifier("aime").occurringOnlyAt(2),//
            anAggrValue().withCount(1L).withCountInPosition(3, 1L).withIdentifier("le").occurringOnlyAt(3),//
            anAggrValue().withCount(1L).withCountInPosition(4, 1L).withIdentifier("Chat").occurringOnlyAt(4),//
            anAggrValue().withCount(1L).withCountInPosition(5, 1L).withIdentifier("Noir").occurringOnlyAt(5),//
            anAggrValue().withCount(1L).withCountInPosition(6, 1L).withIdentifier("!").occurringOnlyAt(6)//
        ));
        assertThat(values.size(), is(7));
    }

    @Test
    public void streamAggregatedValues__multiple_Values_in_different_positions() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension sentences = dim().withName("Sentence").mock();
        final Dimension tokens = dim().withName("Token").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("J", "'", "aime", "le", "chat", "noir", "!") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S2") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Et", "aussi", "un", "chat", "rouge", ".") //
            ) //
            .commit();


        final List<AggregatedValue<String>> values = rob.streamFullyAggregatedValues(sentences, tokens, String.class).collect(toList());

        //noinspection unchecked
        assertThat(values, hasItems(//
            anAggrValue().withCount(1L).withCountInPosition(0, 1L).withIdentifier("J").occurringOnlyAt(0),//
            anAggrValue().withCount(1L).withCountInPosition(0, 1L).withIdentifier("Et").occurringOnlyAt(0),//
            anAggrValue().withCount(1L).withCountInPosition(1, 1L).withIdentifier("'").occurringOnlyAt(1),//
            anAggrValue().withCount(1L).withCountInPosition(1, 1L).withIdentifier("aussi").occurringOnlyAt(1),//
            anAggrValue().withCount(1L).withCountInPosition(2, 1L).withIdentifier("un").occurringOnlyAt(2),//
            anAggrValue().withCount(1L).withCountInPosition(2, 1L).withIdentifier("aime").occurringOnlyAt(2),//
            anAggrValue().withCount(1L).withCountInPosition(3, 1L).withIdentifier("le").occurringOnlyAt(3),//
            anAggrValue().withCount(2L).withCountInPosition(4, 1L).withIdentifier("chat").occurringOnlyAt(3, 4).withCountInPosition(3, 1L),//
            anAggrValue().withCount(1L).withCountInPosition(4, 1L).withIdentifier("rouge").occurringOnlyAt(4),//
            anAggrValue().withCount(1L).withCountInPosition(5, 1L).withIdentifier("noir").occurringOnlyAt(5),//
            anAggrValue().withCount(1L).withCountInPosition(5, 1L).withIdentifier(".").occurringOnlyAt(5),//
            anAggrValue().withCount(1L).withCountInPosition(6, 1L).withIdentifier("!").occurringOnlyAt(6)//
        ));
        assertThat(values.size(), is(12));
    }


    @Test
    public void streamAggregatedValues__multiple_Values_in_same_positions() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension sentences = dim().withName("Sentence").mock();
        final Dimension tokens = dim().withName("Token").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("J", "'", "aime", "un", "chat", "noir", "!") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S2") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Et", "aussi", "un", "chat", "rouge", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S3") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Un", "chat", "noir", "et", "un", "chat", "rouge", "!") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S4") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Et", "aussi", "un", "chat", "gris", ".") //
            ) //
            .commit();


        final List<AggregatedValue<String>> values = rob.streamFullyAggregatedValues(sentences, tokens, String.class).collect(toList());

        //noinspection unchecked
        assertThat(values, hasItems(//
            anAggrValue().withCount(1L).withCountInPosition(0, 1L).withIdentifier("J").occurringOnlyAt(0),//
            anAggrValue().withCount(2L).withCountInPosition(0, 2L).withIdentifier("Et").occurringOnlyAt(0),//
            anAggrValue().withCount(1L).withCountInPosition(0, 1L).withIdentifier("Un").occurringOnlyAt(0),//
            anAggrValue().withCount(1L).withCountInPosition(1, 1L).withIdentifier("'").occurringOnlyAt(1),//
            anAggrValue().withCount(2L).withCountInPosition(1, 2L).withIdentifier("aussi").occurringOnlyAt(1),//
            anAggrValue().withCount(4L).withCountInPosition(2, 2L).withIdentifier("un").occurringOnlyAt(2, 3, 4).withCountInPosition(4, 1L).withCountInPosition(3, 1L),//
            anAggrValue().withCount(1L).withCountInPosition(2, 1L).withIdentifier("aime").occurringOnlyAt(2),//
            anAggrValue().withCount(5L)
                .withIdentifier("chat").occurringOnlyAt(1, 3, 4, 5)
                .withCountInPosition(1, 1L)
                .withCountInPosition(3, 2L)
                .withCountInPosition(4, 1L)
                .withCountInPosition(5, 1L),//
            anAggrValue().withCount(2L).withCountInPosition(4, 1L).withIdentifier("rouge").occurringOnlyAt(4, 6).withCountInPosition(6, 1),//
            anAggrValue().withCount(1L).withCountInPosition(4, 1L).withIdentifier("gris").occurringOnlyAt(4),//
            anAggrValue().withCount(1L).withCountInPosition(3, 1L).withIdentifier("et").occurringOnlyAt(3),//
            anAggrValue().withCount(2L).withCountInPosition(5, 1L).withIdentifier("noir").occurringOnlyAt(2, 5).withCountInPosition(2, 1L),//
            anAggrValue().withCount(2L).withCountInPosition(5, 2L).withIdentifier(".").occurringOnlyAt(5),//
            anAggrValue().withCount(2L).withCountInPosition(6, 1L).withIdentifier("!").occurringOnlyAt(6, 7).withCountInPosition(7, 1L)//
        ));
        assertThat(values.size(), is(14));
    }

    @Test
    public void streamAggregatedValues__null_parentDimension_arg() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension tokens = dim().withName("Token").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("First") //
            ) //
            .commit();

        final List<AggregatedValue<String>> values = rob.streamFullyAggregatedValues(null, tokens, String.class).collect(toList());

        //noinspection unchecked
        assertThat(values.size(), is(0));
    }

    @Test
    public void streamAggregatedValues__null_leafDimension_arg() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("First") //
            ) //
            .commit();

        final List<AggregatedValue<String>> values = rob.streamFullyAggregatedValues(sentences, null, String.class).collect(toList());

        //noinspection unchecked
        assertThat(values.size(), is(0));
    }

    @Test(expected = IllegalArgumentException.class)
    public void streamAggregatedValues__null_type_arg() {
        final DriverReadOnlyBackend rob = readOnlyBackend();
        final Dimension sentences = dim().withName("Sentence").mock();
        final Dimension tokens = dim().withName("Token").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("First") //
            ) //
            .commit();

        rob.streamFullyAggregatedValues(sentences, tokens, null);
    }



    private static DriverReadOnlyBackend readOnlyBackend() {
        return new DriverReadOnlyBackend(GraphDatabase.driver("bolt://" + localhost_7687), newAggregator());
    }

}
