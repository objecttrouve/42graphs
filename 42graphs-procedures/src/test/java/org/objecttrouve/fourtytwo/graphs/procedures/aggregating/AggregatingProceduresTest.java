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

import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.Neo4jRule;
import org.objecttrouve.fourtytwo.graphs.api.Dimension;
import org.objecttrouve.fourtytwo.graphs.backend.init.EmbeddedBackend;
import org.objecttrouve.fourtytwo.graphs.procedures.quantities.QuantityProcedures;

import java.util.Map;

import static java.util.Optional.ofNullable;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.objecttrouve.fourtytwo.graphs.matchers.NeoDbMatcher.aGraph;
import static org.objecttrouve.fourtytwo.graphs.matchers.NeoDbMatcher.theEmptyGraph;
import static org.objecttrouve.fourtytwo.graphs.matchers.NeoNodeMatcher.aNode;
import static org.objecttrouve.fourtytwo.graphs.mocks.DimensionMock.dim;
import static org.objecttrouve.fourtytwo.graphs.mocks.TestStringSequenceTree.aStringSequence;

public class AggregatingProceduresTest {

    private static final boolean noInit = false;
    private static final Dimension tokens = dim().withName("Token").mock();
    private static final Dimension sentences = dim().withName("Sentence").mock();

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
        .withProcedure(QuantityProcedures.class)
        .withProcedure(AggregatingProcedures.class);

    private EmbeddedBackend graph;
    private Driver driver;
    private GraphDatabaseService db;

    @Before
    public void setup() {
        graph = new EmbeddedBackend(//
            () -> neo4j.getGraphDatabaseService(), () -> {
            throw new UnsupportedOperationException("Won't use it here. ");
        });
        driver = GraphDatabase.driver(
            neo4j.boltURI(),
            Config.build().withoutEncryption().toConfig()
        );
        this.db = neo4j.getGraphDatabaseService();
    }

    @Test
    public void aggregateDirectNeighbourCount__empty_DB() {

        this.callAggregateDirectNeighbourCount(sentences, tokens);

        assertThat(db, is(theEmptyGraph()));
    }

    @Test
    public void aggregateDirectNeighbourCount__one_lonely_token() {

        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Word") //
            ) //
            .commit();

        this.callAggregateDirectNeighbourCount(sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("Word")
                .withPropDirectNeighbourCount(0L)
        )));
    }

    @Test
    public void aggregateDirectNeighbourCount__one_token__with_a_preceding_token() {

        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("one", "Word") //
            ) //
            .commit();

        this.callAggregateDirectNeighbourCount(sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("Word")
                .withPropDirectNeighbourCount(1L)
        )));
    }

    @Test
    public void aggregateDirectNeighbourCount__one_token__with_a_following_token() {

        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Word", "one") //
            ) //
            .commit();

        this.callAggregateDirectNeighbourCount(sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("Word")
                .withPropDirectNeighbourCount(1L)
        )));
    }

    @Test
    public void aggregateDirectNeighbourCount__two_occurrences__in_two_sentences__with_a_following_token() {

        graph.writer(noInit) //
            .add(
                aStringSequence()
                    .withRoot("S")
                    .withRootDimension("Sentence")
                    .withLeafDimension("Token")
                    .withLeaves("Word", "one")
            )
            .add(
                aStringSequence()
                    .withRoot("T")
                    .withRootDimension("Sentence")
                    .withLeafDimension("Token")
                    .withLeaves("Word", "two")
            )
            .commit();

        this.callAggregateDirectNeighbourCount(sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("Word")
                .withPropDirectNeighbourCount(2L)
        )));
    }

    @Test
    public void aggregateDirectNeighbourCount__two_occurrences__in_two_sentences__with_a_preceding_token() {

        graph.writer(noInit) //
            .add(
                aStringSequence()
                    .withRoot("S")
                    .withRootDimension("Sentence")
                    .withLeafDimension("Token")
                    .withLeaves("one", "Word")
            )
            .add(
                aStringSequence()
                    .withRoot("T")
                    .withRootDimension("Sentence")
                    .withLeafDimension("Token")
                    .withLeaves("the", "Word")
            )
            .commit();

        this.callAggregateDirectNeighbourCount(sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("Word")
                .withPropDirectNeighbourCount(2L)
        )));
    }

    @Test
    public void aggregateDirectNeighbourCount__null_parentDimension_arg() {

        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Word", "one", ".") //
            ) //
            .commit();

        this.callAggregateDirectNeighbourCount(null, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("Word")
                .withPropDirectNeighbourCount(0L)
        )));
    }

    @Test
    public void aggregateDirectNeighbourCount__null_leafDimension_arg() {

        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Word", "one", ".") //
            ) //
            .commit();

        this.callAggregateDirectNeighbourCount(sentences, null);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("Word")
                .withPropDirectNeighbourCount(0L)
        )));
    }

    @Test
    public void aggregateDirectNeighbourCount__one_token__with_a_preceding_and_a_following_token() {

        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("my", "Word", "one") //
            ) //
            .commit();

        this.callAggregateDirectNeighbourCount(sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("Word")
                .withPropDirectNeighbourCount(2L)
        )));
    }



    @Test
    public void aggregateDirectNeighbourCount__multiple_occurrences__with_multiple_neighbours__in_distinct_sentences() {

        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("1") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("my", "Word", "one") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("2") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("your", "Word", "two") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("3") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("one", "Word", "here") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("4") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("one", "Word", "there") //
            ) //
            .commit();

        this.callAggregateDirectNeighbourCount(sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("Word")
                .withPropDirectNeighbourCount(3L + 3L)
        )));
    }


    @Test
    public void aggregateDirectNeighbourCount__multiple_occurrences__with_multiple_neighbours__in_same_sentence() {

        graph.writer(noInit)
            .add(
                aStringSequence()
                    .withRoot("1")
                    .withRootDimension("Sentence")
                    .withLeafDimension("Token")
                    .withLeaves("my", "Word", "one", "your", "Word", "two")
            )
            .commit();

        this.callAggregateDirectNeighbourCount(sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("Word")
                .withPropDirectNeighbourCount(4L)
        )));
    }

    @Test
    public void aggregateDirectNeighbourCount__multiple_occurrences__with_multiple_neighbours__in_same_sentence__with_duplicates() {

        graph.writer(noInit)
            .add(
                aStringSequence()
                    .withRoot("1")
                    .withRootDimension("Sentence")
                    .withLeafDimension("Token")
                    .withLeaves("my", "Word", "one", "your", "Word", "two", "my", "Word", "one")
            )
            .commit();

        this.callAggregateDirectNeighbourCount(sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("Word")
                .withPropDirectNeighbourCount(4L)
        )));
    }

    @Test
    public void aggregateDirectNeighbourCount__multiple_occurrences__with_multiple_neighbours__in_same_sentence__with_inverted_duplicates() {

        graph.writer(noInit)
            .add(
                aStringSequence()
                    .withRoot("1")
                    .withRootDimension("Sentence")
                    .withLeafDimension("Token")
                    .withLeaves("my", "Word", "one", "your", "Word", "two", "one", "Word", "my")
            )
            .commit();

        this.callAggregateDirectNeighbourCount(sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("Word")
                .withPropDirectNeighbourCount(4L)
        )));
    }

    @Test
    public void aggregateDirectNeighbourCount__multiple_occurrences__with_multiple_neighbours__in_distinct_sentences__and_distractors() {

        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("1") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("my", "Word", "one") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("2") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("your", "Word", "two") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("3") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Lemma") //
                    .withLeaves("their", "Word", "too") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("4") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("one", "Word", "here") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("5") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("one", "Word", "there") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("6") //
                    .withRootDimension("Trigram") //
                    .withLeafDimension("Lemma") //
                    .withLeaves("their", "Word", "too") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("7") //
                    .withRootDimension("Trigram") //
                    .withLeafDimension("Token") //
                    .withLeaves("their", "Word", "too") //
            ) //
            .commit();

        this.callAggregateDirectNeighbourCount(sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("Word")
                .inDimension("Token")
                .unique()
                .withPropDirectNeighbourCount(3L + 3L)
        )));
    }

    private void callAggregateDirectNeighbourCount(final Dimension parentDimension, final Dimension leafDimension) {
        final Transaction tx = db.beginTx();
        final Map<String, Object> parameters = Maps.newHashMap();
        parameters.put("parentDimension", ofNullable(parentDimension).map(Dimension::getName).orElse(null));
        parameters.put("leafDimension", ofNullable(leafDimension).map(Dimension::getName).orElse(null));
        db.execute("CALL " + AggregatingProcedures.procAggregateDirectNeighbourCounts + "({parentDimension}, {leafDimension})", parameters);
        tx.success();
    }


}