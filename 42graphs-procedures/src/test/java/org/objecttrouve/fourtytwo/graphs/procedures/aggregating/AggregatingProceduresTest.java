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

package org.objecttrouve.fourtytwo.graphs.procedures.aggregating;

import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
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
    private static final Dimension documents = dim().withName("Document").mock();

    @ClassRule
    public static Neo4jRule neo4j = new Neo4jRule()
        .withProcedure(QuantityProcedures.class)
        .withProcedure(AggregatingProcedures.class);

    private static EmbeddedBackend graph;
    private static GraphDatabaseService db;

    @BeforeClass
    public static void setupStatic() {
        graph = new EmbeddedBackend(//
            () -> neo4j.getGraphDatabaseService(), () -> {
            throw new UnsupportedOperationException("Won't use it here. ");
        });
        db = neo4j.getGraphDatabaseService();
    }

    @Before
    public void setupTest(){
        final Transaction tx = db.beginTx();
        db.execute("MATCH (n) DELETE n");
        tx.success();
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
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Word") //
            ) //
            .commit();

        this.callAggregateDirectNeighbourCount(sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("Word")
                .withPropDirectNeighbourCount("Token", 0L)
        )));
    }

    @Test
    public void aggregateDirectNeighbourCount__one_token__with_a_preceding_token() {

        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("one", "Word") //
            ) //
            .commit();

        this.callAggregateDirectNeighbourCount(sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("Word")
                .withPropDirectNeighbourCount("Token",1L)
        )));
    }

    @Test
    public void aggregateDirectNeighbourCount__one_token__with_a_following_token() {

        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Word", "one") //
            ) //
            .commit();

        this.callAggregateDirectNeighbourCount(sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("Word")
                .withPropDirectNeighbourCount("Token",1L)
        )));
    }

    @Test
    public void aggregateDirectNeighbourCount__two_occurrences__in_two_sentences__with_a_following_token() {

        graph.writer(noInit) //
            .add(
                aStringSequence()
                    .withRoot("S")
                    .withParentDimension("Sentence")
                    .withChildDimension("Token")
                    .withLeaves("Word", "one")
            )
            .add(
                aStringSequence()
                    .withRoot("T")
                    .withParentDimension("Sentence")
                    .withChildDimension("Token")
                    .withLeaves("Word", "two")
            )
            .commit();

        this.callAggregateDirectNeighbourCount(sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("Word")
                .withPropDirectNeighbourCount("Token",2L)
        )));
    }

    @Test
    public void aggregateDirectNeighbourCount__two_occurrences__in_two_sentences__with_a_preceding_token() {

        graph.writer(noInit) //
            .add(
                aStringSequence()
                    .withRoot("S")
                    .withParentDimension("Sentence")
                    .withChildDimension("Token")
                    .withLeaves("one", "Word")
            )
            .add(
                aStringSequence()
                    .withRoot("T")
                    .withParentDimension("Sentence")
                    .withChildDimension("Token")
                    .withLeaves("the", "Word")
            )
            .commit();

        this.callAggregateDirectNeighbourCount(sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("Word")
                .withPropDirectNeighbourCount("Token",2L)
        )));
    }

    @Test
    public void aggregateDirectNeighbourCount__null_parentDimension_arg() {

        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Word", "one", ".") //
            ) //
            .commit();

        this.callAggregateDirectNeighbourCount(null, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("Word")
                .withPropDirectNeighbourCount("Token",0L)
        )));
    }

    @Test
    public void aggregateDirectNeighbourCount__null_childDimension_arg() {

        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Word", "one", ".") //
            ) //
            .commit();

        this.callAggregateDirectNeighbourCount(sentences, null);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("Word")
                .withPropDirectNeighbourCount("Token",0L)
        )));
    }

    @Test
    public void aggregateDirectNeighbourCount__one_token__with_a_preceding_and_a_following_token() {

        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("my", "Word", "one") //
            ) //
            .commit();

        this.callAggregateDirectNeighbourCount(sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("Word")
                .withPropDirectNeighbourCount("Token",2L)
        )));
    }



    @Test
    public void aggregateDirectNeighbourCount__multiple_occurrences__with_multiple_neighbours__in_distinct_sentences() {

        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("1") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("my", "Word", "one") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("2") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("your", "Word", "two") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("3") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("one", "Word", "here") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("4") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("one", "Word", "there") //
            ) //
            .commit();

        this.callAggregateDirectNeighbourCount(sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("Word")
                .withPropDirectNeighbourCount("Token",3L + 3L)
        )));
    }


    @Test
    public void aggregateDirectNeighbourCount__multiple_occurrences__with_multiple_neighbours__in_same_sentence() {

        graph.writer(noInit)
            .add(
                aStringSequence()
                    .withRoot("1")
                    .withParentDimension("Sentence")
                    .withChildDimension("Token")
                    .withLeaves("my", "Word", "one", "your", "Word", "two")
            )
            .commit();

        this.callAggregateDirectNeighbourCount(sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("Word")
                .withPropDirectNeighbourCount("Token",4L)
        )));
    }

    @Test
    public void aggregateDirectNeighbourCount__multiple_occurrences__with_multiple_neighbours__in_same_sentence__with_duplicates() {

        graph.writer(noInit)
            .add(
                aStringSequence()
                    .withRoot("1")
                    .withParentDimension("Sentence")
                    .withChildDimension("Token")
                    .withLeaves("my", "Word", "one", "your", "Word", "two", "my", "Word", "one")
            )
            .commit();

        this.callAggregateDirectNeighbourCount(sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("Word")
                .withPropDirectNeighbourCount("Token",4L)
        )));
    }

    @Test
    public void aggregateDirectNeighbourCount__multiple_occurrences__with_multiple_neighbours__in_same_sentence__with_inverted_duplicates() {

        graph.writer(noInit)
            .add(
                aStringSequence()
                    .withRoot("1")
                    .withParentDimension("Sentence")
                    .withChildDimension("Token")
                    .withLeaves("my", "Word", "one", "your", "Word", "two", "one", "Word", "my")
            )
            .commit();

        this.callAggregateDirectNeighbourCount(sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("Word")
                .withPropDirectNeighbourCount("Token",4L)
        )));
    }

    @Test
    public void aggregateDirectNeighbourCount__multiple_occurrences__with_multiple_neighbours__in_distinct_sentences__and_distractors() {

        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("1") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("my", "Word", "one") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("2") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("your", "Word", "two") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("3") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Lemma") //
                    .withLeaves("their", "Word", "too") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("4") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("one", "Word", "here") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("5") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("one", "Word", "there") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("6") //
                    .withParentDimension("Trigram") //
                    .withChildDimension("Lemma") //
                    .withLeaves("their", "Word", "too") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("7") //
                    .withParentDimension("Trigram") //
                    .withChildDimension("Token") //
                    .withLeaves("their", "Word", "too") //
            ) //
            .commit();

        this.callAggregateDirectNeighbourCount(sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("Word")
                .inDimension("Token")
                .unique()
                .withPropDirectNeighbourCount("Token",3L + 3L)
        )));
    }


    @Test
    public void aggregateLength__empty_DB() {

        this.callAggregateLength(sentences, tokens);

        assertThat(db, is(theEmptyGraph()));
    }


    @Test
    public void aggregateLength__on_node_without_children() {

        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Word") //
            ) //
            .commit();

        this.callAggregateLength(tokens, dim().withName("None").mock());

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("Word")
                .withPropLength("None", 0)
        )));
    }

    @Test
    public void aggregateLength__on_node_with_one_child() {

        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Word") //
            ) //
            .commit();

        this.callAggregateLength(sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("S")
                .withPropLength("Token", 1)
        )));
    }

    @Test
    public void aggregateLength__on_node_with_three_children() {

        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("One","word", ".") //
            ) //
            .commit();

        this.callAggregateLength(sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("S")
                .withPropLength("Token", 3)
        )));
    }


    @Test
    public void aggregateLength__on_node_with_three_children__in_irrelevant_dimension() {

        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("LCToken") //
                    .withLeaves("one","word", ".") //
            ) //
            .commit();

        this.callAggregateLength(sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("S")
                .withPropLength("Token", 0)
        )));
    }

    @Test
    public void aggregateLength__on_node_with_three_target_children__and_others() {

        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("One","word", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("LCToken") //
                    .withLeaves("one","word", ".") //
            ) //
            .commit();

        this.callAggregateLength(sentences, tokens);

        assertThat(db, is(aGraph()
            .ofOrder(7)
            .containingExactly(
                aNode().inDimension("Sentence").withIdentifier("S").withPropLength("Token", 3),
                aNode().inDimension("Token").withIdentifier("One"),
                aNode().inDimension("Token").withIdentifier("word"),
                aNode().inDimension("Token").withIdentifier("."),
                aNode().inDimension("LCToken").withIdentifier("one"),
                aNode().inDimension("LCToken").withIdentifier("word"),
                aNode().inDimension("LCToken").withIdentifier(".")
            )));
    }

    @Test
    public void aggregateLength__on_multiple_nodes() {

        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S1") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("LCToken") //
                    .withLeaves("one","word", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S2") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("LCToken") //
                    .withLeaves("and","another", "word", ".") //
            ) //
            .commit();

        this.callAggregateLength(sentences, dim().withName("LCToken").mock());

        assertThat(db, is(aGraph().containing(
            aNode().withIdentifier("S1").withPropLength("LCToken", 3),
            aNode().withIdentifier("S2").withPropLength("LCToken", 4)
        )));
    }

    @Test
    public void aggregateLongest__on_node_without_grandchildren() {

        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("The F... Manual") //
                    .withParentDimension("Document") //
                    .withChildDimension("Sentence") //
                    .withLeaves("S1") //
            ) //
            .commit();
        this.callAggregateLength(sentences, tokens);

        this.callAggregateLongest(documents, sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("The F... Manual")
                .withPropLongest("Sentence", "Token", 0)
        )));
    }

    @Test
    public void aggregateLongest__on_node_with_one_grandchild() {

        graph.writer(noInit)
            .add(
                aStringSequence()
                    .withRoot("The F... Manual")
                    .withParentDimension("Document")
                    .withChildDimension("Sentence")
                    .withLeaves("S1")
            )
            .add(
                aStringSequence()
                    .withRoot("S1")
                    .withParentDimension("Sentence")
                    .withChildDimension("Token")
                    .withLeaves("Read")
            )
            .commit();
        this.callAggregateLength(sentences, tokens);

        this.callAggregateLongest(documents, sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("The F... Manual")
                .withPropLongest("Sentence", "Token", 1)
        )));
    }


    @Test
    public void aggregateLongest__on_node_with_a_child_with_multiple_grandchildren() {

        graph.writer(noInit)
            .add(
                aStringSequence()
                    .withRoot("The F... Manual")
                    .withParentDimension("Document")
                    .withChildDimension("Sentence")
                    .withLeaves("S1")
            )
            .add(
                aStringSequence()
                    .withRoot("S1")
                    .withParentDimension("Sentence")
                    .withChildDimension("Token")
                    .withLeaves("Read", "the", "f...", "manual", "!")
            )
            .commit();
        this.callAggregateLength(sentences, tokens);

        this.callAggregateLongest(documents, sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("The F... Manual")
                .withPropLongest("Sentence", "Token", 5)
        )));
    }

    @Test
    public void aggregateLongest__on_node_with_multiple_children_with_multiple_grandchildren_each() {

        graph.writer(noInit)
            .add(
                aStringSequence()
                    .withRoot("The F... Manual")
                    .withParentDimension("Document")
                    .withChildDimension("Sentence")
                    .withLeaves("S1", "S2", "S3", "S4")
            )
            .add(
                aStringSequence()
                    .withRoot("S1")
                    .withParentDimension("Sentence")
                    .withChildDimension("Token")
                    .withLeaves("Read", "the", "f...", "manual", "!")
            )
            .add(
                aStringSequence()
                    .withRoot("S2")
                    .withParentDimension("Sentence")
                    .withChildDimension("Token")
                    .withLeaves("Don't", "make", "me", "think", "!")
            )
            .add(
                aStringSequence()
                    .withRoot("S3")
                    .withParentDimension("Sentence")
                    .withChildDimension("Token")
                    .withLeaves("User", "error", "...")
            )

            .commit();
        this.callAggregateLength(sentences, tokens);

        this.callAggregateLongest(documents, sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("The F... Manual")
                .withPropLongest("Sentence", "Token", 5)
        )));
    }


    @Test
    public void aggregateLongest__on_node_with_multiple_children_with_multiple_grandchildren_with_distractors__01() {

        graph.writer(noInit)
            .add(
                aStringSequence()
                    .withRoot("The F... Manual")
                    .withParentDimension("Document")
                    .withChildDimension("Sentence")
                    .withLeaves("S1", "S2", "S3", "S4")
            )
            .add(
                aStringSequence()
                    .withRoot("S1")
                    .withParentDimension("Sentence")
                    .withChildDimension("Saying")
                    .withLeaves("Read", "the", "f...", "manual", "!")
            )
            .add(
                aStringSequence()
                    .withRoot("S2")
                    .withParentDimension("Sentence")
                    .withChildDimension("Saying")
                    .withLeaves("Don't", "make", "me", "think", "!")
            )
            .add(
                aStringSequence()
                    .withRoot("S3")
                    .withParentDimension("Sentence")
                    .withChildDimension("Token")
                    .withLeaves("User", "error", "...")
            )
            .add(
                aStringSequence()
                    .withRoot("S4")
                    .withParentDimension("Sentence")
                    .withChildDimension("Token")
                    .withLeaves("500")
            )
            .commit();
        this.callAggregateLength(sentences, tokens);

        this.callAggregateLongest(documents, sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("The F... Manual")
                .withPropLongest("Sentence", "Token", 3)
        )));
    }

    private void callAggregateDirectNeighbourCount(final Dimension parentDimension, final Dimension childDimension) {
        final Transaction tx = db.beginTx();
        final Map<String, Object> parameters = Maps.newHashMap();
        parameters.put("parentDimension", ofNullable(parentDimension).map(Dimension::getName).orElse(null));
        parameters.put("childDimension", ofNullable(childDimension).map(Dimension::getName).orElse(null));
        db.execute("CALL " + AggregatingProcedures.procAggregateDirectNeighbourCounts + "({parentDimension}, {childDimension})", parameters);
        tx.success();
    }

    private void callAggregateLength(final Dimension parentDimension, final Dimension childDimension) {
        final Transaction tx = db.beginTx();
        final Map<String, Object> parameters = Maps.newHashMap();
        parameters.put("parentDimension", ofNullable(parentDimension).map(Dimension::getName).orElse(null));
        parameters.put("childDimension", ofNullable(childDimension).map(Dimension::getName).orElse(null));
        db.execute("CALL " + AggregatingProcedures.procAggregateLength + "({parentDimension}, {childDimension})", parameters);
        tx.success();
    }

    @SuppressWarnings("SameParameterValue")
    private void callAggregateLongest(final Dimension grandParentDimension, final Dimension parentDimension, final Dimension childDimension) {
        final Transaction tx = db.beginTx();
        final Map<String, Object> parameters = Maps.newHashMap();
        parameters.put("grandParentDimension", ofNullable(grandParentDimension).map(Dimension::getName).orElse(null));
        parameters.put("parentDimension", ofNullable(parentDimension).map(Dimension::getName).orElse(null));
        parameters.put("childDimension", ofNullable(childDimension).map(Dimension::getName).orElse(null));
        db.execute("CALL " + AggregatingProcedures.procAggregateLongest + "({grandParentDimension}, {parentDimension}, {childDimension})", parameters);
        tx.success();
    }


}