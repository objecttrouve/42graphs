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

import java.util.Collections;
import java.util.Map;

import static java.util.Arrays.asList;
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
    private static final Dimension corpus = dim().withName("Corpus").mock();

    @ClassRule
    public static Neo4jRule neo4j = new Neo4jRule()
        .withProcedure(QuantityProcedures.class)
        .withProcedure(AggregatingProcedures.class);

    private static EmbeddedBackend graph;
    private static GraphDatabaseService db;

    @BeforeClass
    public static void setupStatic() {
        db = neo4j.getGraphDatabaseService();
        graph = new EmbeddedBackend(//
            () -> db, () -> {
            throw new UnsupportedOperationException("Won't use it here. ");
        });
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


    @Test
    public void aggregateLongest__on_node_with_multiple_children_with_multiple_grandchildren_with_distractors__02() {

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
        this.callAggregateLength(dim().withName("Saying").mock(), tokens);

        this.callAggregateLongest(documents, sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("The F... Manual")
                .withPropLongest("Sentence", "Token", 3)
        )));
    }

    @Test
    public void aggregateLongest__on_node_with_multiple_children_with_multiple_grandchildren_with_distractors__03() {

        graph.writer(noInit)
            .add(
                aStringSequence()
                    .withRoot("The F... Manual")
                    .withParentDimension("Document")
                    .withChildDimension("Sentence")
                    .withLeaves("S1", "S2", "S4")
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
            .add(
                aStringSequence()
                    .withRoot("S5")
                    .withParentDimension("Sentence")
                    .withChildDimension("Token")
                    .withLeaves("\uD83C\uDFC6", "\uD83C\uDFC5")
            )
            .add(
                aStringSequence()
                    .withRoot("Quickstart")
                    .withParentDimension("Document")
                    .withChildDimension("Sentence")
                    .withLeaves("S3", "S5")
            )
            .commit();
        this.callAggregateLength(sentences, tokens);
        this.callAggregateLength(dim().withName("Saying").mock(), tokens);

        this.callAggregateLongest(documents, sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("The F... Manual")
                .withPropLongest("Sentence", "Token", 1),
            aNode()
                .withIdentifier("Quickstart")
                .withPropLongest("Sentence", "Token", 3)
        )));
    }


    @Test
    public void aggregateMaxLongest__only_one_longest_to_select() {

        graph.writer(noInit)
            .add(
                aStringSequence()
                    .withRoot("Tales")
                    .withParentDimension("Corpus")
                    .withChildDimension("Document")
                    .withLeaves("Fairy")
            )
            .add(
                aStringSequence()
                    .withRoot("Fairy") //
                    .withParentDimension("Document")
                    .withChildDimension("Sentence")
                    .withLeaves("S1")
            )
            .add(
                aStringSequence()
                    .withRoot("S1")
                    .withParentDimension("Sentence")
                    .withChildDimension("Token")
                    .withLeaves("Once")
            )

            .commit();
        this.callAggregateLength(sentences, tokens);
        this.callAggregateLongest(documents, sentences, tokens);

        this.callAggregateMaxLongest(corpus, documents, sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("Tales")
                .withPropLongest("Sentence", "Token", 1)
        )));
    }


    @Test
    public void aggregateMaxLongest__multiple_longest_to_aggregate() {

        graph.writer(noInit)
            .add(
                aStringSequence()
                    .withRoot("Tales")
                    .withParentDimension("Corpus")
                    .withChildDimension("Document")
                    .withLeaves("Fairy", "Fairy Tale")
            )
            .add(
                aStringSequence()
                    .withRoot("Fairy") //
                    .withParentDimension("Document")
                    .withChildDimension("Sentence")
                    .withLeaves("S1")
            )
            .add(
                aStringSequence()
                    .withRoot("S1")
                    .withParentDimension("Sentence")
                    .withChildDimension("Token")
                    .withLeaves("Once")
            )
            .add(
                aStringSequence()
                    .withRoot("Fairy Tale") //
                    .withParentDimension("Document")
                    .withChildDimension("Sentence")
                    .withLeaves("S2")
            )
            .add(
                aStringSequence()
                    .withRoot("S2")
                    .withParentDimension("Sentence")
                    .withChildDimension("Token")
                    .withLeaves("Once", "upon", "a", "time", "...")
            )
            .commit();
        this.callAggregateLength(sentences, tokens);
        this.callAggregateLongest(documents, sentences, tokens);

        this.callAggregateMaxLongest(corpus, documents, sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("Tales")
                .withPropLongest("Sentence", "Token", 5)
        )));
    }


    @Test
    public void aggregateMaxLongest__multiple_longest_to_aggregate__with_distractor() {

        graph.writer(noInit)
            .add(
                aStringSequence()
                    .withRoot("Tales")
                    .withParentDimension("Corpus")
                    .withChildDimension("Document")
                    .withLeaves("Fairy", "Fairy Tale", "Fair Trade")
            )
            .add(
                aStringSequence()
                    .withRoot("Fairy") //
                    .withParentDimension("Document")
                    .withChildDimension("Sentence")
                    .withLeaves("S1")
            )
            .add(
                aStringSequence()
                    .withRoot("S1")
                    .withParentDimension("Sentence")
                    .withChildDimension("Token")
                    .withLeaves("Once")
            )
            .add(
                aStringSequence()
                    .withRoot("Fairy Tale") //
                    .withParentDimension("Document")
                    .withChildDimension("Sentence")
                    .withLeaves("S2")
            )
            .add(
                aStringSequence()
                    .withRoot("S2")
                    .withParentDimension("Sentence")
                    .withChildDimension("Token")
                    .withLeaves("Once", "upon", "a", "time", "...")
            )
            .add(
                aStringSequence()
                    .withRoot("Fair Trade") //
                    .withParentDimension("Illusion")
                    .withChildDimension("Sentence")
                    .withLeaves("S3")
            )
            .add(
                aStringSequence()
                    .withRoot("S3")
                    .withParentDimension("Sentence")
                    .withChildDimension("Token")
                    .withLeaves("After", "the", "apocalypse", "...")
            )
            .commit();

        this.callAggregateLength(sentences, tokens);
        this.callAggregateLongest(documents, sentences, tokens);

        this.callAggregateMaxLongest(corpus, documents, sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("Tales")
                .withPropLongest("Sentence", "Token", 5)
        )));
    }

    @Test
    public void aggregatePositionCounts__on_node_without_children() {

        graph.writer(noInit)
            .add(
                aStringSequence()
                    .withRoot("Doc")
                    .withParentDimension("Document")
                    .withChildDimension("Sentence")
                    .withLeaves()
            )
            .commit();

        this.callAggregatePositionCounts(documents, sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode().withIdentifier("Doc").withPropAggrPositionCount("Sentence", "Token", Collections.emptyList())
        )));
    }

    @Test
    public void aggregatePositionCounts__on_node_with_a_child_with_a_length() {

        graph.writer(noInit)
            .add(
                aStringSequence()
                    .withRoot("Doc")
                    .withParentDimension("Document")
                    .withChildDimension("Sentence")
                    .withLeaves("S1")
            )
            .add(
                aStringSequence()
                    .withRoot("S1")
                    .withParentDimension("Sentence")
                    .withChildDimension("Token")
                    .withLeaves("Machu", "Picchu")
            )
            .commit();
        this.callAggregateLength(sentences, tokens);

        this.callAggregatePositionCounts(documents, sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("Doc")
                .withPropAggrPositionCount("Sentence", "Token", asList(1,1))
        )));
    }


    @Test
    public void aggregatePositionCounts__on_multiple_nodes_with_length() {

        graph.writer(noInit)
            .add(
                aStringSequence()
                    .withRoot("Doc")
                    .withParentDimension("Document")
                    .withChildDimension("Sentence")
                    .withLeaves("S1", "S2", "S3")
            )
            .add(
                aStringSequence()
                    .withRoot("S1")
                    .withParentDimension("Sentence")
                    .withChildDimension("Token")
                    .withLeaves("Machu", "Picchu")
            )
            .add(
                aStringSequence()
                    .withRoot("S2")
                    .withParentDimension("Sentence")
                    .withChildDimension("Token")
                    .withLeaves("Tiwanacu")
            )
            .add(
                aStringSequence()
                    .withRoot("S3")
                    .withParentDimension("Sentence")
                    .withChildDimension("Token")
                    .withLeaves("San", "Pedro", "de", "Atacama")
            )
            .commit();
        this.callAggregateLength(sentences, tokens);

        this.callAggregatePositionCounts(documents, sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("Doc")
                .withPropAggrPositionCount("Sentence", "Token", asList(3,2,1,1))
        )));
    }



    @Test
    public void aggregatePositionCounts__on_multiple_top_nodes() {

        graph.writer(noInit)
            .add(
                aStringSequence()
                    .withRoot("Doc")
                    .withParentDimension("Document")
                    .withChildDimension("Sentence")
                    .withLeaves("S1", "S2", "S3")
            )
            .add(
                aStringSequence()
                    .withRoot("S1")
                    .withParentDimension("Sentence")
                    .withChildDimension("Token")
                    .withLeaves("Machu", "Picchu")
            )
            .add(
                aStringSequence()
                    .withRoot("S2")
                    .withParentDimension("Sentence")
                    .withChildDimension("Token")
                    .withLeaves("Tiwanacu")
            )
            .add(
                aStringSequence()
                    .withRoot("S3")
                    .withParentDimension("Sentence")
                    .withChildDimension("Token")
                    .withLeaves("San", "Pedro", "de", "Atacama")
            )
            .add(
                aStringSequence()
                    .withRoot("Doc 2")
                    .withParentDimension("Document")
                    .withChildDimension("Sentence")
                    .withLeaves("S4", "S5")
            )
            .add(
                aStringSequence()
                    .withRoot("S4")
                    .withParentDimension("Sentence")
                    .withChildDimension("Token")
                    .withLeaves("Pollo", "con", "arroz")
            )
            .add(
                aStringSequence()
                    .withRoot("S5")
                    .withParentDimension("Sentence")
                    .withChildDimension("Token")
                    .withLeaves("Pollo", "con", "arroz", "y", "una", "palta")
            )
            .commit();
        this.callAggregateLength(sentences, tokens);

        this.callAggregatePositionCounts(documents, sentences, tokens);

        assertThat(db, is(aGraph().containing(
            aNode()
                .withIdentifier("Doc")
                .withPropAggrPositionCount("Sentence", "Token", asList(3,2,1,1)),
            aNode()
                .withIdentifier("Doc 2")
                .withPropAggrPositionCount("Sentence", "Token", asList(2,2,2,1,1,1))
        )));
    }

    private void callAggregatePositionCounts(final Dimension targetDimension, final Dimension parentDimension, final Dimension childDimension) {
        final Transaction tx = db.beginTx();
        final Map<String, Object> parameters = Maps.newHashMap();
        parameters.put("parentDimension", str(parentDimension));
        parameters.put("childDimension", str(childDimension));
        parameters.put("targetDimension", str(targetDimension));
        db.execute("CALL " + AggregatingProcedures.procAggregatePositionCounts + "({parentDimension}, {childDimension}, {targetDimension})", parameters);
        tx.success();
    }
    private void callAggregateDirectNeighbourCount(final Dimension parentDimension, final Dimension childDimension) {
        callAggrParentChild(AggregatingProcedures.procAggregateDirectNeighbourCounts, parentDimension, childDimension);
    }

    private void callAggregateLength(final Dimension parentDimension, final Dimension childDimension) {
        callAggrParentChild(AggregatingProcedures.procAggregateLength, parentDimension, childDimension);
    }

    @SuppressWarnings("SameParameterValue")
    private void callAggregateLongest(final Dimension grandParentDimension, final Dimension parentDimension, final Dimension childDimension) {
        final Transaction tx = db.beginTx();
        final Map<String, Object> parameters = Maps.newHashMap();
        parameters.put("grandParentDimension", str(grandParentDimension));
        parameters.put("parentDimension", str(parentDimension));
        parameters.put("childDimension", str(childDimension));
        db.execute("CALL " + AggregatingProcedures.procAggregateLongest + "({grandParentDimension}, {parentDimension}, {childDimension})", parameters);
        tx.success();
    }

    @SuppressWarnings("SameParameterValue")
    private void callAggregateMaxLongest(
        final Dimension parentDimension,
        final Dimension childDimension,
        final Dimension propagatedParentDimension,
        final Dimension propagatedChildDimension
    ) {
        final Transaction tx = db.beginTx();
        final Map<String, Object> parameters = Maps.newHashMap();
        parameters.put("parentDimension", str(parentDimension));
        parameters.put("childDimension", str(childDimension));

        parameters.put("propagatedParentDimension", str(propagatedParentDimension));
        parameters.put("propagatedChildDimension", str(propagatedChildDimension));


        db.execute("CALL " + AggregatingProcedures.procAggregateMaxLongest + "({parentDimension}, {childDimension}, {propagatedParentDimension}, {propagatedChildDimension})", parameters);
        tx.success();
    }

    private void callAggrParentChild(final String proc, final Dimension parentDimension, final Dimension childDimension) {
        final Transaction tx = db.beginTx();
        final Map<String, Object> parameters = Maps.newHashMap();
        parameters.put("parentDimension", str(parentDimension));
        parameters.put("childDimension", str(childDimension));
        db.execute("CALL " + proc + "({parentDimension}, {childDimension})", parameters);
        tx.success();
    }

    private String str(final Dimension propagatedParentDimension) {
        return ofNullable(propagatedParentDimension).map(Dimension::getName).orElse(null);
    }


}