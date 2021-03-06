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

package org.objecttrouve.fourtytwo.graphs.procedures.quantities;


import org.hamcrest.MatcherAssert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.harness.junit.Neo4jRule;
import org.objecttrouve.fourtytwo.graphs.api.Dimension;
import org.objecttrouve.fourtytwo.graphs.api.Value;
import org.objecttrouve.fourtytwo.graphs.backend.init.EmbeddedBackend;

import static java.util.Optional.ofNullable;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.driver.v1.Values.parameters;
import static org.objecttrouve.fourtytwo.graphs.mocks.DimensionMock.dim;
import static org.objecttrouve.fourtytwo.graphs.mocks.StringValue.str;
import static org.objecttrouve.fourtytwo.graphs.mocks.TestStringSequenceTree.aStringSequence;
import static org.objecttrouve.fourtytwo.graphs.procedures.quantities.QuantityProcedures.*;


public class QuantityProceduresTest {

    private static final boolean noInit = false;

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
        .withProcedure(QuantityProcedures.class);

    private EmbeddedBackend graph;
    private Driver driver;

    @Before
    public void setup() {
        graph = new EmbeddedBackend(//
            () -> neo4j.getGraphDatabaseService(), () -> {
            throw new UnsupportedOperationException("Won't use it here. ");
        });
        driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig());
    }


    // --- countAllValues ------------------------------

    @Test
    public void countAllValues__empty_graph() {

        final StatementResult result = callCountAllValues("SomeDim");

        assertThat(quantity(result), is(0L));
    }

    @Test
    public void countAllValues__one_relevant_node() {

        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Word") //
            ) //
            .commit();

        final StatementResult result = callCountAllValues("Token");

        assertThat(quantity(result), is(1L));
    }

    @Test
    public void countAllValues__three_relevant_nodes() {

        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("One", "two", "three") //
            ) //
            .commit();

        final StatementResult result = callCountAllValues("Token");

        assertThat(quantity(result), is(3L));
    }

    @Test
    public void countAllValues__three_relevant_nodes__and_three_distractors() {

        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("One", "two", "three") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Num") //
                    .withLeaves("1", "2", "3") //
            ) //
            .commit();

        final StatementResult result = callCountAllValues("Token");

        assertThat(quantity(result), is(3L));
    }

    @Test
    public void countAllValues__no_relevant_nodes() {

        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Num") //
                    .withLeaves("1", "2", "3") //
            ) //
            .commit();

        final StatementResult result = callCountAllValues("Token");

        assertThat(quantity(result), is(0L));
    }


    private StatementResult callCountAllValues(final String dimension) {
        return driver.session()//
            .run(//
                "CALL " + procCountAllValues + "({dimension})", //
                parameters("dimension", dimension));
    }


    // --- countAllOccurrences -----------------------------

    @Test
    public void countAllOccurrences__empty_DB() {
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();

        final StatementResult result = callCountAllOccurrences(sentences, tokens);

        assertThat(quantity(result), is(0L));
    }

    @Test
    public void countAllOccurrences__one_occurrence() {

        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Word") //
            ) //
            .commit();


        final StatementResult result = callCountAllOccurrences(sentences, tokens);

        assertThat(quantity(result), is(1L));
    }


    @Test
    public void countAllOccurrences__three_occurrences_in_same_sentence() {
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Three", "words", ".") //
            ) //
            .commit();

        final StatementResult result = callCountAllOccurrences(sentences, tokens);

        assertThat(quantity(result), is(3L));
    }


    @Test
    public void countAllOccurrences__multiple_sentences() {
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Three", "words", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("E") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Second", "sentence", "with", "words", ".") //
            ) //
            .commit();

        final StatementResult result = callCountAllOccurrences(sentences, tokens);

        assertThat(quantity(result), is(8L));
    }

    @Test
    public void countAllOccurrences__multiple_sentences_and_multiple_dimensions() {

        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Three", "words", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("E") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Second", "sentence", "with", "words", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("LCToken") //
                    .withLeaves("three", "words", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("E") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("LCToken") //
                    .withLeaves("second", "sentence", "with", "words", ".") //
            ) //
            .commit();

        final StatementResult result = callCountAllOccurrences(sentences, tokens);

        assertThat(quantity(result), is(8L));
    }

    @Test
    public void countAllOccurrences__multiple_sentences_and_multiple_dimensions_2() {
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Three", "words", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("s") //
                    .withParentDimension("LCSentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Second", "sentence", "with", "words", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("s") //
                    .withParentDimension("LCSentence") //
                    .withChildDimension("LCToken") //
                    .withLeaves("three", "words", ".") //
            ) //
            .commit();

        final StatementResult result = callCountAllOccurrences(sentences, tokens);

        assertThat(quantity(result), is(3L));
    }

    private StatementResult callCountAllOccurrences(final Dimension parentDimension, final Dimension childDimension) {
        return driver.session()//
            .run(//
                "CALL " + procCountAllOccurrences + "({parentDimension}, {childDimension})", //
                parameters(
                    "parentDimension", parentDimension.getName(),
                    "childDimension", childDimension.getName()
                ));
    }

    // --- countOccurrences ---------------------------------------

    @Test
    public void countOccurrences__empty_DB() {
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        
        final long count = quantity(callCountAllOccurrences(str("word"), sentences, tokens));

        MatcherAssert.assertThat(count, is(0L));
    }

    @Test
    public void countOccurrences__one_occurrence() {
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Word") //
            ) //
            .commit();


        final long count = quantity(callCountAllOccurrences(str("Word"), sentences, tokens));

        MatcherAssert.assertThat(count, is(1L));
    }

    @Test
    public void countOccurrences__one_of_three_in_same_sentence() {
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Three", "words", ".") //
            ) //
            .commit();


        final long count = quantity(callCountAllOccurrences(str("words"), sentences, tokens));

        MatcherAssert.assertThat(count, is(1L));
    }


    @Test
    public void countOccurrences__three_of_three_in_same_sentence() {
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("words", "words", "words") //
            ) //
            .commit();


        final long count = quantity(callCountAllOccurrences(str("words"), sentences, tokens));

        MatcherAssert.assertThat(count, is(3L));
    }

    @Test
    public void countOccurrences__two_of_three_in_same_sentence_case_sensitive() {
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Words", "words", "words") //
            ) //
            .commit();


        final long count = quantity(callCountAllOccurrences(str("words"), sentences, tokens));

        MatcherAssert.assertThat(count, is(2L));
    }

    @Test
    public void countOccurrences__multiple_sentences() {
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Three", "words", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("E") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Second", "sentence", "with", "words", ".") //
            ) //
            .commit();


        final long count = quantity(callCountAllOccurrences(str("words"), sentences, tokens));

        MatcherAssert.assertThat(count, is(2L));
    }

    @Test
    public void countOccurrences__null_parent_Dimension() {
        final Dimension tokens = dim().withName("Token").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Word") //
            ) //
            .commit();


        final long count = quantity(callCountAllOccurrences(str("Word"), null, tokens));

        MatcherAssert.assertThat(count, is(0L));
    }

    @Test
    public void countOccurrences__null_leaf_Dimension() {
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Word") //
            ) //
            .commit();


        final long count = quantity(callCountAllOccurrences(str("Word"), sentences, null));

        MatcherAssert.assertThat(count, is(0L));
    }

    @Test
    public void countOccurrences__null_null_Dimension() {
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Word") //
            ) //
            .commit();

        final long count = quantity(callCountAllOccurrences(str("Words"), null, null));

        MatcherAssert.assertThat(count, is(0L));
    }

    @Test
    public void countOccurrences__null_Value() {
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Word") //
            ) //
            .commit();


        final long count = quantity(callCountAllOccurrences(null, sentences, tokens));

        MatcherAssert.assertThat(count, is(0L));
    }

    @Test
    public void countOccurrences__multiple_sentences_and_multiple_dimensions() {
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Three", "words", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("E") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Second", "sentence", "with", "words", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("LCToken") //
                    .withLeaves("three", "words", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("E") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("LCToken") //
                    .withLeaves("second", "sentence", "with", "words", ".") //
            ) //
            .commit();


        final long count = quantity(callCountAllOccurrences(str("words"), sentences, tokens));

        MatcherAssert.assertThat(count, is(2L));
    }

    @Test
    public void countOccurrences__multiple_sentences_and_multiple_dimensions_2() {
        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Three", "words", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("s") //
                    .withParentDimension("LCSentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Second", "sentence", "with", "words", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("s") //
                    .withParentDimension("LCSentence") //
                    .withChildDimension("LCToken") //
                    .withLeaves("three", "words", ".") //
            ) //
            .commit();


        final long count = quantity(callCountAllOccurrences(str("words"), sentences, tokens));

        MatcherAssert.assertThat(count, is(1L));
    }


    private StatementResult callCountAllOccurrences(final Value<String> value, final Dimension parentDimension, final Dimension childDimension) {
        return driver.session()//
            .run(//
                "CALL " + procCountOccurrences + "({value},{parentDimension}, {childDimension})", //
                parameters(
                    "value", ofNullable(value).map(Value::getIdentifier).orElse(null),
                    "parentDimension", ofNullable(parentDimension).map(Dimension::getName).orElse(null),
                    "childDimension", ofNullable(childDimension).map(Dimension::getName).orElse(null)
                ));
    }

    // --- countNeighbours -------------------------------------


    @Test
    public void streamNeighbours__no_Tokens() {

        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();

        final long neighbourCount = quantity(callCountNeighbours(str("Word"), sentences, tokens, 1));

        MatcherAssert.assertThat(neighbourCount, is(0L));
    }

    @Test
    public void streamNeighbours__no_neighbours() {

        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Word") //
            ) //
            .commit();


        final long neighbourCount = quantity(callCountNeighbours(str("Word"), sentences, tokens, 1));

        MatcherAssert.assertThat(neighbourCount, is(0L));
    }

    @Test
    public void streamNeighbours__one_following_Token() {

        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Word", "one", ".") //
            ) //
            .commit();


        final long neighbourCount = quantity(callCountNeighbours(str("Word"), sentences, tokens, 1));

        MatcherAssert.assertThat(neighbourCount, is(1L));
    }


    @Test
    public void streamNeighbours__null_self_arg() {

        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Word", "one", ".") //
            ) //
            .commit();


        final long neighbourCount = quantity(callCountNeighbours(null, sentences, tokens, 1));

        MatcherAssert.assertThat(neighbourCount, is(0L));
    }


    @Test
    public void streamNeighbours__null_parentDimension_arg() {

        final Dimension tokens = dim().withName("Token").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Word", "one", ".") //
            ) //
            .commit();


        final long neighbourCount = quantity(callCountNeighbours(str("Word"), null, tokens, 1));

        MatcherAssert.assertThat(neighbourCount, is(0L));
    }

    @Test
    public void streamNeighbours__null_childDimension_arg() {

        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Word", "one", ".") //
            ) //
            .commit();


        final long neighbourCount = quantity(callCountNeighbours(str("Word"), sentences, null, 1));

        MatcherAssert.assertThat(neighbourCount, is(0L));
    }

    @Test
    public void streamNeighbours__one_preceding_Token() {

        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Word", "one", ".") //
            ) //
            .commit();


        final long neighbourCount = quantity(callCountNeighbours(str("one"), sentences, tokens, -1));

        MatcherAssert.assertThat(neighbourCount, is(1L));
    }

    @Test
    public void streamNeighbours__self_is_the_closest_neighbour_but_apparently_it_doesnt_match() {

        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Word", "one", ".") //
            ) //
            .commit();


        final long neighbourCount = quantity(callCountNeighbours(str("one"), sentences, tokens, 0));

        MatcherAssert.assertThat(neighbourCount, is(0L));
    }

    @Test
    public void streamNeighbours__one_neighbour_away() {

        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Word", "one", ".") //
            ) //
            .commit();


        final long neighbourCount = quantity(callCountNeighbours(str("Word"), sentences, tokens, 2));

        MatcherAssert.assertThat(neighbourCount, is(1L));
    }


    @Test
    public void streamNeighbours__across_multiple_sentences() {

        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Sentence", "with", "one", "word", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S2") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Not", "really", "just", "one", "word", ".") //
            ) //
            .commit();


        final long neighbourCount = quantity(callCountNeighbours(str("word"), sentences, tokens, -2));

        MatcherAssert.assertThat(neighbourCount, is(2L));
    }

    @Test
    public void streamNeighbours__across_multiple_sentences_same_neighbour_occurs_twice() {

        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Sentence", "with", "one", "word", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S2") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Not", "really", "just", "one", "word", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S3") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("But", "really", "just", "a", "word", ".") //
            ) //
            .commit();


        final long neighbourCount = quantity(callCountNeighbours(str("word"), sentences, tokens, -2));

        MatcherAssert.assertThat(neighbourCount, is(2L));
    }


    @Test
    public void streamNeighbours__across_multiple_sentences_with_distractors() {

        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Sentence", "with", "one", "word", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("LCSentence") //
                    .withChildDimension("Token") //
                    .withLeaves("sentence", "with", "one", "word", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S2") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Not", "really", "just", "one", "word", ".") //
            ) //
            .commit();


        final long neighbourCount = quantity(callCountNeighbours(str("word"), sentences, tokens, -2));

        MatcherAssert.assertThat(neighbourCount, is(2L));
    }

    private StatementResult callCountNeighbours(final Value<String> self, final Dimension parentDimension, final Dimension childDimension, final long vicinity) {
        return driver.session()//
            .run(//
                "CALL " + procCountNeighbours + "({self},{parentDimension}, {childDimension}, {vicinity})", //
                parameters(
                    "self", ofNullable(self).map(Value::getIdentifier).orElse(null),
                    "parentDimension", ofNullable(parentDimension).map(Dimension::getName).orElse(null),
                    "childDimension", ofNullable(childDimension).map(Dimension::getName).orElse(null),
                    "vicinity", vicinity
                ));
    }

    // --- shared helpers -----------------------------------------

    private Long quantity(final StatementResult result) {
        return ofNullable(result)//
            .filter(StatementResult::hasNext)//
            .map(StatementResult::next)//
            .map(LongQuantityRecord::fromNeoRecord)//
            .map(LongQuantityRecord::getQuantity)//
            .orElse(0L);
    }
}