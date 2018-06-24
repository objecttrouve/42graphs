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

package org.objecttrouve.fourtytwo.graphs.procedures.quantities;


import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.harness.junit.Neo4jRule;
import org.objecttrouve.fourtytwo.graphs.api.Dimension;
import org.objecttrouve.fourtytwo.graphs.backend.init.EmbeddedBackend;

import static java.util.Optional.ofNullable;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.driver.v1.Values.parameters;
import static org.objecttrouve.fourtytwo.graphs.mocks.DimensionMock.dim;
import static org.objecttrouve.fourtytwo.graphs.mocks.TestStringSequenceTree.aStringSequence;
import static org.objecttrouve.fourtytwo.graphs.procedures.quantities.QuantityProcedures.procCountAllOccurrences;
import static org.objecttrouve.fourtytwo.graphs.procedures.quantities.QuantityProcedures.procCountAllValues;


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
    public void countAllValues__empty_graph() throws Throwable {

        final StatementResult result = callCountAllValues("SomeDim");

        assertThat(quantity(result), is(0L));
    }

    @Test
    public void countAllValues__one_relevant_node() throws Throwable {

        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("Word") //
            ) //
            .commit();

        final StatementResult result = callCountAllValues("Token");

        assertThat(quantity(result), is(1L));
    }

    @Test
    public void countAllValues__three_relevant_nodes() throws Throwable {

        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("One", "two", "three") //
            ) //
            .commit();

        final StatementResult result = callCountAllValues("Token");

        assertThat(quantity(result), is(3L));
    }

    @Test
    public void countAllValues__three_relevant_nodes__and_three_distractors() throws Throwable {

        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
                    .withLeaves("One", "two", "three") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Num") //
                    .withLeaves("1", "2", "3") //
            ) //
            .commit();

        final StatementResult result = callCountAllValues("Token");

        assertThat(quantity(result), is(3L));
    }

    @Test
    public void countAllValues__no_relevant_nodes() throws Throwable {

        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Num") //
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
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
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
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
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

        final StatementResult result = callCountAllOccurrences(sentences, tokens);

        assertThat(quantity(result), is(3L));
    }

    private StatementResult callCountAllOccurrences(final Dimension parentDimension, final Dimension leafDimension) {
        return driver.session()//
            .run(//
                "CALL " + procCountAllOccurrences + "({parentDimension}, {leafDimension})", //
                parameters(
                    "parentDimension", parentDimension.getName(),
                    "leafDimension", leafDimension.getName()
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