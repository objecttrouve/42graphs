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

package org.objecttrouve.fourtytwo.graphs.procedures.values;

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

import java.util.List;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.neo4j.driver.v1.Values.parameters;
import static org.objecttrouve.fourtytwo.graphs.matchers.ValueMatcher.aStringValue;
import static org.objecttrouve.fourtytwo.graphs.mocks.DimensionMock.dim;
import static org.objecttrouve.fourtytwo.graphs.mocks.StringValue.str;
import static org.objecttrouve.fourtytwo.graphs.mocks.TestStringSequenceTree.aStringSequence;

@SuppressWarnings("unchecked")
public class ValueProceduresTest {

    private static final boolean noInit = false;

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
        .withProcedure(ValueProcedures.class);

    private EmbeddedBackend graph;
    private Driver driver;

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
    }


    // --- retrieveAllValues --------------------------------------


    @Test
    public void retrieveAllValues__no_Tokens() {

        final Dimension tokens = dim().withName("Token").mock();

        final List<Value<String>> tokenNodes = values(callRetrieveAllValues(tokens));

        assertThat(tokenNodes.size(), is(0));
    }

    @Test
    public void retrieveAllValues__one_Token() {

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

        final List<Value<String>> tokenNodes = values(callRetrieveAllValues(tokens));

        assertThat(tokenNodes.size(), is(1));
        assertThat(tokenNodes, hasItem(aStringValue().withIdentifier("Word")));
    }

    @Test
    public void retrieveAllValues__multiple_Tokens_from_same_Sentence() {

        final Dimension tokens = dim().withName("Token").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Some", "words", "come", "with", "other", "words", ".") //
            ) //
            .commit();

        final List<Value<String>> tokenNodes = values(callRetrieveAllValues(tokens));

        assertThat(tokenNodes.size(), is(6));
        assertThat(tokenNodes, hasItems(//
            aStringValue().withIdentifier("Some"), //
            aStringValue().withIdentifier("words"), //
            aStringValue().withIdentifier("come"), //
            aStringValue().withIdentifier("with"), //
            aStringValue().withIdentifier("other"), //
            aStringValue().withIdentifier(".") //
            )
        );
    }


    @Test
    public void retrieveAllValues__multiple_Tokens_from_multiple_Sentences() {

        final Dimension tokens = dim().withName("Token").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Some", "words", "come", "with", "other", "words", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S2") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("And", "words", "occur", "in", "sentences", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S3") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Words", "are", "case-sensitive", ".") //
            ) //
            .commit();

        final List<Value<String>> tokenNodes = values(callRetrieveAllValues(tokens));

        assertThat(tokenNodes.size(), is(13));
        assertThat(tokenNodes, hasItems(//
            aStringValue().withIdentifier("Some"), //
            aStringValue().withIdentifier("words"), //
            aStringValue().withIdentifier("Words"), //
            aStringValue().withIdentifier("come"), //
            aStringValue().withIdentifier("with"), //
            aStringValue().withIdentifier("other"), //
            aStringValue().withIdentifier("occur"), //
            aStringValue().withIdentifier("in"), //
            aStringValue().withIdentifier("sentences"), //
            aStringValue().withIdentifier("are"), //
            aStringValue().withIdentifier("case-sensitive"), //
            aStringValue().withIdentifier("And"), //
            aStringValue().withIdentifier(".") //
            )
        );
    }

    @Test
    public void retrieveAllValues__multiple_Tokens_from_multiple_Sentences_and_distractors() {

        final Dimension tokens = dim().withName("Token").mock();
        graph.writer(noInit) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Some", "words", "come", "with", "other", "words", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("UCToken") //
                    .withLeaves("SOME", "WORDS", "COME", "IN", "UPPERCASE", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S2") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("And", "words", "occur", "in", "sentences", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("S3") //
                    .withParentDimension("Sentence") //
                    .withChildDimension("Token") //
                    .withLeaves("Words", "are", "case-sensitive", ".") //
            ) //
            .commit();

        final List<Value<String>> tokenNodes = values(callRetrieveAllValues(tokens));

        assertThat(tokenNodes.size(), is(13));
        assertThat(tokenNodes, hasItems(//
            aStringValue().withIdentifier("Some"), //
            aStringValue().withIdentifier("words"), //
            aStringValue().withIdentifier("Words"), //
            aStringValue().withIdentifier("come"), //
            aStringValue().withIdentifier("with"), //
            aStringValue().withIdentifier("other"), //
            aStringValue().withIdentifier("occur"), //
            aStringValue().withIdentifier("in"), //
            aStringValue().withIdentifier("sentences"), //
            aStringValue().withIdentifier("are"), //
            aStringValue().withIdentifier("case-sensitive"), //
            aStringValue().withIdentifier("And"), //
            aStringValue().withIdentifier(".") //
            )
        );
    }


    private StatementResult callRetrieveAllValues(final Dimension dimension) {
        return driver.session()//
            .run(//
                "CALL " + ValueProcedures.procRetrieveAllValues + "({dimension})", //
                parameters("dimension", dimension.getName()));
    }


    // --- retrieveNeighbours -------------------------------------


    @Test
    public void streamNeighbours__no_Tokens() {

        final Dimension tokens = dim().withName("Token").mock();
        final Dimension sentences = dim().withName("Sentence").mock();

        final List<Value<String>> tokenNodes = values(callRetrieveNeighbors(str("Word"), sentences, tokens, 1));

        assertThat(tokenNodes.size(), is(0));
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


        final List<Value<String>> tokenNodes = values(callRetrieveNeighbors(str("Word"), sentences, tokens, 1));

        assertThat(tokenNodes.size(), is(0));
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


        final List<Value<String>> tokenNodes = values(callRetrieveNeighbors(str("Word"), sentences, tokens, 1));

        assertThat(tokenNodes.size(), is(1));
        assertThat(tokenNodes, hasItem(aStringValue().withIdentifier("one")));
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


        final List<Value<String>> tokenNodes = values(callRetrieveNeighbors(null, sentences, tokens, 1));

        assertThat(tokenNodes.size(), is(0));
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


        final List<Value<String>> tokenNodes = values(callRetrieveNeighbors(str("Word"), null, tokens, 1));

        assertThat(tokenNodes.size(), is(0));
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


        final List<Value<String>> tokenNodes = values(callRetrieveNeighbors(str("Word"), sentences, null, 1));

        assertThat(tokenNodes.size(), is(0));
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


        final List<Value<String>> tokenNodes = values(callRetrieveNeighbors(str("one"), sentences, tokens, -1));

        assertThat(tokenNodes.size(), is(1));
        assertThat(tokenNodes, hasItem(aStringValue().withIdentifier("Word")));
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


        final List<Value<String>> tokenNodes = values(callRetrieveNeighbors(str("one"), sentences, tokens, 0));

        assertThat(tokenNodes.size(), is(0));
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


        final List<Value<String>> tokenNodes = values(callRetrieveNeighbors(str("Word"), sentences, tokens, 2));

        assertThat(tokenNodes.size(), is(1));
        assertThat(tokenNodes, hasItem(aStringValue().withIdentifier(".")));
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


        final List<Value<String>> tokenNodes = values(callRetrieveNeighbors(str("word"), sentences, tokens, -2));

        assertThat(tokenNodes.size(), is(2));
        assertThat(tokenNodes, hasItem(aStringValue().withIdentifier("with")));
        assertThat(tokenNodes, hasItem(aStringValue().withIdentifier("just")));
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


        final List<Value<String>> tokenNodes = values(callRetrieveNeighbors(str("word"), sentences, tokens, -2));

        assertThat(tokenNodes.size(), is(2));
        assertThat(tokenNodes, hasItems( //
            aStringValue().withIdentifier("with"),//
            aStringValue().withIdentifier("just")//
        ));
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


        final List<Value<String>> tokenNodes = values(callRetrieveNeighbors(str("word"), sentences, tokens, -2));

        assertThat(tokenNodes.size(), is(2));
        assertThat(tokenNodes, hasItem(aStringValue().withIdentifier("with")));
        assertThat(tokenNodes, hasItem(aStringValue().withIdentifier("just")));
    }


    private StatementResult callRetrieveNeighbors(final Value<String> self, final Dimension parentDimension, final Dimension childDimension, final long vicinity) {
        return driver.session()//
            .run(//
                "CALL " + ValueProcedures.procRetrieveNeighbours + "({self},{parentDimension},{childDimension},{vicinity})", //
                parameters(
                    "self", ofNullable(self).map(Value::getIdentifier).orElse(null),
                    "childDimension", ofNullable(childDimension).map(Dimension::getName).orElse(null),
                    "parentDimension", ofNullable(parentDimension).map(Dimension::getName).orElse(null),
                    "vicinity", vicinity
                ));
    }

    // --- shared helpers -----------------------------------------

    private List<Value<String>> values(final StatementResult result) {
        return result.list().stream().map(StringValueRecord::fromNeoRecord).collect(toList());
    }
}