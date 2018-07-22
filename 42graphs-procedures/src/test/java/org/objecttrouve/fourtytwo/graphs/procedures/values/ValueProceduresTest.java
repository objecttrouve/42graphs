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

import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.neo4j.driver.v1.Values.parameters;
import static org.objecttrouve.fourtytwo.graphs.matchers.ValueMatcher.aStringValue;
import static org.objecttrouve.fourtytwo.graphs.mocks.DimensionMock.dim;
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
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
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
                    .withRootDimension("Sentence") //
                    .withLeafDimension("Token") //
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

    // --- shared helpers -----------------------------------------

    private List<Value<String>> values(final StatementResult result) {
        return result.list().stream().map(StringValueRecord::fromNeoRecord).collect(toList());
    }
}