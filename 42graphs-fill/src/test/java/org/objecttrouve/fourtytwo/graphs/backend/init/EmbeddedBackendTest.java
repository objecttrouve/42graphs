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

package org.objecttrouve.fourtytwo.graphs.backend.init;


import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.objecttrouve.fourtytwo.graphs.api.Dimension;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.objecttrouve.fourtytwo.graphs.matchers.NeoDbMatcher.aGraph;
import static org.objecttrouve.fourtytwo.graphs.matchers.NeoDbMatcher.theEmptyGraph;
import static org.objecttrouve.fourtytwo.graphs.matchers.NeoNodeMatcher.aNode;
import static org.objecttrouve.fourtytwo.graphs.matchers.NeoRelationMatcher.aRelation;
import static org.objecttrouve.fourtytwo.graphs.mocks.TestStringSequenceTree.aStringSequence;

@RunWith(Parameterized.class)
@SuppressWarnings({"SpellCheckingInspection", "ConstantConditions"})
public class EmbeddedBackendTest {

    public EmbeddedBackendTest(final boolean initializing) {
        this.init = initializing;
    }

    @Parameterized.Parameters
    public static List<Object[]> data() {
        return Arrays.asList(new Object[][]{
            {true}, {false}
        });
    }


    @ClassRule
    public static TemporaryFolder tmpFolder = new TemporaryFolder();
    private static EmbeddedBackend graph;


    @AfterClass
    public static void destroy() {
        graph.shutdown();
        tmpFolder.delete();
    }

    @BeforeClass
    public static void init() throws IOException {

        final File storeDir = tmpFolder.newFolder();
        graph = new EmbeddedBackend(//
            () -> new GraphDatabaseFactory()//
                .newEmbeddedDatabaseBuilder(storeDir)
                .newGraphDatabase(), () -> {
            try {
                return BatchInserters.inserter(storeDir);
            } catch (final IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
    }

    @Before
    public void clean() {
        final GraphDatabaseService db = graph.getDb();
        final Transaction tx = db.beginTx();
        db.execute("MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r");
        assertThat(db, is(theEmptyGraph()));
        done(tx);
    }

    private final boolean init;

    @Test
    public void addASequenceTreeWithJustARootItem() {

        graph.writer(init) //
            .add( //
                aStringSequence()//
                    .withRoot("rootid") //
                    .withParentDimension("rootdim") //
                    .withChildDimension("leaf") //
                    .withLeaves(/* Empty. */) //
            ) //
            .commit();

        final Transaction tx = tx();
        assertThat(graph.getDb(), is(//
            aGraph()//
                .ofOrder(1L) //
                .containing( //
                    aNode() //
                        .withIdentifier("rootid") //
                        .inDimension("rootdim") //
                ) //
        ));
        done(tx);
    }

    @Test
    public void addASequenceTreeWithARootAndOneLeaf() {

        graph.writer(init) //
            .add( //
                aStringSequence()//
                    .withRoot("rootid") //
                    .withParentDimension("rootdim") //
                    .withChildDimension("leafdim") //
                    .withLeaves("oneleaf") //
            ) //
            .commit();

        final Transaction tx = tx();
        assertThat(graph.getDb(), is(//
            aGraph()//
                .ofOrder(2L) //
                .containing( //
                    aNode() //
                        .withIdentifier("oneleaf") //
                        .inDimension("leafdim") //
                        .ofDegree(1) //
                        .ofOutgoingDegree(1) //
                        .with( //
                            aRelation()//
                                .to( //
                                    aNode() //
                                        .withIdentifier("rootid") //
                                        .inDimension("rootdim") //
                                        .ofIncomingDegree(1) //
                                ) //
                                .inDimension("leafdim") //
                                .atPosition(0)//
                        ) //
                )));
        done(tx);

    }

    @Test
    public void addASequenceTreeWithARootAndThreeLeaves() {

        graph.writer(init) //
            .add( //
                aStringSequence()//
                    .withRoot("the root") //
                    .withParentDimension("root") //
                    .withChildDimension("leaf") //
                    .withLeaves("one", "two", "three") //
            ) //
            .commit();

        final Transaction tx = tx();
        assertThat(graph.getDb(), is(//
            aGraph()//
                .ofOrder(4L) //
                .containing( //
                    aNode() //
                        .ofIncomingDegree(3) //
                        .ofOutgoingDegree(0)//
                        .withIdentifier("the root") //
                        .inDimension("root") //
                        .with(//
                            aRelation() //
                                .from(//
                                    aNode() //
                                        .withIdentifier("one") //
                                        .inDimension("leaf") //
                                        .ofIncomingDegree(0) //
                                        .ofOutgoingDegree(1)//
                                )//
                                .inDimension("leaf")//
                                .atPosition(0),//
                            aRelation() //
                                .from(//
                                    aNode() //
                                        .withIdentifier("two") //
                                        .inDimension("leaf") //
                                        .ofIncomingDegree(0) //
                                        .ofOutgoingDegree(1)//
                                )//
                                .inDimension("leaf")//
                                .atPosition(1),//
                            aRelation() //
                                .from(//
                                    aNode() //
                                        .withIdentifier("three") //
                                        .inDimension("leaf") //
                                        .ofIncomingDegree(0) //
                                        .ofOutgoingDegree(1)//
                                )//
                                .inDimension("leaf")//
                                .atPosition(2)//
                        )
                ) //
        ));
        done(tx);
    }

    @Test
    public void addSameSequenceTwiceInSameTransaction() {

        graph.writer(init) //
            .add( //
                aStringSequence()//
                    .withRoot("rootid") //
                    .withParentDimension("rootdim") //
                    .withChildDimension("leafdim") //
                    .withLeaves("oneleaf") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("rootid") //
                    .withParentDimension("rootdim") //
                    .withChildDimension("leafdim") //
                    .withLeaves("oneleaf") //
            ) //
            .commit();

        final Transaction tx = tx();
        assertThat(graph.getDb(), is(//
            aGraph()//
                .ofOrder(2L) //
                .containing( //
                    aNode() //
                        .unique()//
                        .withIdentifier("rootid")//
                        .ofDegree(1) //
                        .with( //
                            aRelation()//
                                .from( //
                                    aNode() //
                                        .unique()
                                        .withIdentifier("oneleaf")
                                        .with(//
                                            aRelation()//
                                                .to( //
                                                    aNode()//
                                                        .unique()
                                                        .withIdentifier("rootid")//
                                                )
                                        )
                                )
                        )
                )));
        done(tx);

    }

    @Test
    public void addSameSequenceTwiceInSeparateTransactions() {

        Assume.assumeFalse(init);
        graph.writer(init) //
            .add( //
                aStringSequence()//
                    .withRoot("rootid") //
                    .withParentDimension("rootdim") //
                    .withChildDimension("leafdim") //
                    .withLeaves("oneleaf") //
            ) //
            .commit();
        graph.writer(init) //
            .add( //
                aStringSequence()//
                    .withRoot("rootid") //
                    .withParentDimension("rootdim") //
                    .withChildDimension("leafdim") //
                    .withLeaves("oneleaf") //
            ) //
            .commit();

        final Transaction tx = tx();
        assertThat(graph.getDb(), is(//
            aGraph()//
                .ofOrder(2L) //
                .containing( //
                    aNode() //
                        .unique()//
                        .withIdentifier("rootid")//
                        .ofDegree(1) //
                        .with( //
                            aRelation()//
                                .from( //
                                    aNode() //
                                        .unique()
                                        .withIdentifier("oneleaf")
                                        .with(//
                                            aRelation()//
                                                .to( //
                                                    aNode()//
                                                        .unique()
                                                        .withIdentifier("rootid")//
                                                )
                                        )
                                )
                        )
                )));
        done(tx);

    }


    @Test
    public void addTwoDisjointSequenceTreesWithSameDimensions() {

        graph.writer(init) //
            .add( //
                aStringSequence()//
                    .withRoot("s1") //
                    .withParentDimension("sentence") //
                    .withChildDimension("token") //
                    .withLeaves("This", "is", "a", "sentence", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("s2") //
                    .withParentDimension("sentence") //
                    .withChildDimension("token") //
                    .withLeaves("That", "was", "really", "the", "truth", "!") //
            ) //
            .commit();

        final Transaction tx = tx();
        assertThat(graph.getDb(), is(//
            aGraph()//
                .ofOrder(13L) //
                .ofSize(13L - 2L) //
                .containing( //
                    aNode() //
                        .inDimension("sentence") //
                        .withIdentifier("s1") //
                        .withAllRelationsBeing(aRelation().inDimension("token")) //
                        .withAllRelatedNodesBeing(aNode().inDimension("token")) //
                        .with(//
                            aRelation().atPosition(0).from(aNode().withIdentifier("This")),//
                            aRelation().atPosition(1).from(aNode().withIdentifier("is")),//
                            aRelation().atPosition(2).from(aNode().withIdentifier("a")),//
                            aRelation().atPosition(3).from(aNode().withIdentifier("sentence")),//
                            aRelation().atPosition(4).from(aNode().withIdentifier("."))//
                        ),
                    aNode() //
                        .inDimension("sentence") //
                        .withIdentifier("s2") //
                        .withAllRelationsBeing(aRelation().inDimension("token")) //
                        .withAllRelatedNodesBeing(aNode().inDimension("token")) //
                        .with(//
                            aRelation().atPosition(0).from(aNode().withIdentifier("That")),//
                            aRelation().atPosition(1).from(aNode().withIdentifier("was")),//
                            aRelation().atPosition(2).from(aNode().withIdentifier("really")),//
                            aRelation().atPosition(3).from(aNode().withIdentifier("the")),//
                            aRelation().atPosition(4).from(aNode().withIdentifier("truth")),//
                            aRelation().atPosition(5).from(aNode().withIdentifier("!"))//
                        ))
        ));
        done(tx);
    }

    @Test
    public void addTwoSequenceTreesWithSameDimensionsAndLeafOverlap() {

        graph.writer(init) //
            .add( //
                aStringSequence()//
                    .withRoot("s1") //
                    .withParentDimension("sentence") //
                    .withChildDimension("token") //
                    .withLeaves("This", "is", "a", "sentence", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("s2") //
                    .withParentDimension("sentence") //
                    .withChildDimension("token") //
                    .withLeaves("This", "is", "really", "the", "truth", ".") //
            ) //
            .commit();

        final Transaction tx = tx();
        assertThat(graph.getDb(), is(//
            aGraph()//
                .ofOrder(10L) //
                .ofSize(11L) //
                .containing( //
                    aNode() //
                        .unique()
                        .inDimension("sentence") //
                        .withIdentifier("s1") //
                        .withAllRelationsBeing(aRelation().inDimension("token")) //
                        .withAllRelatedNodesBeing(aNode().inDimension("token")) //
                        .with(//
                            aRelation().atPosition(0).from(aNode().withIdentifier("This").unique()),//
                            aRelation().atPosition(1).from(aNode().withIdentifier("is").unique()),//
                            aRelation().atPosition(2).from(aNode().withIdentifier("a")),//
                            aRelation().atPosition(3).from(aNode().withIdentifier("sentence")),//
                            aRelation().atPosition(4).from(aNode().withIdentifier(".").unique())//
                        ),
                    aNode() //
                        .unique()
                        .inDimension("sentence") //
                        .withIdentifier("s2") //
                        .withAllRelationsBeing(aRelation().inDimension("token")) //
                        .withAllRelatedNodesBeing(aNode().inDimension("token")) //
                        .with(//
                            aRelation().atPosition(0).from(aNode().withIdentifier("This").unique()),//
                            aRelation().atPosition(1).from(aNode().withIdentifier("is").unique()),//
                            aRelation().atPosition(2).from(aNode().withIdentifier("really")),//
                            aRelation().atPosition(3).from(aNode().withIdentifier("the")),//
                            aRelation().atPosition(4).from(aNode().withIdentifier("truth")),//
                            aRelation().atPosition(5).from(aNode().withIdentifier(".").unique())//
                        ))
        ));
        done(tx);
    }


    @Test
    public void addTwoDisjointSequenceTreesWithDifferentDimensions() {

        graph.writer(init) //
            .add( //
                aStringSequence()//
                    .withRoot("s1") //
                    .withParentDimension("sentence") //
                    .withChildDimension("token") //
                    .withLeaves("This", "is", "a", "sentence", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("s2") //
                    .withParentDimension("lcsentence") //
                    .withChildDimension("lctoken") //
                    .withLeaves("that", "was", "really", "the", "truth", "!") //
            ) //
            .commit();

        final Transaction tx = tx();
        assertThat(graph.getDb(), is(//
            aGraph()//
                .ofOrder(13L) //
                .ofSize(13L - 2L) //
                .containing( //
                    aNode() //
                        .inDimension("sentence") //
                        .withIdentifier("s1") //
                        .withAllRelationsBeing(aRelation().inDimension("token")) //
                        .withAllRelatedNodesBeing(aNode().inDimension("token")) //
                        .with(//
                            aRelation().atPosition(0).from(aNode().withIdentifier("This")),//
                            aRelation().atPosition(1).from(aNode().withIdentifier("is")),//
                            aRelation().atPosition(2).from(aNode().withIdentifier("a")),//
                            aRelation().atPosition(3).from(aNode().withIdentifier("sentence")),//
                            aRelation().atPosition(4).from(aNode().withIdentifier("."))//
                        ),
                    aNode() //
                        .inDimension("lcsentence") //
                        .withIdentifier("s2") //
                        .withAllRelationsBeing(aRelation().inDimension("lctoken")) //
                        .withAllRelatedNodesBeing(aNode().inDimension("lctoken")) //
                        .with(//
                            aRelation().atPosition(0).from(aNode().withIdentifier("that")),//
                            aRelation().atPosition(1).from(aNode().withIdentifier("was")),//
                            aRelation().atPosition(2).from(aNode().withIdentifier("really")),//
                            aRelation().atPosition(3).from(aNode().withIdentifier("the")),//
                            aRelation().atPosition(4).from(aNode().withIdentifier("truth")),//
                            aRelation().atPosition(5).from(aNode().withIdentifier("!"))//
                        ))
        ));
        done(tx);
    }


    @Test
    public void addSequenceTreesWithSameRootAndDifferentChildDimensions() {

        graph.writer(init) //
            .add( //
                aStringSequence()//
                    .withRoot("s1") //
                    .withParentDimension("sentence") //
                    .withChildDimension("token") //
                    .withLeaves("This", "is", "a", "sentence", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("s1") //
                    .withParentDimension("sentence") //
                    .withChildDimension("lctoken") //
                    .withLeaves("that", "is", "a", "sentence", ".") //
            ) //
            .commit();

        final Transaction tx = tx();
        assertThat(graph.getDb(), is(//
            aGraph()//
                .ofOrder(11L) //
                .ofSize(10L) //
                .containing( //
                    aNode() //
                        .inDimension("sentence") //
                        .withIdentifier("s1") //
                        .with(//
                            aRelation().atPosition(0).inDimension("token").from(aNode().inDimension("token").withIdentifier("This")),//
                            aRelation().atPosition(1).inDimension("token").from(aNode().inDimension("token").withIdentifier("is")),//
                            aRelation().atPosition(2).inDimension("token").from(aNode().inDimension("token").withIdentifier("a")),//
                            aRelation().atPosition(3).inDimension("token").from(aNode().inDimension("token").withIdentifier("sentence")),//
                            aRelation().atPosition(4).inDimension("token").from(aNode().inDimension("token").withIdentifier(".")),//
                            aRelation().atPosition(0).inDimension("lctoken").from(aNode().inDimension("lctoken").withIdentifier("that")),//
                            aRelation().atPosition(1).inDimension("lctoken").from(aNode().inDimension("lctoken").withIdentifier("is")),//
                            aRelation().atPosition(2).inDimension("lctoken").from(aNode().inDimension("lctoken").withIdentifier("a")),//
                            aRelation().atPosition(3).inDimension("lctoken").from(aNode().inDimension("lctoken").withIdentifier("sentence")),//
                            aRelation().atPosition(4).inDimension("lctoken").from(aNode().inDimension("lctoken").withIdentifier("."))//
                        ))
        ));
        done(tx);
    }


    @Test
    public void addAHierarchyOnTop() {

        graph.writer(init) //
            .add( //
                aStringSequence()//
                    .withRoot("s1") //
                    .withParentDimension("sentence") //
                    .withChildDimension("token") //
                    .withLeaves("This", "is", "a", "sentence", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("s2") //
                    .withParentDimension("sentence") //
                    .withChildDimension("token") //
                    .withLeaves("This", "is", "really", "the", "truth", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("doc1") //
                    .withParentDimension("doc") //
                    .withChildDimension("sentence") //
                    .withLeaves("s1", "s2") //
            ) //
            .commit();

        final Transaction tx = tx();
        assertThat(graph.getDb(), is(//
            aGraph()//
                .ofOrder(11L) //
                .ofSize(13L) //
                .containing( //
                    aNode()
                        .withIdentifier("doc1") //
                        .inDimension("doc") //
                        .withAllIncomingRelationsBeing(aRelation().inDimension("sentence")) //
                        .withAllIncomingRelatedNodesBeing(aNode().inDimension("sentence")) //
                        .with(
                            aRelation().from(
                                aNode() //
                                    .unique()
                                    .inDimension("sentence") //
                                    .withIdentifier("s1") //
                                    .withAllIncomingRelationsBeing(aRelation().inDimension("token")) //
                                    .withAllIncomingRelatedNodesBeing(aNode().inDimension("token")) //
                                    .with(//
                                        aRelation().atPosition(0).from(aNode().withIdentifier("This").unique()),//
                                        aRelation().atPosition(1).from(aNode().withIdentifier("is").unique()),//
                                        aRelation().atPosition(2).from(aNode().withIdentifier("a")),//
                                        aRelation().atPosition(3).from(aNode().withIdentifier("sentence")),//
                                        aRelation().atPosition(4).from(aNode().withIdentifier(".").unique())//
                                    )
                            ),
                            aRelation().from(
                                aNode() //
                                    .unique()
                                    .inDimension("sentence") //
                                    .withIdentifier("s2") //
                                    .withAllIncomingRelationsBeing(aRelation().inDimension("token")) //
                                    .withAllIncomingRelatedNodesBeing(aNode().inDimension("token")) //
                                    .with(//
                                        aRelation().atPosition(0).from(aNode().withIdentifier("This").unique()),//
                                        aRelation().atPosition(1).from(aNode().withIdentifier("is").unique()),//
                                        aRelation().atPosition(2).from(aNode().withIdentifier("really")),//
                                        aRelation().atPosition(3).from(aNode().withIdentifier("the")),//
                                        aRelation().atPosition(4).from(aNode().withIdentifier("truth")),//
                                        aRelation().atPosition(5).from(aNode().withIdentifier(".").unique())//
                                    ))
                        )
                )


        ));
        done(tx);
    }


    @Test
    public void addCanAddInAnyOrder() {

        graph.writer(init) //
            .add( //
                aStringSequence()//
                    .withRoot("s2") //
                    .withParentDimension("sentence") //
                    .withChildDimension("token") //
                    .withLeaves("This", "is", "really", "the", "truth", ".") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("doc1") //
                    .withParentDimension("doc") //
                    .withChildDimension("sentence") //
                    .withLeaves("s1", "s2") //
            ) //
            .add( //
                aStringSequence()//
                    .withRoot("s1") //
                    .withParentDimension("sentence") //
                    .withChildDimension("token") //
                    .withLeaves("This", "is", "a", "sentence", ".") //
            ) //
            .commit();

        final Transaction tx = tx();
        assertThat(graph.getDb(), is(//
            aGraph()//
                .ofOrder(11L) //
                .ofSize(13L) //
                .containing( //
                    aNode()
                        .withIdentifier("doc1") //
                        .inDimension("doc") //
                        .withAllIncomingRelationsBeing(aRelation().inDimension("sentence")) //
                        .withAllIncomingRelatedNodesBeing(aNode().inDimension("sentence")) //
                        .with(
                            aRelation().from(
                                aNode() //
                                    .unique()
                                    .inDimension("sentence") //
                                    .withIdentifier("s1") //
                                    .withAllIncomingRelationsBeing(aRelation().inDimension("token")) //
                                    .withAllIncomingRelatedNodesBeing(aNode().inDimension("token")) //
                                    .with(//
                                        aRelation().atPosition(0).from(aNode().withIdentifier("This").unique()),//
                                        aRelation().atPosition(1).from(aNode().withIdentifier("is").unique()),//
                                        aRelation().atPosition(2).from(aNode().withIdentifier("a")),//
                                        aRelation().atPosition(3).from(aNode().withIdentifier("sentence")),//
                                        aRelation().atPosition(4).from(aNode().withIdentifier(".").unique())//
                                    )
                            ),
                            aRelation().from(
                                aNode() //
                                    .unique()
                                    .inDimension("sentence") //
                                    .withIdentifier("s2") //
                                    .withAllIncomingRelationsBeing(aRelation().inDimension("token")) //
                                    .withAllIncomingRelatedNodesBeing(aNode().inDimension("token")) //
                                    .with(//
                                        aRelation().atPosition(0).from(aNode().withIdentifier("This").unique()),//
                                        aRelation().atPosition(1).from(aNode().withIdentifier("is").unique()),//
                                        aRelation().atPosition(2).from(aNode().withIdentifier("really")),//
                                        aRelation().atPosition(3).from(aNode().withIdentifier("the")),//
                                        aRelation().atPosition(4).from(aNode().withIdentifier("truth")),//
                                        aRelation().atPosition(5).from(aNode().withIdentifier(".").unique())//
                                    ))
                        )
                )


        ));
        done(tx);
    }


    private Dimension dim(final String rootdim) {
        return () -> rootdim;
    }


    private void done(final Transaction tx) {
        tx.success();
        tx.close();
    }

    private Transaction tx() {
        return graph.getDb().beginTx(5, SECONDS);
    }


}