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

import org.junit.Test;
import org.objecttrouve.fourtytwo.graphs.api.Graph;
import org.objecttrouve.fourtytwo.graphs.api.GraphWriter;
import org.objecttrouve.fourtytwo.graphs.mocks.BackendMock;
import org.objecttrouve.fourtytwo.graphs.mocks.WritingMock;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.objecttrouve.fourtytwo.graphs.mocks.TestStringSequenceTree.aStringSequence;
import static org.objecttrouve.fourtytwo.graphs.matchers.SequenceTreeMatcher.aSequenceTree;

public class ValidatingEmbeddedBackendTest {

    @Test
    public void addANullSequenceTree() {

        final WritingMock writer = WritingMock.ofWriting();
        final BackendMock backend = BackendMock.ofBackend().writing(writer);
        final Graph graph = new ValidatingEmbeddedBackend(backend.getMock());

        try {
            graph.writer(false) //
                    .add(null); //
        } catch (final IllegalArgumentException e) {
            /* Expected. */
        }

        writer.verifyNoSequenceAdded();
        writer.verifyAborted();
    }

    @Test
    public void addASequenceTreeWithANullRoot() {

        final WritingMock writer = WritingMock.ofWriting();
        final BackendMock backend = BackendMock.ofBackend().writing(writer);
        final Graph graph = new ValidatingEmbeddedBackend(backend.getMock());
        try {
            graph.writer(false) //
                    .add( //
                            aStringSequence()//
                                    .withRoot(null) //
                                    .withRootDimension("not null root dimension") //
                                    .withLeaves("not null leaf") //
                                    .withLeafDimension("not null leaf dimension") //
                    );

        } catch (final IllegalArgumentException e) {
            /* Expected. */
        }

        writer.verifyNoSequenceAdded();
        writer.verifyAborted();

    }

    @Test
    public void addASequenceTreeWithANullRootDimension() {

        final WritingMock writer = WritingMock.ofWriting();
        final BackendMock backend = BackendMock.ofBackend().writing(writer);
        final Graph graph = new ValidatingEmbeddedBackend(backend.getMock());
        try {
            graph.writer(false) //
                    .add( //
                            aStringSequence()//
                                    .withRoot("not null") //
                                    .withRootDimension(null) //
                                    .withLeaves("not null leaf") //
                                    .withLeafDimension("not null leaf dimension") //
                    );

        } catch (final IllegalArgumentException e) {
            /* Expected. */
        }

        writer.verifyNoSequenceAdded();
        writer.verifyAborted();

    }

    @Test
    public void addASequenceTreeWithANullLeafDimensionWhenThereAreLeafs() {

        final WritingMock writer = WritingMock.ofWriting();
        final BackendMock backend = BackendMock.ofBackend().writing(writer);
        final Graph graph = new ValidatingEmbeddedBackend(backend.getMock());
        try {
            graph.writer(false) //
                    .add( //
                            aStringSequence()//
                                    .withRoot("not null") //
                                    .withRootDimension("not null root dimension") //
                                    .withLeaves("not null leaf") //
                                    .withLeafDimension(null) //
                    );

        } catch (final IllegalArgumentException e) {
            /* Expected. */
        }

        writer.verifyNoSequenceAdded();
        writer.verifyAborted();

    }

    @Test
    public void addASequenceTreeWithANullLeafList() {

        final WritingMock writer = WritingMock.ofWriting();
        final BackendMock backend = BackendMock.ofBackend().writing(writer);
        final Graph graph = new ValidatingEmbeddedBackend(backend.getMock());
        try {
            graph.writer(false) //
                    .add( //
                            aStringSequence()//
                                    .withRoot("not null") //
                                    .withRootDimension("not null root dimension") //
                                    .withLeafSequence(null) //
                                    .withLeafDimension("not null leaf dimension") //
                    );

        } catch (final IllegalArgumentException e) {
            /* Expected. */
        }

        writer.verifyNoSequenceAdded();
        writer.verifyAborted();

    }

    @Test
    public void addASequenceTreeOnTheHappyPath() {

        final WritingMock writer = WritingMock.ofWriting();
        final BackendMock backend = BackendMock.ofBackend().writing(writer);
        final Graph graph = new ValidatingEmbeddedBackend(backend.getMock());
        graph.writer(false) //
                .add( //
                        aStringSequence()//
                                .withRoot("root") //
                                .withRootDimension("rootDimension") //
                                .withLeaves("l1", "l2", "l3") //
                                .withLeafDimension("leafDimension") //
                );

        writer.verifyAddedSequence(//
                aSequenceTree(String.class, String.class)//
                        .withRootId("root") //
                        .withRootDimension("rootDimension") //
                        .withLeaves("l1", "l2", "l3") //
                        .withLeafDimension("leafDimension") //
        );

    }

    @Test
    public void addASequenceTreeWithoutValues() {

        final WritingMock writer = WritingMock.ofWriting();
        final BackendMock backend = BackendMock.ofBackend().writing(writer);
        final Graph graph = new ValidatingEmbeddedBackend(backend.getMock());
        graph.writer(false) //
                .add( //
                        aStringSequence()//
                                .withRoot("root") //
                                .withRootDimension("rootDimension") //
                                .withLeaves(/* Empty List. */) //
                                .withLeafDimension("leafDimension") //
                );

        writer.verifyAddedSequence(//
                aSequenceTree(String.class, String.class)//
                        .withRootId("root") //
                        .withRootDimension("rootDimension") //
                        .withLeaves(/* Empty List. */) //
                        .withLeafDimension("leafDimension") //
        );

    }

    @Test
    public void addASequenceTreeOnTheHappyPathReturnsABuilder() {

        final WritingMock writer = WritingMock.ofWriting();
        final BackendMock backend = BackendMock.ofBackend().writing(writer);
        final Graph graph = new ValidatingEmbeddedBackend(backend.getMock());
        final GraphWriter builder = graph.writer(false) //
                .add( //
                        aStringSequence()//
                                .withRoot("root") //
                                .withRootDimension("rootDimension") //
                                .withLeaves("l1", "l2", "l3") //
                                .withLeafDimension("leafDimension") //
                );

        assertThat(builder, notNullValue());
    }

    @Test
    public void addASequenceTreeAndCommit() {

        final WritingMock writer = WritingMock.ofWriting();
        final BackendMock backend = BackendMock.ofBackend().writing(writer);
        final Graph graph = new ValidatingEmbeddedBackend(backend.getMock());
        graph.writer(false) //
                .add( //
                        aStringSequence()//
                                .withRoot("root") //
                                .withRootDimension("rootDimension") //
                                .withLeaves("l1", "l2", "l3") //
                                .withLeafDimension("leafDimension") //
                ) //
                .commit();


        writer.verifyAddedSequence(//
                aSequenceTree(String.class, String.class)//
                        .withRootId("root") //
                        .withRootDimension("rootDimension") //
                        .withLeaves("l1", "l2", "l3") //
                        .withLeafDimension("leafDimension") //
        )
        .verifyCommitted();

    }
}