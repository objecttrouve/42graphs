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

package org.objecttrouve.fourtytwo.graphs.mocks;

import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.objecttrouve.fourtytwo.graphs.api.SequenceTree;
import org.objecttrouve.fourtytwo.graphs.api.GraphWriter;
import org.objecttrouve.fourtytwo.graphs.matchers.SequenceTreeMatcher;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class WritingMock {
    private final GraphWriter graphWriter = mock(GraphWriter.class);


    public static WritingMock ofWriting(){
        return new WritingMock();
    }

    private WritingMock(){
        //noinspection unchecked
        when(graphWriter.add(any(SequenceTree.class))).thenReturn(graphWriter);
    }

    @SuppressWarnings("WeakerAccess")
    public GraphWriter getMock(){
        return graphWriter;
    }

    @SuppressWarnings("UnusedReturnValue")
    public WritingMock verifyNoSequenceAdded(){
        //noinspection unchecked
        verify(graphWriter, never()).add(any(SequenceTree.class));
        return this;
    }

    public void verifyAborted() {
        Mockito.verify(graphWriter, times(1)).abort();
    }

    @SuppressWarnings("UnusedReturnValue")
    public WritingMock verifyAddedSequence(final SequenceTreeMatcher expectedTreeSequence) {
        final ArgumentCaptor<SequenceTree> stArg = ArgumentCaptor.forClass(SequenceTree.class);
        //noinspection unchecked
        verify(graphWriter, times(1)).add(stArg.capture());
        //noinspection unchecked
        assertThat(stArg.getValue(), is(expectedTreeSequence));
        return this;
    }

    public void verifyCommitted() {
        Mockito.verify(graphWriter, times(1)).commit();
    }
}
