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

import org.objecttrouve.fourtytwo.graphs.api.Dimension;
import org.objecttrouve.fourtytwo.graphs.api.SequenceTree;
import org.objecttrouve.fourtytwo.graphs.api.Value;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

public class TestStringSequenceTree implements SequenceTree<String, String> {

    private String root;
    private String rootDimension;
    private String leafDimension;
    private List<String> stringSequence;

    public static TestStringSequenceTree aStringSequence() {
        return new TestStringSequenceTree();
    }

    public TestStringSequenceTree(){
    }

    public TestStringSequenceTree withRoot(final String root){
        this.root = root;
        return this;
    }

    public TestStringSequenceTree withLeafDimension(final String leafDimension) {
        this.leafDimension = leafDimension;
        return this;
    }

    public TestStringSequenceTree withRootDimension(final String rootDimension) {
        this.rootDimension = rootDimension;
        return this;
    }

    public TestStringSequenceTree withLeaves(final String... stringSequence){
            return withLeafSequence(Arrays.asList(stringSequence));
    }

    public TestStringSequenceTree withLeafSequence(final List<String> stringSequence) {
        this.stringSequence = stringSequence;
        return this;
    }

    public TestStringSequenceTree(final String root, final String rootDimension, final String leafDimension, final String... strings) {
        this.root = root;
        this.rootDimension = rootDimension;
        this.leafDimension = leafDimension;
        this.stringSequence = asList(strings);
    }

    @Override
    public Value<String> getRoot() {
        return root != null ? () -> root : null;
    }

    @Override
    public Dimension getRootDimension() {
        return rootDimension != null ? () -> rootDimension : null;
    }
    @Override
    public Dimension getLeafDimension() {
        return leafDimension != null ? () -> leafDimension : null ;
    }

    @Override
    public List<Value<String>> getValues() {
        return stringSequence != null ? stringSequence.stream()//
                .map(s -> (Value<String>) () -> s)//
                .collect(Collectors.toList()) //
                : null ;
    }
}
