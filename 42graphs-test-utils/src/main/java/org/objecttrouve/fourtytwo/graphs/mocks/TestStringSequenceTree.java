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
    private String parentDimension;
    private String childDimension;
    private List<String> stringSequence;

    public static TestStringSequenceTree aStringSequence() {
        return new TestStringSequenceTree();
    }

    private TestStringSequenceTree(){
    }

    public TestStringSequenceTree withRoot(final String root){
        this.root = root;
        return this;
    }

    public TestStringSequenceTree withChildDimension(final String childDimension) {
        this.childDimension = childDimension;
        return this;
    }

    public TestStringSequenceTree withParentDimension(final String parentDimension) {
        this.parentDimension = parentDimension;
        return this;
    }

    public TestStringSequenceTree withLeaves(final String... stringSequence){
            return withLeafSequence(Arrays.asList(stringSequence));
    }

    public TestStringSequenceTree withLeafSequence(final List<String> stringSequence) {
        this.stringSequence = stringSequence;
        return this;
    }

    public TestStringSequenceTree(final String root, final String parentDimension, final String childDimension, final String... strings) {
        this.root = root;
        this.parentDimension = parentDimension;
        this.childDimension = childDimension;
        this.stringSequence = asList(strings);
    }

    @Override
    public Value<String> getRoot() {
        return root != null ? () -> root : null;
    }

    @Override
    public Dimension getParentDimension() {
        return parentDimension != null ? () -> parentDimension : null;
    }
    @Override
    public Dimension getChildDimension() {
        return childDimension != null ? () -> childDimension : null ;
    }

    @Override
    public List<Value<String>> getValues() {
        return stringSequence != null ? stringSequence.stream()//
                .map(s -> (Value<String>) () -> s)//
                .collect(Collectors.toList()) //
                : null ;
    }
}
