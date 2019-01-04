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

public class TestIntegerSequenceTree implements SequenceTree<Integer, Integer> {

    private Integer root;
    private String parentDimension;
    private String childDimension;
    private List<Integer> stringSequence;

    public static TestIntegerSequenceTree anIntegerSequence() {
        return new TestIntegerSequenceTree();
    }

    private TestIntegerSequenceTree(){
    }

    public TestIntegerSequenceTree withRoot(final Integer root){
        this.root = root;
        return this;
    }

    public TestIntegerSequenceTree withChildDimension(final String childDimension) {
        this.childDimension = childDimension;
        return this;
    }

    public TestIntegerSequenceTree withParentDimension(final String parentDimension) {
        this.parentDimension = parentDimension;
        return this;
    }

    public TestIntegerSequenceTree withLeaves(final Integer... stringSequence){
            return withLeafSequence(Arrays.asList(stringSequence));
    }

    private TestIntegerSequenceTree withLeafSequence(final List<Integer> stringSequence) {
        this.stringSequence = stringSequence;
        return this;
    }

    public TestIntegerSequenceTree(final Integer root, final String parentDimension, final String childDimension, final Integer... strings) {
        this.root = root;
        this.parentDimension = parentDimension;
        this.childDimension = childDimension;
        this.stringSequence = asList(strings);
    }

    @Override
    public Value<Integer> getRoot() {
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
    public List<Value<Integer>> getValues() {
        return stringSequence != null ? stringSequence.stream()//
                .map(i -> (Value<Integer>) () -> i)//
                .collect(Collectors.toList()) //
                : null ;
    }
}
