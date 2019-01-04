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

package org.objecttrouve.fourtytwo.graphs.examples.common;

import org.objecttrouve.fourtytwo.graphs.api.Dimension;
import org.objecttrouve.fourtytwo.graphs.api.SequenceTree;
import org.objecttrouve.fourtytwo.graphs.api.Value;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

public class StringSequenceTree implements SequenceTree<String, String> {

    private final String root;
    private final String rootDimension;
    private final String childDimension;
    private final List<String> stringSequence;


    public StringSequenceTree(final String root, final String rootDimension, final String childDimension, final String... strings) {
        this.root = root;
        this.rootDimension = rootDimension;
        this.childDimension = childDimension;
        this.stringSequence = asList(strings);
    }

    @Override
    public Value<String> getRoot() {
        return () -> root;
    }

    @Override
    public Dimension getRootDimension() {
        return () -> rootDimension;
    }
    @Override
    public Dimension getChildDimension() {
        return () -> childDimension;
    }

    @Override
    public List<Value<String>> getValues() {
        return stringSequence.stream()//
                .map(s -> (Value<String>) () -> s)//
                .collect(Collectors.toList());
    }
}
