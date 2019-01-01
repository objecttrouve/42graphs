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

import org.objecttrouve.fourtytwo.graphs.api.SequenceTree;
import org.objecttrouve.fourtytwo.graphs.api.GraphWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValidatingWriter implements GraphWriter {

  private static final Logger logger = LoggerFactory.getLogger(ValidatingWriter.class);
    private final GraphWriter delegate;

    ValidatingWriter(final GraphWriter delegate) {
        this.delegate = delegate;
    }

    @Override
    public void commit() {
        delegate.commit();
    }

    @Override
    public <T, U> GraphWriter add(final SequenceTree<T, U> sequenceTree) {
      logger.trace("Validating {} {}...", SequenceTree.class, sequenceTree);
        if (sequenceTree == null){
            abort();
            throw new IllegalArgumentException("Sequence tree not be null.");
        }
        if (sequenceTree.getRoot() == null) {
            abort();
            throw new IllegalArgumentException("Root item must not be null.");
        }
        if (sequenceTree.getRootDimension() == null) {
            abort();
            throw new IllegalArgumentException("Root dimension must not be null.");
        }
        if (sequenceTree.getValues() == null) {
            abort();
            throw new IllegalArgumentException("Values must not be null. (Empty would be OK.)");
        }
        if (sequenceTree.getLeafDimension() == null) {
            abort();
            throw new IllegalArgumentException("Leaf dimension must not be null.");
        }

        return delegate.add(sequenceTree);
    }

    @Override
    public void abort() {
        delegate.abort();
    }
}
