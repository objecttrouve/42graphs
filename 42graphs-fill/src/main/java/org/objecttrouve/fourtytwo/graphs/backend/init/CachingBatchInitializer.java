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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.objecttrouve.fourtytwo.graphs.api.Dimension;
import org.objecttrouve.fourtytwo.graphs.api.SequenceTree;
import org.objecttrouve.fourtytwo.graphs.api.Value;
import org.objecttrouve.fourtytwo.graphs.api.GraphWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CachingBatchInitializer implements GraphWriter {

  private static final Logger logger = LoggerFactory.getLogger(CachingBatchInitializer.class);

  private final BatchInserter init;

  LoadingCache<NodeKey, Long> nodes = CacheBuilder.newBuilder()
      .build(
          new CacheLoader<NodeKey, Long>() {
            public Long load(final NodeKey key){
              final Object id = key.getId();
              final Map<String, Object> props = new HashMap<>();
              props.put(Value.idKey, id);
              return init.createNode(props, Label.label(key.getDimension()));
            }
          });
  LoadingCache<RelationKey, Long> relations = CacheBuilder.newBuilder()
      .build(
          new CacheLoader<RelationKey, Long>() {
            public Long load(final RelationKey key){
              final long childNode = key.getChildNode();
              final long parentNode = key.getParentNode();
              final String type = key.getDimension();
              final int position = key.getPosition();
              final Map<String, Object> props = new HashMap<>();
              props.put(Dimension.positionKey, position);
              return init.createRelationship(childNode, parentNode, RelationshipType.withName(type), props);
            }
          });


  CachingBatchInitializer(final BatchInserter init) {
    this.init = init;
  }



  @Override
  public <T, U> GraphWriter add(final SequenceTree<T, U> sequenceTree) {
    logger.trace("Adding {} {}...", SequenceTree.class.getName(), sequenceTree.toString());
    final Dimension rootDimension = sequenceTree.getRootDimension();
    final Long parentId = nodes.getUnchecked(NodeKey.key(sequenceTree.getRoot().getIdentifier(), rootDimension.getName()));
    final List<Value<U>> values = sequenceTree.getValues();
    final Dimension leafDimension = sequenceTree.getLeafDimension();
    for (int i = 0; i < values.size(); i++) {
      final Value<U> child = values.get(i);
      final Long childId = nodes.getUnchecked(NodeKey.key(child.getIdentifier(), leafDimension.getName()));
      relations.getUnchecked(RelationKey.key(childId, parentId, leafDimension.getName(), i));
    }
    init.setNodeProperty(parentId, leafDimension.childrenSizeKey(), values.size());
    return this;
  }

  @Override
  public void abort() {
    // TODO: What's happening with pending changes? Handle this more gently!!
    throw new UnsupportedOperationException("Cancellation is not supported.");
  }

  @Override
  public void commit() {
    logger.info("Commit and shutdown {}...", CachingBatchInitializer.class.getName());
    init.shutdown();
  }

}
