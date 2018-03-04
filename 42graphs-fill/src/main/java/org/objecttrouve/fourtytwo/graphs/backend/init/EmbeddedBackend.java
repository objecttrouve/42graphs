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

package org.objecttrouve.fourtytwo.graphs.backend.init;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.objecttrouve.fourtytwo.graphs.api.Graph;
import org.objecttrouve.fourtytwo.graphs.api.GraphWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.function.Supplier;

@NotThreadSafe
public class EmbeddedBackend implements Graph {

  private static final Logger logger = LoggerFactory.getLogger(CachingBatchInitializer.class);
  private GraphDatabaseService db;
  private final Supplier<GraphDatabaseService> serviceFactory;
  private final Supplier<BatchInserter> batchFactory;


  public EmbeddedBackend(final Supplier<GraphDatabaseService> serviceFactory, final Supplier<BatchInserter> batchFactory) {
    this.serviceFactory = serviceFactory;
    this.batchFactory = batchFactory;
    this.db = serviceFactory.get();
    logger.debug("Created {}.", EmbeddedBackend.class.getName());
  }

  @Override
  public GraphWriter writer(final boolean init) {
    if (!init) {
      final GraphDatabaseService dbs = getDb();
      final Transaction transaction = dbs.beginTx();
      return new TransactionalWriter(db, transaction);
    } else {
      db.shutdown();
      return new CachingBatchInitializer(batchFactory.get());
    }
  }

  GraphDatabaseService getDb() {
    if (!db.isAvailable(10000)) {
      db = serviceFactory.get();
    }
    return db;
  }

  @Override
  public void shutdown() {
    db.shutdown();
  }


}
