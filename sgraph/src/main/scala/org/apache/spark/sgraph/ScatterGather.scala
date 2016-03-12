/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sgraph

import scala.reflect.ClassTag

object ScatterGather {
  def apply[VD: ClassTag, ED: ClassTag, A: ClassTag]
  (graph: Graph[VD, ED],
    initalMsg: A,
    maxIterations: Int = Int.MaxValue)
  (vprog: (VertexId, VD, A) => VD,
    scatter: (VertexId, VD, Int) => (VertexId, A),
    gather: (A, A) => A)= {
    var g: Graph[VD, ED] = graph.mapVertices((vid, vdata) => vprog(vid, vdata, initalMsg)).cache()
    //graph.unpersistVertices(false)
    // get the updates
    var updates= g.mapReduceUpdates(scatter, gather).cache()
    var numUpdates = updates.count()
    var prevG: Graph[VD, ED] = null
    var i = 0
    while(numUpdates > 0 && i < maxIterations) {
      prevG = g
      // wrap a new UpdateVertexRDD with the vertex program `vprog`
      val newUpdates = UpdateVertexRDD(g.vertices.innerJoin(updates)(vprog)).cache()
      // outer join, update vertices in original VertexRDD and SourceVertexRDD
      g = g.outerJoinVertices(newUpdates){(vid, old, newOpt) => newOpt.getOrElse(old)}
      g.cache()
      g.vertices.count()
      val oldUpdates = newUpdates
      val oldUpdates2 = updates
      updates = g.mapReduceUpdates(scatter, gather).cache()
      numUpdates = updates.count()
      oldUpdates.unpersist(blocking = false)
      oldUpdates2.unpersist(blocking = false)
      prevG.unpersistVertices(blocking = false)
      i = i + 1
    }
    g
  }
}
