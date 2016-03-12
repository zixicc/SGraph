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

package org.apache.spark.sgraph.impl

import scala.reflect.ClassTag

import org.apache.spark.SparkContext._
import org.apache.spark.sgraph._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.ClosureCleaner
import org.apache.spark.HashPartitioner


/**
 * A graph that supports computation on graphs.
 *
 * Graphs are represented using two classes of data: vertex-partitioned and
 * edge-partitioned. `vertices` contains vertex attributes, which are vertex-partitioned. `edges`
 * contains edge attributes, which are edge-partitioned. For operations on vertex neighborhoods,
 * vertex attributes are replicated to the edge partitions where they appear as sources or
 * destinations. `routingTable` stores the routing information for shipping vertex attributes to
 * edge partitions. `replicatedVertexView` stores a view of the replicated vertex attributes created
 * using the routing table.
 */
class GraphImpl[VD: ClassTag, ED: ClassTag] protected (
    @transient val sourceVertices: SourceVertexRDD[VD],
    @transient val vertices: VertexRDD[VD],
    @transient val edges: EdgeRDD[ED])
  extends Graph[VD, ED] with Serializable {

  /** Default constructor is provided to support serialization */
  protected def this() = this(null, null, null)

  override def persist(newLevel: StorageLevel): Graph[VD, ED] = {
    vertices.persist(newLevel)
    edges.persist(newLevel)
    this
  }

  override def cache(): Graph[VD, ED] = {
    sourceVertices.cache()
    vertices.cache()
    edges.cache()
    this
  }

  override def unpersistVertices(blocking: Boolean = true): Graph[VD, ED] = {
    vertices.unpersist(blocking)
    sourceVertices.unpersist(blocking)
    this
  }

  override def reverse: Graph[VD, ED] = {
    val newETable = edges.mapEdgePartitions((pid, part) => part.reverse)
    new GraphImpl(sourceVertices, vertices, newETable)
  }

  override def mapVertices[VD2: ClassTag](f: (VertexId, VD) => VD2): Graph[VD2, ED] = {
    GraphImpl(sourceVertices.mapVertexParitions(_.map(f)), vertices.mapVertexPartitions(_.map(f)), edges)
  }

  override def mapEdges[ED2: ClassTag](
      f: (PartitionID, Iterator[Edge[ED]]) => Iterator[ED2]): Graph[VD, ED2] = {
    val newETable = edges.mapEdgePartitions((pid, part) => part.map(f(pid, part.iterator)))
    new GraphImpl(sourceVertices, vertices, newETable)
  }

  // map and reduce updates for algorithms without accessing destination vertex attribute
  override def mapReduceUpdates[A: ClassTag]
  (mapFunc: (VertexId, VD, Int) => (VertexId, A),reduceFunc: (A, A) => A)
  : UpdateVertexRDD[A] = {
    ClosureCleaner.clean(mapFunc)
    ClosureCleaner.clean(reduceFunc)

    val mapResult = sourceVertices.partitionsRDD.zipPartitions(edges.partitionsRDD, true) { (sVPartIter, ePartIter) =>
      val (_, edgePartition) = ePartIter.next()
      val sourceVertexPartition = sVPartIter.next()
      val dstIds = edgePartition.dstIds
      var pos = 0
      var vid: VertexId = 0
      var degree = 0
      val itr = sourceVertexPartition.indexIterator.flatMap(e => {
        pos += degree
        vid = e._1
        degree = e._2
        val attr = e._3
        val iterator = Iterator.tabulate(degree)(x => {
          mapFunc(dstIds(pos + x), attr, degree)
        })
        iterator
      })
      UpdateVertexPartition(itr, reduceFunc).iterator
    }
    UpdateVertexRDD(mapResult, reduceFunc, vertices.partitioner.get)
  }

  // map and reduce updates for algorithms accessing destination vertex attribute
  def mapReduceUpdates2[A: ClassTag, VD2: ClassTag]
  (mapFunc: (VertexId, VD, VertexId, VD) => Iterator[(VertexId, VD2)],reduceFunc: (VD2, VD2) => VD2, activeVertexSet: UpdateVertexRDD[Null])
  : UpdateVertexRDD[VD2] = {
    ClosureCleaner.clean(mapFunc)
    ClosureCleaner.clean(reduceFunc)

    val shippedVertices = vertices.shipDestinationVertices().partitionBy(new HashPartitioner(edges.partitions.size))
    val dstVertices = shippedVertices.mapPartitions(iter => {
      Iterator(VertexPartition(iter.flatMap(_._2.iterator)))
    }, preservesPartitioning = true)
    if (null == activeVertexSet) {
      val mapResult = sourceVertices.partitionsRDD.zipPartitions(edges.partitionsRDD, dstVertices, true) { (sVPartIter, ePartIter, dstVPartIter) =>
        val (_, edgePartition) = ePartIter.next()
        val sourceVertexPartition = sVPartIter.next()
        val dstIds = edgePartition.dstIds
        val dstVPart = dstVPartIter.next()
        var pos = 0
        var vid: VertexId = 0
        var degree = 0
        val itr = sourceVertexPartition.indexIterator.flatMap(e => {
          pos += degree
          vid = e._1
          degree = e._2
          val attr = e._3
          (0 until degree).iterator.flatMap(i => {
            mapFunc(vid, attr, dstIds(pos + i), dstVPart(dstIds(pos + i)))
          })
        })
        UpdateVertexPartition(itr, reduceFunc).iterator
      }
      UpdateVertexRDD(mapResult, reduceFunc, vertices.partitioner.get)
    } else {
      val mapResult = sourceVertices.partitionsRDD.zipPartitions(edges.partitionsRDD, dstVertices, activeVertexSet.partitionsRDD, true) {(sVPartIter, ePartIter, dstVPartIter, activeVPartIter) =>
        val (_, edgePartition) = ePartIter.next()
        val sourceVertexPartition = sVPartIter.next()
        //val dstIds = edgePartition.dstIds
        val dstIdsIterator = edgePartition.dstIds.iterator
        val dstVPart = dstVPartIter.next()
        val activeVPart = activeVPartIter.next()
        var pos = 0
        var vid: VertexId = 0
        var degree = 0
        val itr = sourceVertexPartition.indexIterator.flatMap(e => {
          pos += degree
          vid = e._1
          degree = e._2
          val attr = e._3
          if (activeVPart.index.getPos(vid) != -1)
            (0 until degree).iterator.flatMap(i => {
              val dstVid = dstIdsIterator.next()
              mapFunc(vid, attr, dstVid, dstVPart(dstVid))
            })
          else {
            for (i <- 1 to degree)
              dstIdsIterator.next()
            Iterator.empty
          }
        })
        UpdateVertexPartition(itr, reduceFunc).iterator
      }
      UpdateVertexRDD(mapResult, reduceFunc, vertices.partitioner.get)
    }
  }

  override def outerJoinVertices[U: ClassTag, VD2: ClassTag]
  (other: RDD[(VertexId, U)])
  (mapFunc: (VertexId, VD, Option[U]) => VD2): Graph[VD2, ED] = {
    val newVertices: VertexRDD[VD2] = vertices.leftJoin(other)(mapFunc).cache()
    val newSourceVertices: SourceVertexRDD[VD2] = sourceVertices.leftJoin(other)(mapFunc).cache()
    GraphImpl(newSourceVertices, newVertices, edges)
  }

} // end of class GraphImpl


object GraphImpl {

  def fromEdgePartitions[VD: ClassTag, ED: ClassTag](
      edgePartitions: RDD[(PartitionID, EdgePartition[ED])],
      defaultVertexAttr: VD,
      edgeStorageLevel: StorageLevel,
      vertexStorageLevel: StorageLevel,
      sourceVertexStorageLevel: StorageLevel): GraphImpl[VD, ED] = {
    fromEdgeRDD(
      EdgeRDD.fromEdgePartitions(edgePartitions),
      defaultVertexAttr,
      edgeStorageLevel,
      vertexStorageLevel,
      sourceVertexStorageLevel)
  }

  def apply[VD: ClassTag, ED: ClassTag](
      sourceVertices: SourceVertexRDD[VD],
      vertices: VertexRDD[VD],
      edges: EdgeRDD[ED]): GraphImpl[VD, ED] = {
    new GraphImpl(sourceVertices, vertices, edges)
  }

  private def fromEdgeRDD[VD: ClassTag, ED: ClassTag](
      edges: EdgeRDD[ED],
      defaultVertexAttr: VD,
      edgeStorageLevel: StorageLevel,
      vertexStorageLevel: StorageLevel,
      sourceVertexStorageLevel: StorageLevel): GraphImpl[VD, ED] = {
    // withTargetStorageLevel method creates a new rdd adding a storage level
    val edgesCached = edges.withTargetStorageLevel(edgeStorageLevel).cache()
    val vertices = VertexRDD.fromEdges(edgesCached, edgesCached.partitions.size, defaultVertexAttr)
      .withTargetStorageLevel(vertexStorageLevel).cache()
    val sourceVertices = SourceVertexRDD.fromEdges(edgesCached, defaultVertexAttr).withTargetStorageLevel(sourceVertexStorageLevel).cache()
    GraphImpl(sourceVertices, vertices, edgesCached)
  }
}
