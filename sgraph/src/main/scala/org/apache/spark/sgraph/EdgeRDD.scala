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

import scala.reflect.{classTag, ClassTag}

import org.apache.spark.{OneToOneDependency, Partition, Partitioner, TaskContext}
import org.apache.spark.sgraph.impl._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * `EdgeRDD[ED]` extends `RDD[Edge[ED]]` by storing the edges in columnar format on each partition
 * for performance.
 */
class EdgeRDD[@specialized ED: ClassTag](
    val partitionsRDD: RDD[(PartitionID, EdgePartition[ED])],
    val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  extends RDD[Edge[ED]](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  partitionsRDD.setName("EdgeRDD")

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  /**
   * If `partitionsRDD` already has a partitioner, use it. Otherwise assume that the
   * [[PartitionID]]s in `partitionsRDD` correspond to the actual partitions and create a new
   * partitioner that allows co-partitioning with `partitionsRDD`.
   */
  override val partitioner =
    partitionsRDD.partitioner.orElse(Some(Partitioner.defaultPartitioner(partitionsRDD)))

  override def compute(part: Partition, context: TaskContext): Iterator[Edge[ED]] = {
//    if (firstParent[(PartitionID, EdgePartition[ED])].iterator(part, context).hasNext)
      firstParent[(PartitionID, EdgePartition[ED])].iterator(part, context).next._2.iterator
  }

  override def collect(): Array[Edge[ED]] = this.map(_.copy()).collect()

  override def persist(newLevel: StorageLevel): EdgeRDD[ED] = {
    partitionsRDD.persist(newLevel)
    this
  }

  /** Persist this RDD with the default storage level (`targetStorageLevel`). */
  override def cache(): EdgeRDD[ED] = {
    partitionsRDD.persist(targetStorageLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): EdgeRDD[ED] = {
    partitionsRDD.unpersist(blocking)
    this
  }

  private[sgraph] def mapEdgePartitions[ED2: ClassTag](f: (PartitionID, EdgePartition[ED]) => EdgePartition[ED2])
    : EdgeRDD[ED2] = {
    new EdgeRDD[ED2](partitionsRDD.mapPartitions({ iter =>
      val (pid, ep) = iter.next()
      Iterator(Tuple2(pid, f(pid, ep)))
    }, preservesPartitioning = true))
  }

  /**
   * Map the values in an edge partitioning preserving the structure but changing the values.
   *
   * @tparam ED2 the new edge value type
   * @param f the function from an edge to a new edge value
   * @return a new EdgeRDD containing the new edge values
   */
  def mapValues[ED2: ClassTag](f: Edge[ED] => ED2): EdgeRDD[ED2] =
    mapEdgePartitions((pid, part) => part.map(f))

  /**
   * Reverse all the edges in this RDD.
   *
   * @return a new EdgeRDD containing all the edges reversed
   */
  def reverse: EdgeRDD[ED] = mapEdgePartitions((pid, part) => part.reverse)


  def shipDstVertices(activeVertexSet: UpdateVertexRDD[Null]) = {
    if (activeVertexSet == null) {
      partitionsRDD.mapPartitions(ePartIter => {
        val (_, edgePartition) = ePartIter.next()
        val itr = edgePartition.dstIds.iterator.map(vid => (vid, vid))
        Iterator(VertexPartition(itr))
      })
    } else {
      partitionsRDD.zipPartitions(activeVertexSet.partitionsRDD) { (ePartIter, activeVPartIter) =>
        val (_, edgePartition) = ePartIter.next()
        val dstIds = edgePartition.dstIds
        val activeVPart = activeVPartIter.next()
        var pos = 0
        var degree = 0
        val itr = edgePartition.indexIterator.flatMap(e => {
          pos += degree
          val vid = e._1
          degree = e._2
          if (activeVPart.index.getPos(vid) == -1)
            Iterator.empty
          else
            (0 until degree).iterator.map(i => (dstIds(pos + i), dstIds(pos + i)))
        })
        Iterator(VertexPartition(itr))
      }
    }
  }

  private[sgraph] def withTargetStorageLevel(
      targetStorageLevel: StorageLevel): EdgeRDD[ED] = {
    new EdgeRDD(this.partitionsRDD, targetStorageLevel)
  }
}

object EdgeRDD {
  /**
   * Creates an EdgeRDD from already-constructed edge partitions.
   *
   * @tparam ED the edge attribute type
   */
  def fromEdgePartitions[ED: ClassTag](
      edgePartitions: RDD[(Int, EdgePartition[ED])]): EdgeRDD[ED] = {
    new EdgeRDD(edgePartitions)
  }
}
