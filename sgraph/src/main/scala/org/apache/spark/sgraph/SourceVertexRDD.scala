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

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.sgraph.util.collection.{PrimitiveVector, PrimitiveKeyOpenHashMap}
import org.apache.spark.{TaskContext, Partition, OneToOneDependency}
import org.apache.spark.sgraph.impl.SourceVertexPartition
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

/**
 * `SourceVertexRDD[VD]` stores source vertex of edges and the number index of each vertex.
 * The index comes from index of `EdgePartition[ED]`
 */
class SourceVertexRDD[@specialized VD: ClassTag](
    val partitionsRDD: RDD[SourceVertexPartition[VD]],
    val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  extends RDD[(VertexId, VD)](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  require(partitionsRDD.partitioner.isDefined)

  partitionsRDD.setName("SourceVertexRDD")

  override val partitioner = partitionsRDD.partitioner

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  override protected def getPreferredLocations(s: Partition): Seq[String] =
    partitionsRDD.preferredLocations(s)
  override def persist(newLevel: StorageLevel): SourceVertexRDD[VD] = {
    partitionsRDD.persist(newLevel)
    this
  }

  /** Persist this RDD with the default storage level (`targetStorageLevel`). */
  override def cache(): SourceVertexRDD[VD] = {
    partitionsRDD.persist(targetStorageLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): SourceVertexRDD[VD] = {
    partitionsRDD.unpersist(blocking)
    this
  }

  /** The number of vertices in the RDD. */
  override def count(): Long = {
    partitionsRDD.map(_.size).reduce(_ + _)
  }

  /**
   * Provides the `RDD[(VertexId, VD)]` equivalent output.
   */
  override def compute(part: Partition, context: TaskContext): Iterator[(VertexId, VD)] = {
    firstParent[SourceVertexPartition[VD]].iterator(part, context).next.iterator
  }

  private[sgraph] def withTargetStorageLevel(
      targetStorageLevel: StorageLevel): SourceVertexRDD[VD] = {
    new SourceVertexRDD(this.partitionsRDD, targetStorageLevel)
  }

  private[sgraph] def mapVertexParitions[VD2: ClassTag](f: SourceVertexPartition[VD] => SourceVertexPartition[VD2]): SourceVertexRDD[VD2] = {
    val newPartitionsRDD = partitionsRDD.mapPartitions(_.map(f), preservesPartitioning = true)
    new SourceVertexRDD(newPartitionsRDD, targetStorageLevel)
  }

  /**
   * Left joins this RDD with another VertexRDD with the same index. This function will fail if both
   * VertexRDDs do not share the same index. The resulting vertex set contains an entry for each
   * vertex in `this`. If `other` is missing any vertex in this VertexRDD, `f` is passed `None`.
   *
   * @tparam VD2 the attribute type of the other VertexRDD
   * @tparam VD3 the attribute type of the resulting VertexRDD
   *
   * @param other the other VertexRDD with which to join.
   * @param f the function mapping a vertex id and its attributes in this and the other vertex set
   * to a new vertex attribute.
   * @return a VertexRDD containing the results of `f`
   */
  def leftZipJoin[VD2: ClassTag, VD3: ClassTag]
  (other: UpdateVertexRDD[VD2])(f: (VertexId, VD, Option[VD2]) => VD3): SourceVertexRDD[VD3] = {
    val newPartitionsRDD = partitionsRDD.zipPartitions(
      other.partitionsRDD, preservesPartitioning = true
    ) { (thisIter, otherIter) =>
      val thisPart = thisIter.next()
      val otherPart = otherIter.next()
      Iterator(thisPart.leftJoin(otherPart)(f))
    }
    new SourceVertexRDD(newPartitionsRDD, targetStorageLevel)
  }

  /**
   * Left joins this VertexRDD with an RDD containing vertex attribute pairs. If the other RDD is
   * backed by a VertexRDD with the same index then the efficient [[leftZipJoin]] implementation is
   * used. The resulting VertexRDD contains an entry for each vertex in `this`. If `other` is
   * missing any vertex in this VertexRDD, `f` is passed `None`. If there are duplicates, the vertex
   * is picked arbitrarily.
   *
   * @tparam VD2 the attribute type of the other VertexRDD
   * @tparam VD3 the attribute type of the resulting VertexRDD
   *
   * @param other the other VertexRDD with which to join
   * @param f the function mapping a vertex id and its attributes in this and the other vertex set
   * to a new vertex attribute.
   * @return a VertexRDD containing all the vertices in this VertexRDD with the attributes emitted
   * by `f`.
   */
  def leftJoin[VD2: ClassTag, VD3: ClassTag]
  (other: RDD[(VertexId, VD2)])
  (f: (VertexId, VD, Option[VD2]) => VD3)
  : SourceVertexRDD[VD3] = {
    // Test if the other vertex is a VertexRDD to choose the optimal join strategy.
    // If the other set is a VertexRDD then we use the much more efficient leftZipJoin
    other match {
      case other: UpdateVertexRDD[_] =>
        leftZipJoin(other)(f)
      case _ =>
        new SourceVertexRDD[VD3](
          partitionsRDD.zipPartitions(
            other.partitionBy(this.partitioner.get), preservesPartitioning = true)
          { (part, msgs) =>
            val sourceVertexPartition: SourceVertexPartition[VD] = part.next()
            Iterator(sourceVertexPartition.leftJoin(msgs)(f))
          },
          targetStorageLevel
        )
    }
  }

  /**
   * Efficiently inner joins this VertexRDD with another VertexRDD sharing the same index. See
   * [[innerJoin]] for the behavior of the join.
   */
  def innerZipJoin[U: ClassTag, VD2: ClassTag](other: UpdateVertexRDD[U])
                                              (f: (VertexId, VD, U) => VD2): SourceVertexRDD[VD2] = {
    val newPartitionsRDD = partitionsRDD.zipPartitions(
      other.partitionsRDD, preservesPartitioning = true
    ) { (thisIter, otherIter) =>
      val thisPart = thisIter.next()
      val otherPart = otherIter.next()
      Iterator(thisPart.innerJoin(otherPart)(f))
    }
    new SourceVertexRDD(newPartitionsRDD, targetStorageLevel)
  }

  /**
   * Inner joins this VertexRDD with an RDD containing vertex attribute pairs. If the other RDD is
   * backed by a VertexRDD with the same index then the efficient [[innerZipJoin]] implementation is
   * used.
   *
   * @param other an RDD containing vertices to join. If there are multiple entries for the same
   * vertex, one is picked arbitrarily.
   * @param f the join function applied to corresponding values of `this` and `other`
   * @return a VertexRDD co-indexed with `this`, containing only vertices that appear in both `this`
   * and `other`, with values supplied by `f`
   */
  def innerJoin[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, U)])
                                           (f: (VertexId, VD, U) => VD2): SourceVertexRDD[VD2] = {
    // Test if the other vertex is a VertexRDD to choose the optimal join strategy.
    // If the other set is a VertexRDD then we use the much more efficient innerZipJoin
    other match {
      case other: UpdateVertexRDD[_] =>
        innerZipJoin(other)(f)
      case _ =>
        new SourceVertexRDD[VD2](
          partitionsRDD.zipPartitions(
            other.partitionBy(this.partitioner.get), preservesPartitioning = true)
          { (part, msgs) =>
            val sourceVertexPartition: SourceVertexPartition[VD] = part.next()
            Iterator(sourceVertexPartition.innerJoin(msgs)(f))
          },
          targetStorageLevel
        )
    }
  }

}

object SourceVertexRDD {
  def fromEdges[VD: ClassTag](
      edges: EdgeRDD[_], defaultVal: VD): SourceVertexRDD[VD] = {
    val sourceVertexPartitions = edges.partitionsRDD.mapPartitions({ iter =>
      val (_, edgePartition) = iter.next()
      val map: PrimitiveKeyOpenHashMap[VertexId, Int] = edgePartition.index
      val sourceIndex: PrimitiveVector[(VertexId, Int)] = edgePartition.sourceIndex
      Iterator(SourceVertexPartition(map, sourceIndex, edgePartition.size, defaultVal))
    }, preservesPartitioning = true)
    new SourceVertexRDD(sourceVertexPartitions)
  }
}