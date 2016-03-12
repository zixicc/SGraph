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

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sgraph.impl.{MsgRDDFunctions, UpdateVertexPartition}
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

class UpdateVertexRDD[@specialized VD: ClassTag](
    val partitionsRDD: RDD[UpdateVertexPartition[VD]],
    val targetStorageLevel: StorageLevel = StorageLevel.DISK_ONLY)
      extends RDD[(VertexId, VD)](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  partitionsRDD.setName("UpdateVertexRDD")

  override val partitioner = partitionsRDD.partitioner

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  override protected def getPreferredLocations(s: Partition): Seq[String] =
    partitionsRDD.preferredLocations(s)

  override def persist(newLevel: StorageLevel): UpdateVertexRDD[VD] = {
    partitionsRDD.persist(newLevel)
    this
  }

  /** Persist this RDD with the default storage level (`targetStorageLevel`). */
  override def cache(): UpdateVertexRDD[VD] = {
    partitionsRDD.persist(targetStorageLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): UpdateVertexRDD[VD] = {
    partitionsRDD.unpersist(blocking)
    this
  }

  /**
   * Provides the `RDD[(VertexId, VD)]` equivalent output.
   */
  override def compute(part: Partition, context: TaskContext): Iterator[(VertexId, VD)] = {
    firstParent[UpdateVertexPartition[VD]].iterator(part, context).next.iterator
  }
}

object UpdateVertexRDD {

  def apply[VD: ClassTag]
  (rdd: RDD[(VertexId, VD)], partitioner: Partitioner)
  : UpdateVertexRDD[VD] = {
    val partitioned: RDD[(VertexId, VD)] = MsgRDDFunctions.partitionForShuffling(rdd, partitioner)
    val updateVertexPartitions = partitioned.mapPartitions(
      iter => Iterator(UpdateVertexPartition(iter)),
      preservesPartitioning = true)
    new UpdateVertexRDD(updateVertexPartitions)
  }

  def apply[VD: ClassTag]
  (rdd: RDD[(VertexId, VD)], mergeFunc:(VD, VD) => VD, partitioner: Partitioner)
  : UpdateVertexRDD[VD] = {
    val partitioned: RDD[(VertexId, VD)] = MsgRDDFunctions.partitionForShuffling(rdd, partitioner)
    val updateVertexPartitions = partitioned.mapPartitions(
      iter => Iterator(UpdateVertexPartition(iter, mergeFunc)),
      preservesPartitioning = true)
    new UpdateVertexRDD(updateVertexPartitions)
  }

  def apply[VD: ClassTag] (rdd: VertexRDD[VD]) = {
    val newPartitionsRDD = rdd.partitionsRDD.mapPartitions(iter => {
      val vertexPartition = iter.next()
      Iterator(new UpdateVertexPartition(vertexPartition.index, vertexPartition.values, vertexPartition.mask))
    })
    new UpdateVertexRDD(newPartitionsRDD)
  }
}