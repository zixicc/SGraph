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

import org.apache.spark.Logging

import scala.reflect.ClassTag

import org.apache.spark.util.collection.{PrimitiveVector, BitSet}

import org.apache.spark.sgraph._

object VertexPartition {
  /** Construct a `VertexPartition` from the given vertices. */
  def apply[VD: ClassTag](iter: Iterator[(VertexId, VD)], routingTable: RoutingTablePartition, defaultVal: VD)
  : VertexPartition[VD] = {
    val iterator = iter ++ routingTable.iterator.map(vid => (vid, defaultVal))
    val (index, values, mask) = VertexPartitionBase.initFrom(iterator)
    new VertexPartition(index, values, mask, routingTable)
  }

  /** Construct a `VertexPartition` from the given vertices with empty routing table */
  def apply[VD: ClassTag](iter: Iterator[(VertexId, VD)])
  : VertexPartition[VD] = {
    apply(iter, RoutingTablePartition.empty, null.asInstanceOf[VD])
  }

  /** Construct a `VertexPartition` from the given vertices and mergeFun, which merges the same key */
  def apply[VD: ClassTag](iter: Iterator[(VertexId, VD)], mergeFunc: (VD, VD) => VD)
  : VertexPartition[VD] = {
    val (index, values, mask) = VertexPartitionBase.initFrom(iter, mergeFunc)
    new VertexPartition(index, values, mask, RoutingTablePartition.empty)
  }

  import scala.language.implicitConversions

  /**
   * Implicit conversion to allow invoking `VertexPartitionBase` operations directly on a
   * `VertexPartition`.
   */
  implicit def partitionToOps[VD: ClassTag](partition: VertexPartition[VD]) =
    new VertexPartitionOps(partition)

  /**
   * Implicit evidence that `VertexPartition` is a member of the `VertexPartitionBaseOpsConstructor`
   * typeclass. This enables invoking `VertexPartitionBase` operations on a `VertexPartition` via an
   * evidence parameter, as in [[VertexPartitionBaseOps]].
   */
  implicit object VertexPartitionOpsConstructor
    extends VertexPartitionBaseOpsConstructor[VertexPartition] {
    def toOps[VD: ClassTag](partition: VertexPartition[VD])
    : VertexPartitionBaseOps[VD, VertexPartition] = partitionToOps(partition)
  }
}

/** A map from vertex id to vertex attribute. */
private[sgraph] class VertexPartition[VD: ClassTag](
    val index: VertexIdToIndexMap,
    val values: Array[VD],
    val mask: BitSet,
    val routingTable: RoutingTablePartition)
  extends VertexPartitionBase[VD]{

  /**
   * ship destination vertex set of all edge partitions by partition id if necessary
   * @return vertex set and attribute of corresponding vertex
   */
  def shipDestinationVertices(): Iterator[(PartitionID, VertexAttributeBlock[VD])] = {
    Iterator.tabulate(routingTable.numEdgePartitions) {pid =>
      val initialSize = 64
      val vids = new PrimitiveVector[VertexId](initialSize)
      val attrs = new PrimitiveVector[VD](initialSize)
      routingTable.foreachWithinEdgePartition(pid, false, true) {vid =>
        if (isDefined(vid)) {
          vids += vid
          attrs += this(vid)
        }
      }
      (pid, new VertexAttributeBlock(vids.trim().array, attrs.trim().array))
    }
  }

  /**
   * ship destination vertex set that resides in the previous update vertex partition
   * @return vertex set and attribute of corresponding vertex
   */
  def shipDestinationVertices[A: ClassTag](activeVPart: UpdateVertexPartition[Null]): Iterator[(PartitionID, VertexAttributeBlock[VD])] = {
    Iterator.tabulate(routingTable.numEdgePartitions) {pid =>
      val initialSize = 64
      val vids = new PrimitiveVector[VertexId](initialSize)
      val attrs = new PrimitiveVector[VD](initialSize)
      routingTable.foreachWithinEdgePartition(pid, false, true) {vid =>
        if (isDefined(vid)) {
          vids += vid
          attrs += this(vid)
        }
      }
      (pid, new VertexAttributeBlock(vids.trim().array, attrs.trim().array))
    }
  }
}

/**
 * Represents a block of vertices and corresponding attribute of each vertex
 * @param vids
 * @param attrs
 * @tparam VD
 */
private[sgraph] class VertexAttributeBlock[VD: ClassTag](val vids: Array[VertexId], val attrs: Array[VD])
  extends Serializable {
  def iterator: Iterator[(VertexId, VD)] = (0 until vids.size).iterator.map(i => (vids(i), attrs(i)))
}

private[sgraph] class VertexPartitionOps[VD: ClassTag](self: VertexPartition[VD])
  extends VertexPartitionBaseOps[VD, VertexPartition](self) {

  def withIndex(index: VertexIdToIndexMap): VertexPartition[VD] = {
    new VertexPartition(index, self.values, self.mask, self.routingTable)
  }

  def withValues[VD2: ClassTag](values: Array[VD2]): VertexPartition[VD2] = {
    new VertexPartition(self.index, values, self.mask, self.routingTable)
  }

  def withMask(mask: BitSet): VertexPartition[VD] = {
    new VertexPartition(self.index, self.values, mask, self.routingTable)
  }
}