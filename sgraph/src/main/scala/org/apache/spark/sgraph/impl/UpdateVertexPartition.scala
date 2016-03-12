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

import org.apache.spark.util.collection.BitSet

import org.apache.spark.sgraph._

private[sgraph] object UpdateVertexPartition {
  /** Construct a `UpdateVertexPartition` from the given vertices. */
  def apply[VD: ClassTag](iter: Iterator[(VertexId, VD)])
  : UpdateVertexPartition[VD] = {
    val (index, values, mask) = VertexPartitionBase.initFrom(iter)
    new UpdateVertexPartition(index, values, mask)
  }

  /** Construct a `UpdateVertexPartition` from the given vertices and mergeFun, which merges the same key */
  def apply[VD: ClassTag](iter: Iterator[(VertexId, VD)], mergeFunc: (VD, VD) => VD)
  : UpdateVertexPartition[VD] = {
    val (index, values, mask) = VertexPartitionBase.initFrom(iter, mergeFunc)
    new UpdateVertexPartition(index, values, mask)
  }

  import scala.language.implicitConversions

  /**
   * Implicit conversion to allow invoking `VertexPartitionBase` operations directly on a
   * `UpdateVertexPartition`.
   */
  implicit def partitionToOps[VD: ClassTag](partition: UpdateVertexPartition[VD]) =
    new UpdateVertexPartitionOps(partition)

  /**
   * Implicit evidence that `UpdateVertexPartition` is a member of the `VertexPartitionBaseOpsConstructor`
   * typeclass. This enables invoking `VertexPartitionBase` operations on a `VertexPartition` via an
   * evidence parameter, as in [[VertexPartitionBaseOps]].
   */
  implicit object UpdateVertexPartitionOpsConstructor
      extends VertexPartitionBaseOpsConstructor[UpdateVertexPartition] {
    def toOps[VD: ClassTag](partition: UpdateVertexPartition[VD])
    : VertexPartitionBaseOps[VD, UpdateVertexPartition] = partitionToOps(partition)
  }
}

/** A map from vertex id to vertex attribute. */
private[sgraph] class UpdateVertexPartition[VD: ClassTag](
    val index: VertexIdToIndexMap,
    val values: Array[VD],
    val mask: BitSet)
  extends VertexPartitionBase[VD]

private[sgraph] class UpdateVertexPartitionOps[VD: ClassTag](self: UpdateVertexPartition[VD])
  extends VertexPartitionBaseOps[VD, UpdateVertexPartition](self) {

  def withIndex(index: VertexIdToIndexMap): UpdateVertexPartition[VD] = {
    new UpdateVertexPartition(index, self.values, self.mask)
  }

  def withValues[VD2: ClassTag](values: Array[VD2]): UpdateVertexPartition[VD2] = {
    new UpdateVertexPartition(self.index, values, self.mask)
  }

  def withMask(mask: BitSet): UpdateVertexPartition[VD] = {
    new UpdateVertexPartition(self.index, self.values, mask)
  }
}