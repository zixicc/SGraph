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
import org.apache.spark.sgraph._
import org.apache.spark.sgraph.util.collection.{PrimitiveVector, PrimitiveKeyOpenHashMap}
import org.apache.spark.util.collection.BitSet

import scala.reflect.ClassTag


private[sgraph]
class SourceVertexPartition[VD: ClassTag](
    val index: VertexIdToIndexMap,
    val values: Array[VD],
    val sourceIndex: PrimitiveVector[(VertexId, Int)],
    val mask: BitSet,
    val edgeSize: Int) extends VertexPartitionBase[VD] with Logging {
  // get a triple iterator, the first element is source vertex id, the second element is out degree of the source vertex id,
  // the third element is vertex attribute
  def indexIterator = new Iterator[(VertexId, Int, VD)]{
    private[this] var pos = 0
    override def hasNext: Boolean = pos < sourceIndex.size
    override def next():(VertexId, Int, VD) = {
      val tuple = sourceIndex.array(pos)
      val vid = tuple._1
      val value = values(index.getPos(vid))
      if (pos == sourceIndex.size - 1) {
        pos += 1
        val degree = edgeSize - tuple._2
        (vid, degree, value)
      } else {
        pos += 1
        val degree = sourceIndex.array(pos)._2 - tuple._2
        (vid, degree ,value)
      }
    }
  }
}

private[sgraph]
object SourceVertexPartition {
  def apply[VD: ClassTag](map: PrimitiveKeyOpenHashMap[VertexId, Int], sourceIndex: PrimitiveVector[(VertexId, Int)], size: Int, defaultVal: VD): SourceVertexPartition[VD] = {
    val values = Array.fill(map._values.length)(defaultVal)
    new SourceVertexPartition(map.keySet, values, sourceIndex, map.keySet.getBitSet, size)
  }

  import scala.language.implicitConversions

  /**
   * Implicit conversion to allow invoking `VertexPartitionBase` operations directly on a
   * `VertexPartition`.
   */
  implicit def partitionToOps[VD: ClassTag](partition: SourceVertexPartition[VD]) =
    new SourceVertexPartitionOps(partition)

  implicit object SourceVertexPartitionOpsConstructor
    extends VertexPartitionBaseOpsConstructor[SourceVertexPartition] {
    def toOps[VD: ClassTag](partition: SourceVertexPartition[VD])
    : VertexPartitionBaseOps[VD, SourceVertexPartition] = partitionToOps(partition)
  }
}

private[sgraph] class SourceVertexPartitionOps[VD: ClassTag](self: SourceVertexPartition[VD])
  extends VertexPartitionBaseOps[VD, SourceVertexPartition](self) {

  def withIndex(index: VertexIdToIndexMap): SourceVertexPartition[VD] = {
    new SourceVertexPartition(index, self.values, self.sourceIndex, self.mask, self.edgeSize)
  }

  def withValues[VD2: ClassTag](values: Array[VD2]): SourceVertexPartition[VD2] = {
    new SourceVertexPartition(self.index, values, self.sourceIndex, self.mask, self.edgeSize)
  }

  def withMask(mask: BitSet): SourceVertexPartition[VD] = {
    new SourceVertexPartition(self.index, self.values, self.sourceIndex, mask, self.edgeSize)
  }
}