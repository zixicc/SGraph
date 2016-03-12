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

import org.apache.spark.sgraph._
import org.apache.spark.sgraph.util.collection.PrimitiveVector
import org.apache.spark.sgraph.util.collection.PrimitiveKeyOpenHashMap

/**
 * A collection of edges stored in 3 large columnar arrays (src, dst, attribute). The arrays are
 * clustered by src.
 *
 * @param srcIds the source vertex id of each edge
 * @param dstIds the destination vertex id of each edge
 * @param data the attribute associated with each edge
 * @param index a clustered index on source vertex id
 * @param sourceIndex a clustered index on source vertex id by sequence of edges
 * @tparam ED the edge attribute type.
 */
private[sgraph]
class EdgePartition[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED: ClassTag](
    //val srcIds: Array[VertexId],
    val dstIds: Array[VertexId],
    val data: Array[ED],
    val index: PrimitiveKeyOpenHashMap[VertexId, Int],
    val sourceIndex: PrimitiveVector[(VertexId, Int)]) extends Serializable {

  def indexIterator = new Iterator[(VertexId, Int)]{
    private[this] var pos = 0
    override def hasNext: Boolean = pos < sourceIndex.size
    override def next():(VertexId, Int) = {
      val tuple = sourceIndex.array(pos)
      val vid = tuple._1
      if (pos == sourceIndex.size - 1) {
        pos += 1
        val degree = dstIds.length - tuple._2
        (vid, degree)
      } else {
        pos += 1
        val degree = sourceIndex.array(pos)._2 - tuple._2
        (vid, degree)
      }
    }
  }
  /**
   * Reverse all the edges in this partition.
   *
   * @return a new edge partition with all edges reversed.
   */
  def reverse: EdgePartition[ED] = {
    val builder = new EdgePartitionBuilder[ED](size)
    for (e <- iterator) {
      builder.add(e.dstId, e.srcId, e.attr)
    }
    builder.toEdgePartition
  }

  /**
   * Construct a new edge partition by applying the function f to all
   * edges in this partition.
   *
   * @param f a function from an edge to a new attribute
   * @tparam ED2 the type of the new attribute
   * @return a new edge partition with the result of the function `f`
   *         applied to each edge
   */
  def map[ED2: ClassTag](f: Edge[ED] => ED2): EdgePartition[ED2] = {
    val newData = new Array[ED2](data.size)
    val edge = new Edge[ED]()
    val size = data.size
    var i = 0
    while (i < size) {
      //edge.srcId  = srcIds(i)
      edge.dstId  = dstIds(i)
      edge.attr = data(i)
      newData(i) = f(edge)
      i += 1
    }
    new EdgePartition(dstIds, newData, index, sourceIndex)
  }

  /**
   * Construct a new edge partition by using the edge attributes
   * contained in the iterator.
   *
   * @note The input iterator should return edge attributes in the
   * order of the edges returned by `EdgePartition.iterator` and
   * should return attributes equal to the number of edges.
   *
   * @tparam ED2 the type of the new attribute
   * @return a new edge partition with the result of the function `f`
   *         applied to each edge
   */
  def map[ED2: ClassTag](iter: Iterator[ED2]): EdgePartition[ED2] = {
    val newData = new Array[ED2](data.size)
    var i = 0
    while (iter.hasNext) {
      newData(i) = iter.next()
      i += 1
    }
    assert(newData.size == i)
    new EdgePartition(dstIds, newData, index, sourceIndex)
  }

  /**
   * Apply the function f to all edges in this partition.
   *
   * @param f an external state mutating user defined function.
   */
  def foreach(f: Edge[ED] => Unit) {
    iterator.foreach(f)
  }

  /**
   * The number of edges in this partition
   *
   * @return size of the partition
   */
  def size: Int = dstIds.size

  def iterator = new Iterator[Edge[ED]] {
    private[this] val edge = new Edge[ED]
    private[this] var pos = 0
    private[this] var sourcePos = 0

    override def hasNext: Boolean = sourcePos < sourceIndex.size && pos < EdgePartition.this.size

    override def next(): Edge[ED] = {
      if (sourcePos < sourceIndex.size - 1 && pos >= sourceIndex(sourcePos + 1)._2)
        sourcePos += 1
      edge.srcId = sourceIndex(sourcePos)._1
      edge.dstId = dstIds(pos)
      edge.attr = data(pos)
      pos += 1
      edge
    }
  }
}
