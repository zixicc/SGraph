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

/**
 * Represents the way edges are assigned to edge partitions based on their source and destination
 * vertex IDs.
 */
trait PartitionStrategy extends Serializable {
  /** Returns the partition number for a given edge. */
  def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID
}

/**
 * Collection of built-in [[PartitionStrategy]] implementations.
 */
object PartitionStrategy {
  /**
   * Assigns edges to partitions using only the source vertex ID, colocating edges with the same
   * source.
   */
  case object EdgePartition1D extends PartitionStrategy {
    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
      val mixingPrime: VertexId = 1125899906842597L
      //(math.abs(src) * mixingPrime).toInt % numParts
      // fix bug, avoid overflow
      (math.abs(src * mixingPrime) % numParts).toInt
    }
  }
}
