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
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, Logging, SparkContext}
import org.apache.spark.sgraph.impl.{MessageToPartition, EdgePartition, EdgePartitionBuilder, GraphImpl}
import org.apache.spark.sgraph.impl.MsgRDDFunctions._

/**
 * Provides utilities for loading [[Graph]]s from files.
 */
object GraphLoader extends Logging {

  /**
   * Loads a graph from an edge list formatted file where each line contains two integers: a source
   * id and a target id. Skips lines that begin with `#`.
   *
   * If desired the edges can be automatically oriented in the positive
   * direction (source Id < target Id) by setting `canonicalOrientation` to
   * true.
   *
   * @example Loads a file in the following format:
   * {{{
   * # Comment Line
   * # Source Id <\t> Target Id
   * 1   -5
   * 1    2
   * 2    7
   * 1    8
   * }}}
   *
   * @param sc SparkContext
   * @param path the path to the file (e.g., /home/data/file or hdfs://file)
   * @param canonicalOrientation whether to orient edges in the positive
   *        direction
   * @param edgeStorageLevel the desired storage level for the edge partitions
   * @param vertexStorageLevel the desired storage level for the vertex partitions
   */
  def edgeListFile(
                    sc: SparkContext,
                    path: String,
                    canonicalOrientation: Boolean = false,
                    edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
                    vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
                    sourceVertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  : Graph[Int, Int] =
  {
    val startTime = System.currentTimeMillis

    // Parse the edge data table directly into edge partitions
    val lines = sc.textFile(path, 1)
    val edges = lines.mapPartitionsWithIndex { (pid, iter) =>
      val builder = new EdgePartitionBuilder[Int]
      iter.foreach { line =>
        if (!line.isEmpty && line(0) != '#') {
          val lineArray = line.split("\\s+")
          if (lineArray.length < 2) {
            logWarning("Invalid line: " + line)
          }
          val srcId = lineArray(0).toLong
          val dstId = lineArray(1).toLong
          if (canonicalOrientation && srcId > dstId) {
            builder.add(dstId, srcId, 1)
          } else {
            builder.add(srcId, dstId, 1)
          }
        }
      }
      Iterator((pid, builder.toEdgePartition))
    }

    val newEdges = partitionBy(edges, PartitionStrategy.EdgePartition1D, path, edgeStorageLevel)
    newEdges.count()
    logInfo("It took %d ms to load the edges".format(System.currentTimeMillis - startTime))

    GraphImpl.fromEdgePartitions(newEdges, 1, edgeStorageLevel,
      vertexStorageLevel, sourceVertexStorageLevel)
  } // end of edgeListFile

  /**
   *  partition edges using `PartitionStrategy`
   */
  private[this] def partitionBy(
      edges: RDD[(PartitionID, EdgePartition[Int])],
      partitionStrategy: PartitionStrategy,
      path: String,
      edgeStorageLevel: StorageLevel): RDD[(PartitionID, EdgePartition[Int])] = {
    val numPartitions = edges.partitions.size
    val newEdges = EdgeRDD.fromEdgePartitions(edges).map{ e =>
      val pid: PartitionID = partitionStrategy.getPartition(e.srcId, e.dstId, numPartitions)
      new MessageToPartition(pid, (e.srcId, e.dstId, e.attr))
    }
    .partitionBy(new HashPartitioner(numPartitions))
    .mapPartitionsWithIndex((pid, iter) => {
      val builder = new EdgePartitionBuilder[Int]()
      iter.foreach(message => {
        val data = message.data
        builder.add(data._1, data._2, data._3)
      })
      val edgePartition = builder.toEdgePartition
      Iterator((pid, edgePartition))
    }, true)
    .persist(edgeStorageLevel)
    newEdges
  }
}
