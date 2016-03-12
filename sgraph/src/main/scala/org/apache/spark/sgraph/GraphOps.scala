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

import scala.reflect.ClassTag

import org.apache.spark.SparkContext._
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD

/**
 * Contains additional functionality for [[Graph]]. All operations are expressed in terms of the
 * efficient sgraph API. This class is implicitly constructed for each Graph object.
 *
 * @tparam VD the vertex attribute type
 * @tparam ED the edge attribute type
 */
class GraphOps[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]) extends Serializable {

  /** The number of edges in the graph. */
  lazy val numEdges: Long = graph.edges.count()

  /** The number of vertices in the graph. */
  lazy val numVertices: Long = graph.vertices.count()
} // end of GraphOps
