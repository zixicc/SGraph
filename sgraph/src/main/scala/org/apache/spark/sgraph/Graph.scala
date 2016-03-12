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

import org.apache.spark.sgraph.impl._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel


/**
 * The Graph abstractly represents a graph with arbitrary objects
 * associated with vertices and edges.  The graph provides basic
 * operations to access and manipulate the data associated with
 * vertices and edges as well as the underlying structure.  Like Spark
 * RDDs, the graph is a functional data-structure in which mutating
 * operations return new graphs.
 *
 * @note [[GraphOps]] contains additional convenience operations and graph algorithms.
 *
 * @tparam VD the vertex attribute type
 * @tparam ED the edge attribute type
 */
abstract class Graph[VD: ClassTag, ED: ClassTag] protected () extends Serializable {

  /**
   * An RDD containing the source vertices of edges and their attributes
   */
  val sourceVertices: SourceVertexRDD[VD]
  /**
   * An RDD containing the vertices and their associated attributes.
   *
   * @note vertex ids are unique.
   * @return an RDD containing the vertices in this graph
   */
  val vertices: VertexRDD[VD]

  /**
   * An RDD containing the edges and their associated attributes.  The entries in the RDD contain
   * just the source id and target id along with the edge data.
   *
   * @return an RDD containing the edges in this graph
   *
   * @see [[Edge]] for the edge type.
   * along with their vertex data.
   *
   */
  val edges: EdgeRDD[ED]

  /**
   * Caches the vertices and edges associated with this graph at the specified storage level.
   *
   * @param newLevel the level at which to cache the graph.
   *
   * @return A reference to this graph for convenience.
   */
  def persist(newLevel: StorageLevel = StorageLevel.DISK_ONLY): Graph[VD, ED]

  /**
   * Caches the vertices and edges associated with this graph. This is used to
   * pin a graph in memory enabling multiple queries to reuse the same
   * construction process.
   */
  def cache(): Graph[VD, ED]

  /**
   * Uncaches only the vertices of this graph, leaving the edges alone. This is useful in iterative
   * algorithms that modify the vertex attributes but reuse the edges. This method can be used to
   * uncache the vertex attributes of previous iterations once they are no longer needed, improving
   * GC performance.
   */
  def unpersistVertices(blocking: Boolean = true): Graph[VD, ED]

  /**
   * Transforms each vertex attribute in the graph using the map function.
   *
   * @note The new graph has the same structure.  As a consequence the underlying index structures
   * can be reused.
   *
   * @param map the function from a vertex object to a new vertex value
   *
   * @tparam VD2 the new vertex data type
   *
   * @example We might use this operation to change the vertex values
   * from one type to another to initialize an algorithm.
   * {{{
   * val rawGraph: Graph[(), ()] = Graph.textFile("hdfs://file")
   * val root = 42
   * var bfsGraph = rawGraph.mapVertices[Int]((vid, data) => if (vid == root) 0 else Math.MaxValue)
   * }}}
   *
   */
  def mapVertices[VD2: ClassTag](map: (VertexId, VD) => VD2): Graph[VD2, ED]

  /**
   * Transforms each edge attribute in the graph using the map function.  The map function is not
   * passed the vertex value for the vertices adjacent to the edge.  If vertex values are desired,
   * use `mapTriplets`.
   *
   * @note This graph is not changed and that the new graph has the
   * same structure.  As a consequence the underlying index structures
   * can be reused.
   *
   * @param map the function from an edge object to a new edge value.
   *
   * @tparam ED2 the new edge data type
   *
   * @example This function might be used to initialize edge
   * attributes.
   *
   */
  def mapEdges[ED2: ClassTag](map: Edge[ED] => ED2): Graph[VD, ED2] = {
    mapEdges((pid, iter) => iter.map(map))
  }

  /**
   * Transforms each edge attribute using the map function, passing it a whole partition at a
   * time. The map function is given an iterator over edges within a logical partition as well as
   * the partition's ID, and it should return a new iterator over the new values of each edge. The
   * new iterator's elements must correspond one-to-one with the old iterator's elements. If
   * adjacent vertex values are desired, use `mapTriplets`.
   *
   * @note This does not change the structure of the
   * graph or modify the values of this graph.  As a consequence
   * the underlying index structures can be reused.
   *
   * @param map a function that takes a partition id and an iterator
   * over all the edges in the partition, and must return an iterator over
   * the new values for each edge in the order of the input iterator
   *
   * @tparam ED2 the new edge data type
   *
   */
  def mapEdges[ED2: ClassTag](map: (PartitionID, Iterator[Edge[ED]]) => Iterator[ED2])
    : Graph[VD, ED2]

  /**
   * Reverses all edges in the graph.  If this graph contains an edge from a to b then the returned
   * graph contains an edge from b to a.
   */
  def reverse: Graph[VD, ED]

  def mapReduceUpdates[A: ClassTag]
  (mapFun: (VertexId, VD, Int) => (VertexId, A), reduceFun: (A, A) => A)
  : UpdateVertexRDD[A]

  def mapReduceUpdates2[A: ClassTag, VD2: ClassTag]
  (mapFunc: (VertexId, VD, VertexId, VD) => Iterator[(VertexId, VD2)],reduceFunc: (VD2, VD2) => VD2, activeVertexSet: UpdateVertexRDD[Null])
  : UpdateVertexRDD[VD2]
  /**
   * Joins the vertices with entries in the `table` RDD and merges the results using `mapFunc`.  The
   * input table should contain at most one entry for each vertex.  If no entry in `other` is
   * provided for a particular vertex in the graph, the map function receives `None`.
   *
   * @tparam U the type of entry in the table of updates
   * @tparam VD2 the new vertex value type
   *
   * @param other the table to join with the vertices in the graph.
   *              The table should contain at most one entry for each vertex.
   * @param mapFunc the function used to compute the new vertex values.
   *                The map function is invoked for all vertices, even those
   *                that do not have a corresponding entry in the table.
   *
   * @example This function is used to update the vertices with new values based on external data.
   *          For example we could add the out-degree to each vertex record:
   *
   * {{{
   * val rawGraph: Graph[_, _] = Graph.textFile("webgraph")
   * val outDeg: RDD[(VertexId, Int)] = rawGraph.outDegrees()
   * val graph = rawGraph.outerJoinVertices(outDeg) {
   *   (vid, data, optDeg) => optDeg.getOrElse(0)
   * }
   * }}}
   */
  def outerJoinVertices[U: ClassTag, VD2: ClassTag]
  (other: RDD[(VertexId, U)])
  (mapFunc: (VertexId, VD, Option[U]) => VD2): Graph[VD2, ED]

  /**
   * The associated [[GraphOps]] object.
   */
  // Save a copy of the GraphOps object so there is always one unique GraphOps object
  // for a given Graph object, and thus the lazy vals in GraphOps would work as intended.
  val ops = new GraphOps(this)
} // end of Graph


/**
 * The Graph object contains a collection of routines used to construct graphs from RDDs.
 */
object Graph {
  /**
   * Implicitly extracts the [[GraphOps]] member from a graph.
   *
   * To improve modularity the Graph type only contains a small set of basic operations.
   * All the convenience operations are defined in the [[GraphOps]] class which may be
   * shared across multiple graph implementations.
   */
  implicit def graphToGraphOps[VD: ClassTag, ED: ClassTag](g: Graph[VD, ED]) = g.ops
} // end of Graph object
