package org.apache.spark.sgraph.lib

import org.apache.spark.sgraph.{ScatterGather, VertexId, Graph}

import scala.reflect.ClassTag

/**
 * Run PageRank for a fixed number of iterations.
 * Return a graph with vertex attribute and edge attribute
 */
object PageRank {
  /**
   * @param graph the graph
   * @param numIterations iteration number
   * @param resetProb pagerank reset probability
   * @tparam VD the original vertex attribute
   * @tparam ED the original edge attribute
   */
  def run[VD: ClassTag, ED: ClassTag](
    graph: Graph[VD, ED],
    numIterations: Int,
    resetProb: Double = 0.15) = {
    val pagerankGraph = graph.mapVertices((vid, attr) => 1.0).cache()
    //graph.unpersistVertices(false)
    def vertexProgram(vid: VertexId, attr: Double, valueSum: Double): Double =
       resetProb + (1.0 - resetProb) * valueSum
    val vprog = (vid: VertexId, attr: Double, valueSum: Double) => resetProb + (1.0 - resetProb) * valueSum
    def scatter(vid: VertexId, attr: Double, degree: Int): (VertexId, Double) = (vid, attr / degree)
    def gather(a: Double, b: Double): Double = a + b
    val initMsg = 0.0
    ScatterGather(pagerankGraph, initMsg, numIterations)(vertexProgram, scatter, gather)
  }
}
