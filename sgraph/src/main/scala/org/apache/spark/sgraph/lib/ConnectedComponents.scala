package org.apache.spark.sgraph.lib

import scala.reflect.ClassTag
import org.apache.spark.sgraph._
/**
* compute connected components
*/
object ConnectedComponents {

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[VertexId, ED] = {
    val ccGraph = graph.mapVertices((vid, _) => vid)
    val initalMsg = Long.MaxValue
    val vprog = (id:VertexId, attr: VertexId, msg: VertexId) => math.min(attr, msg)
    // get the lowest vertex id that scatters from source vertex
    def scatter(srcId: VertexId, srcAttr: VertexId, dstId: VertexId, dstAttr: VertexId): Iterator[(VertexId, (VertexId, VertexId))] = {
      if (srcAttr < dstAttr) {
        Iterator((dstId, (srcAttr, srcId)))
      } else if (srcAttr > dstAttr) {
        Iterator((srcId, (dstAttr, srcId)))
      } else {
        Iterator.empty
      }
    }
    // second element of tuple a or b is the active vertex id
    val gather = (a:(VertexId, VertexId), b: (VertexId, VertexId)) => {
      if (a._1 < b._1)
        a
      else
        b
    }
    val newGraph = ScatterGather2(ccGraph, initalMsg)(vprog, scatter, gather)
    newGraph
  }
}
