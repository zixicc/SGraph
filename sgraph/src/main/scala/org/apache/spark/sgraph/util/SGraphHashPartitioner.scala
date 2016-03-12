package org.apache.spark.sgraph.util

import org.apache.spark.Partitioner

/**
 * specific hash partitioner for sgraph
 */
private[sgraph]
class SGraphHashPartitioner(partitions: Int) extends  Partitioner{
  def numPartitions = partitions

  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => nonNegativeMod(key.hashCode, numPartitions)
  }

  override def equals(other: Any): Boolean = other match {
    case h: SGraphHashPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  // same as PartitionStrategy.EdgePartition1D.getPartition
  def nonNegativeMod(x: Int, mod: Int): Int = {
    val mixingPrime = 1125899906842597L
    (math.abs(x * mixingPrime) % mod).toInt
  }
}


