package entity

import org.apache.spark.graphx.Graph

abstract class GraphInit {
  val start_vertex = 0L

  def getEnd_vertex : Set[Long] = null

  def getInit_g : Graph[VertexInfo, EdgeInfo] = null

}
