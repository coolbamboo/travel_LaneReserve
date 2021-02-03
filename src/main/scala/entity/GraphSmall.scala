package entity

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import utils.Common._
import utils.VertexEnum

class GraphSmall extends GraphInit with Serializable {
  //开始节点(只能是单起点，而且编号从0开始)
  override val start_vertex = 0L

  override def getEnd_vertex = Set(5L,8L,10L,11L)

  override def getInit_g: Graph[VertexInfo, EdgeInfo] = {
    set_init_g
  }
  var set_init_g : Graph[VertexInfo, EdgeInfo] = null

  def init_in(sc : SparkContext): Graph[VertexInfo, EdgeInfo] = {
    val myVertices = sc.parallelize(Array(
      (0L, new VertexInfo(0L , "起点0", VertexEnum.start)),
      (1L, new VertexInfo(1L, "1", VertexEnum.pass)),
      (2L, new VertexInfo(2L, "2", VertexEnum.pass)),
      (3L, new VertexInfo(3L, "3", VertexEnum.pass)),
      (4L, new VertexInfo(4L, "4", VertexEnum.pass)),
      (5L, new VertexInfo(5L, "5", VertexEnum.end)),
      (6L, new VertexInfo(6L, "6", VertexEnum.pass)),
      (7L, new VertexInfo(7L, "7", VertexEnum.pass)),
      (8L, new VertexInfo(8L, "8", VertexEnum.end)),
      (9L, new VertexInfo(9L, "9", VertexEnum.pass)),
      (10L, new VertexInfo(10L, "10", VertexEnum.end)),
      (11L, new VertexInfo(11L, "11", VertexEnum.end))
    ))

    val myEdges = sc.parallelize(Array(
      Edge(0,1,new EdgeInfo(1, 2.1, 5 , 3.8 , 0.3 , pher_min)),
      Edge(0,2,new EdgeInfo(2, 1.2, 9 , 6.4 , 0.4 , pher_min)),
      Edge(0,3,new EdgeInfo(3, 2, 5 , 3.5 , 0.4 , pher_min)),
      Edge(1,0,new EdgeInfo(4, 2.1, 5 , 3.8 , 0.3 , pher_min)),
      Edge(1,4,new EdgeInfo(5, 1.7, 7 , 3.9 , 0.8 , pher_min)),
      Edge(1,5,new EdgeInfo(6, 2.1, 10 , 7.6 , 0.3 , pher_min)),
      Edge(1,6,new EdgeInfo(7, 2.2, 10 , 6.8 , 0.5 , pher_min)),
      Edge(2,0,new EdgeInfo(8, 1.2, 9 , 6.4 , 0.4 , pher_min)),
      Edge(2,4,new EdgeInfo(9, 1.8, 7 , 3.6 , 0.9 , pher_min)),
      Edge(2,5,new EdgeInfo(10, 2, 6 , 4.6 , 0.3 , pher_min)),
      Edge(2,6,new EdgeInfo(11, 0.9, 10 , 7.2 , 0.4 , pher_min)),
      Edge(3,0,new EdgeInfo(12, 1.2, 9 , 6.4 , 0.4 , pher_min)),
      Edge(3,7,new EdgeInfo(13, 0.9, 7 , 5.5 , 0.3 , pher_min)),
      Edge(4,1,new EdgeInfo(14, 1.7, 7 , 3.9 , 0.8 , pher_min)),
      Edge(4,2,new EdgeInfo(15, 1.8, 7 , 3.6 , 0.9 , pher_min)),
      Edge(4,8,new EdgeInfo(16, 1, 6 , 3.8 , 0.6 , pher_min)),
      Edge(4,9,new EdgeInfo(17, 1.7, 5 , 3.7 , 0.4 , pher_min)),
      Edge(5,1,new EdgeInfo(18, 2.1, 10 , 7.6 , 0.3 , pher_min)),
      Edge(5,2,new EdgeInfo(19, 2, 6 , 4.6 , 0.3 , pher_min)),
      Edge(5,8,new EdgeInfo(20, 1, 4 , 2.8 , 0.4 , pher_min)),
      Edge(5,10,new EdgeInfo(21, 1.5, 6 , 4.2 , 0.4 , pher_min)),
      Edge(6,1,new EdgeInfo(22, 2.2, 10 , 6.8 , 0.5 , pher_min)),
      Edge(6,2,new EdgeInfo(23, 0.9, 10 , 7.2 , 0.4 , pher_min)),
      Edge(6,7,new EdgeInfo(24, 0.6, 11 , 7.5 , 0.5 , pher_min)),
      Edge(6,8,new EdgeInfo(25, 2.3, 4 , 3 , 0.3 , pher_min)),
      Edge(6,10,new EdgeInfo(26, 1.5, 7 , 3.8 , 0.8 , pher_min)),
      Edge(7,6,new EdgeInfo(27, 0.6, 11 , 8.4 , 0.3 , pher_min)),
      Edge(7,9,new EdgeInfo(28, 1.7, 11 , 8.1 , 0.4 , pher_min)),
      Edge(8,4,new EdgeInfo(29, 1, 6 , 3.8 , 0.6 , pher_min)),
      Edge(8,5,new EdgeInfo(30, 0.9, 4 , 2.2 , 0.8 , pher_min)),
      Edge(8,6,new EdgeInfo(31, 2.3, 4 , 2.9 , 0.4 , pher_min)),
      Edge(8,11,new EdgeInfo(32, 0.6, 6 , 3.6 , 0.7 , pher_min)),
      Edge(9,4,new EdgeInfo(33, 1.7, 5 , 3.7 , 0.4 , pher_min)),
      Edge(9,7,new EdgeInfo(34, 1.7, 11 , 8.2 , 0.3 , pher_min)),
      Edge(9,8,new EdgeInfo(35, 1.2, 4 , 2.1 , 0.9 , pher_min)),
      Edge(9,10,new EdgeInfo(36, 2.4, 6 , 4.1 , 0.5 , pher_min)),
      Edge(9,11,new EdgeInfo(37, 1.6, 8 , 5.5 , 0.5 , pher_min)),
      Edge(10,5,new EdgeInfo(38, 1.5, 6 , 4.2 , 0.4 , pher_min)),
      Edge(10,9,new EdgeInfo(39, 2.4, 6 , 4.1 , 0.5 , pher_min)),
      Edge(11,8,new EdgeInfo(40, 0.6, 6 , 3.6 , 0.7 , pher_min)),
      Edge(11,9,new EdgeInfo(41, 1.6, 8 , 5.5 , 0.5 , pher_min))

    ))
    val myGraph: Graph[VertexInfo, EdgeInfo] = Graph(myVertices, myEdges)
    myGraph.cache()
  }
}

object GraphSmall {
  def apply(sc : SparkContext): GraphSmall = {
    val myGraph = new GraphSmall()
    myGraph.set_init_g = myGraph.init_in(sc)
    myGraph
  }
}
