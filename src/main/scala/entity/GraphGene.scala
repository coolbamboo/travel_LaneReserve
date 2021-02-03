package entity

import org.apache.spark.SparkContext
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Graph, VertexId}
import utils.Common.{doubleKeepNum, pher_min, randoff, randval}
import utils.VertexEnum

class GraphGene(val vertext_num : Int) extends GraphInit with Serializable {
  //开始节点(只能是单起点，而且编号从0开始)
  override val start_vertex = 0L

  override def getEnd_vertex: Set[VertexId] = set_end_vertex
  var set_end_vertex : Set[Long] = null

  override def getInit_g: Graph[VertexInfo, EdgeInfo] = set_init_g
  var set_init_g : Graph[VertexInfo, EdgeInfo] = null

  def init_in(sc : SparkContext): Graph[VertexInfo, EdgeInfo] = {
    //生成GridGraph
    val grid = GraphGenerators.gridGraph(sc, math.sqrt(vertext_num).toInt, math.sqrt(vertext_num).toInt)
    val graph: Graph[VertexInfo, EdgeInfo] = grid.mapVertices((vid, vd) => {
      var typeforv = VertexEnum.pass
      if (vid == 0L)
        typeforv = VertexEnum.start
      new VertexInfo(vid, vid.toString, typeforv)
    }).mapEdges(map = edge => {
      //仿真数据
      val lamda = doubleKeepNum(randval(0.5, 2.5), 1)
      val TC: Int = randoff(randval(4, 12))
      val TS = doubleKeepNum(randval(TC / 2, TC * 0.75), 1)
      val miu = doubleKeepNum(randval(0.3, 0.91), 1)
      new EdgeInfo(edge.srcId, lamda, TC, TS, miu, pher_min)
    })
    //最后一个结点是终点
    val maxId = graph.vertices.max()(ord = new Ordering[(VertexId, VertexInfo)]() {
      override def compare(x: (VertexId, VertexInfo), y: (VertexId, VertexInfo)): Int = x._1.compare(y._1)
    })._1
    //设置终点
    set_end_vertex = Set(maxId)
    //终点RDD
    val endVertexRDD = graph.triplets.filter(et => et.dstId == maxId )
      .map(et => et.dstAttr).map(vd =>
      (vd.id, new VertexInfo(vd.id,vd.name,VertexEnum.end)))
    //替换终点属性
    val new_g = graph.joinVertices(endVertexRDD){
      (_, _, newdst) => newdst
    }
    new_g.cache()
  }

}

object GraphGene {
  def apply(sc : SparkContext, vertexNum : Int): GraphGene = {
    val myGraph = new GraphGene(vertexNum)
    myGraph.set_init_g = myGraph.init_in(sc)
    myGraph
  }
}
