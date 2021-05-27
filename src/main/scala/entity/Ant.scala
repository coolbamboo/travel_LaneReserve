package entity

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import utils.Common.{MiusumTimeused_LinkPathInfo, generateOneOrZero, linksToGraph, simpleSort}
import utils.{Common, VertexEnum}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}


class Ant(val graphInit: GraphInit, val inited_g: Graph[Ant_VertexInfo, Ant_EdgeInfo],
          b_pherMuMap: Broadcast[mutable.Map[(VertexId, VertexId), Double]], limit: Double, algoSele: String) extends Serializable {

  val init_g: Graph[Ant_VertexInfo, Ant_EdgeInfo] = inited_g //蚂蚁要走的地图（初始）
  val start_id: VertexId = graphInit.start_vertex

  //定义结束节点的集合（多个）,按结束节点为末尾节点获取路径path:Graph集合（满足结束节点的路径可能多条）
  val end_vertexes: Map[Long, ArrayBuffer[Graph[Ant_VertexInfo, Ant_EdgeInfo]]] = graphInit.getEnd_vertex.foldLeft(
    Map.empty[Long, ArrayBuffer[Graph[Ant_VertexInfo, Ant_EdgeInfo]]]
  )((r: Map[Long, ArrayBuffer[Graph[Ant_VertexInfo, Ant_EdgeInfo]]], key: Long) =>
    r + (key -> ArrayBuffer[Graph[Ant_VertexInfo, Ant_EdgeInfo]]()))

  //结束节点的miu求和值以及产生这个结果的链路（一条链路，链路的边被连在一起形成一个数组）的集合，对应上文多条路径也可能产生多个结果
  val end_F: Map[Long, ListBuffer[MiusumTimeused_LinkPathInfo]] = graphInit.getEnd_vertex.foldLeft(
    Map.empty[Long, ListBuffer[MiusumTimeused_LinkPathInfo]]
  )((r: Map[Long, ListBuffer[MiusumTimeused_LinkPathInfo]], key: Long) =>
    r + (key -> ListBuffer()))

  /**
   * 蚂蚁前进一步
   *
   * @param start_id 从哪个节点开始前进
   */
  private def stepforward(start_id: Long, inputGraph: Graph[Ant_VertexInfo, Ant_EdgeInfo]): (Long, Graph[Ant_VertexInfo, Ant_EdgeInfo]) = {
    if (start_id < 0L)
      throw new Exception("No way found！")
    //找出当前节点后的可选的边(目的节点没有走过,且不是从目的节点退回的)
    val useful_edges = inputGraph.triplets.filter(et =>
      //逻辑：出发节点，目标节点没有走到过，目标节点不能是上一步走过的（不能退回，主要是0节点要考虑），backFromId不能有
      et.srcId == start_id && et.dstAttr.preVid < 0L && et.dstId != 0L && !et.srcAttr.backFromId.contains(et.dstId)
    )
    val useful_edge_num = useful_edges.count
    //没有可走的路径
    if (useful_edge_num == 0) {
      //回退
      backforward(start_id, inputGraph)
    } else {
      //收集可选路上的信息素
      /*val pherArr = useful_edges.map(et => {
        //获取节点中的边信息
        val edgeinfo = et.srcAttr.pher_chosen_Map.get(et.dstId)
        val pher = edgeinfo match {
          case Some(x) => x.pher
          case _ => throw new RuntimeException("vd has no edgeInfo")
        }
        (et.attr.id, pher)
      }).collect()*/
      val pherMuMap: mutable.Map[(VertexId, VertexId), Double] = b_pherMuMap.value.filter(v => v._1._1 == start_id
        && useful_edges.map(et => et.dstId).collect().contains(v._1._2))
      //算出选择哪条边(边的id)
      val selectedEdge = Common.computeProb(pherMuMap.map(m => (m._1._1, m._1._2, m._2)).toArray)
      //选中的节点，set其preid是startid
      val dstVertextRDD = useful_edges.filter(et => et.srcId == selectedEdge._1 && et.dstId == selectedEdge._2)
        .map(et => et.dstAttr).map(vd =>
        (vd.id, Ant_VertexInfo(vd.id, vd.name, vd.v_type, start_id, /*vd.pher_chosen_Map,*/ vd.backFromId, vd.pathFromPre,vd.sended)))
      //替换单点属性（更新全图）
      val new_g = inputGraph.joinVertices(dstVertextRDD) {
        (_, _, newdst) => newdst
      }
      //如果目标节点是终点，要做记录
      val dstVid = recordForEndvertex(dstVertextRDD, new_g)
      //返回下一个节点和生成的图
      (dstVid, new_g)
    }
  }

  //记录终点的链路
  private def recordForEndvertex(dstVertextRDD: RDD[(VertexId, Ant_VertexInfo)], new_g: Graph[Ant_VertexInfo, Ant_EdgeInfo]): VertexId = {
    val dstVid = dstVertextRDD.collect()(0)._1
    val dstV = dstVertextRDD.collect()(0)._2
    dstV.v_type match {
      case VertexEnum.end =>
        //记录（从源点到目的节点）
        val record_g = new_g.subgraph(epred = et => et.dstAttr.preVid >= 0L && et.dstAttr.preVid == et.srcId)
        val endv_graphs = end_vertexes.get(dstVid)
        endv_graphs match {
          case Some(x) => x.append(record_g)
          case _ => throw new RuntimeException("结束节点未放入终止节点集合中")
        }
      case _ => //print("不是终点")
    }
    dstVid
  }

  /**
   * 蚂蚁后退一步
   *
   * @param end_id ,从end_id开始后退,按preVid回退
   * @return 同上
   */
  private def backforward(end_id: VertexId, inputGraph: Graph[Ant_VertexInfo, Ant_EdgeInfo]): (Long, Graph[Ant_VertexInfo, Ant_EdgeInfo]) = {
    //找出当前节点可回退的路线
    val useful_edges = inputGraph.triplets.filter(et =>
      et.dstId == end_id && !(et.dstAttr.preVid < 0) && et.srcId == et.dstAttr.preVid
    )
    val useful_edge_num = useful_edges.count
    //无可退的路径，可能包括：前置节点小于0（事实退回了源点）
    if (useful_edge_num == 0) {
      val zeroGraph = inputGraph.subgraph(vpred = (vid, vd) => vid == 0L)
      //返回一个只有源点的图和-1代码（失败）
      (-1L, zeroGraph)
    } else {
      //找到路径上的两个点
      //目的节点的preVid属性设为负数(既然退了就不是原链路的一部分了)
      val dstVertextRDD = useful_edges.map(et => et.dstAttr).map(vd => {
        val preVid = -1L
        (vd.id, Ant_VertexInfo(vd.id, vd.name, vd.v_type, preVid, /*vd.pher_chosen_Map,*/ vd.backFromId, vd.pathFromPre, vd.sended))
      })
      //源节点的backFromId属性设为目的节点的vid
      val srcVertextRDD = useful_edges.map(et => et.srcAttr).map(vd => {
        val backFromId = end_id
        (vd.id, Ant_VertexInfo(vd.id, vd.name, vd.v_type, vd.preVid, /*vd.pher_chosen_Map,*/ vd.backFromId + backFromId, vd.pathFromPre, vd.sended))
      })
      //替换单点属性（更新全图）
      val new_g = inputGraph.joinVertices(dstVertextRDD) {
        (vid, old, newdst) => newdst
      }.joinVertices(srcVertextRDD) {
        (vid, old, newsrc) => newsrc
      }
      val srcId = useful_edges.map(et => et.srcId).collect()(0)
      //返回下一个节点和新的图
      (srcId, new_g)
    }
  }

  /**
   * 蚂蚁具体的行动(递归前进，经典蚁群)
   *
   * @param start_id 开始节点
   * @param init_g   地图
   * @param algoSele 选择经典蚁群算法还是加强蚁群算法
   * @return 程序运行的代码，1为正常运行结束
   */
  private def run(start_id: Long, init_g: Graph[Ant_VertexInfo, Ant_EdgeInfo], algoSele: String): Int = {
    algoSele match {
      case "common" => //经典蚁群GxACO
        val run_code = stepforward(start_id, init_g) match {
          case (-1L, g) => -1
          case (vid, g) =>
            //包含0，就是还有end节点没有路线，要继续找
            if (end_vertexes.values.map(_.size).toList.contains(0)) run(vid, g, "common")
            else 1
          case _ => 0
        }
        run_code
      case _ => //加强蚁群pGxACO
        val run_code = popFindPath(start_id, init_g) match {
          case (-1L, g) => -1
          case (vid, g) =>
            //包含0，就是还有end节点没有路线，要继续找
            if (end_vertexes.values.map(_.size).toList.contains(0)) run(vid, g, "improve")
            else 1
          case _ => 0
        }
        run_code
    }
  }

  /**
   * 计算限制条件下的LRP
   * 遍历end_vertexes，按照其中的路径（可能多条）计算出需要设专用道的edge，返回结果给end_F
   *
   * @param limit 限制条件
   */
  def computeLRP(limit: Double): Unit = {
    end_vertexes.foreach(entry => {
      val key = entry._1
      val graphs = entry._2
      graphs.foreach(graph => {
        val result: MiusumTimeused_LinkPathInfo = simpleSort(graph, limit, sortAlgo = "ratio")
        end_F(key).append(result)
      })
    })
  }

  /**
   * 启动蚂蚁行动
   */
  def start(): Unit = {
    val returnCode = this.run(start_id, init_g, algoSele)
    if (returnCode == 1)
      computeLRP(limit)
  }

  /**
   * 把一条链路Array[vid],按照Graph找出来
   *
   * @param links  Array[VertexId]一条链路(顺序)
   * @param inputG 输入的地图（原始的，preVid无值）
   */
  private def recordForEndvertex(links: Array[VertexId], inputG: Graph[Ant_VertexInfo, Ant_EdgeInfo]): Unit = {
    val linksG: Graph[Ant_VertexInfo, Ant_EdgeInfo] = linksToGraph(links, inputG)
    //记录（从源点到目的节点）
    val record_g = linksG.subgraph(epred = et => et.dstAttr.preVid >= 0L && et.dstAttr.preVid == et.srcId)
    val endv_graphs = end_vertexes.get(links.reverse(0))
    endv_graphs match {
      case Some(x) => x.append(record_g)
      case _ => throw new RuntimeException("结束节点未放入终止节点集合中")
    }
  }

  /**
   * 改进算法pGxACO：用pregel的方法选取路径
   *
   * @param start_id   开始节点的id，在本算法中，开始节点是一直不变的，区别于蚁群的递归
   * @param inputGraph 输入的原始地图
   * @return 开始节点的id和原始地图（input和output是一样的没变化）
   */
  private def popFindPath(start_id: Long, inputGraph: Graph[Ant_VertexInfo, Ant_EdgeInfo]): (Long, Graph[Ant_VertexInfo, Ant_EdgeInfo]) = {
    //mapReduce:瘦身，每条边计算为1或者0，1选中，0未选中。信息素越大越有利于选为1
    val edgeinfoInVertex_g: VertexRDD[Map[(VertexId, VertexId), Int]] = inputGraph.aggregateMessages[Map[(VertexId, VertexId), Int]](
      ctx => {
        val srcid = ctx.srcId
        val dstid = ctx.dstId
        val pher = b_pherMuMap.value.getOrElse((srcid, dstid), 0.0)
        if (pher > 0.0)
          ctx.sendToDst(Map((srcid, dstid) -> generateOneOrZero(pher)))
      },
      //(a, b) => a ++ b
      //如果考虑尽量让图链接，减少不连通，可以考虑聚合时每个收到消息的节点至少保证一条边为1
      (a, b) => {
        var edgeMap = a ++ b
        if(!edgeMap.exists(_._2 == 1)){
          edgeMap = edgeMap + (edgeMap.head._1 -> 1)//把第一条边设为1
        }
        edgeMap
      }
    )
    //选出的边RDD
    val edgeChosenRDD: RDD[((VertexId, VertexId), Int)] = edgeinfoInVertex_g.flatMap(x => x._2).filter(y => y._2 == 1)
    //生成新的边RDD（结合图）
    val edgeChosenInputRDD: RDD[((VertexId, VertexId), Ant_EdgeInfo)] = {
      val inputERDD: RDD[((VertexId, VertexId), Ant_EdgeInfo)] = inputGraph.edges.map(edge => ((edge.srcId, edge.dstId), edge.attr))
      edgeChosenRDD.join(inputERDD).mapValues(_._2)
    }
    //新的图
    val popfindgraph: Graph[Ant_VertexInfo, Ant_EdgeInfo] = Graph(inputGraph.vertices, edgeChosenInputRDD.map(edge => Edge(edge._1._1, edge._1._2, edge._2)))
    //对新图找到源点0到end节点的链路(不用上面的蚂蚁寻路，用pregel并行)
    //发送的消息结构为(Set(路径vid链条),该链条的总的TC, 当前弧的TC).逻辑就是找TC最小的贪婪
    val pregeledG = popfindgraph.pregel(
      (Array[Long](start_id), 0.0, -1.0), //初始化时发送的消息，这里发送(Set(起始节点的id),之前总路径的TC和=0.0, 当前单条弧TC=-1.0)
      Int.MaxValue, //迭代次数
      EdgeDirection.Out //发送方向，向出边的方向
    )(
      //接收到消息以后的动作
      vprog = (vid, vd, distMsg) => {
        //接收到的是发送点的set[vid]和这条边的TC时间
        var change = false
        if (distMsg._1.length == 1 && distMsg._1.head == start_id && distMsg._3 < 0.0) //初始发的消息不处理
          change = false
        else change = true
        if (change) {
          //println(s"顶点$vid，属性${(vd.pathFromPre._1.mkString("Array(", ", ", ")"), vd.pathFromPre._2)}，收到消息${(distMsg._1.mkString("Array(", ", ", ")"), distMsg._2, distMsg._3)}")
          if (vd.pathFromPre._1.nonEmpty) { //已经有值更新进来过
            if (vd.pathFromPre._2 > distMsg._2 + distMsg._3) { //保存的链路TC和更大，则更新成消息中的
              //println(s"顶点$vid，更新后${((distMsg._1 :+ vid).mkString("Array(", ", ", ")"), distMsg._2 + distMsg._3)}")
              Ant_VertexInfo(vd.id, vd.name, vd.v_type,
                distMsg._1.last,
                vd.backFromId,
                (distMsg._1 :+ vid, distMsg._2 + distMsg._3),
                mutable.Set())//只要更新过消息，sended失效，设为空
            } else { //保留自身的原链路
              //println(s"顶点$vid，没有更新，还是${(vd.pathFromPre._1.mkString("Array(", ", ", ")"), vd.pathFromPre._2)}")
              vd
            }
          } else {
            //第一次更新pathFromPre
            //println(s"顶点$vid，更新后${((distMsg._1 :+ vid).mkString("Array(", ", ", ")"), distMsg._2 + distMsg._3)}")
            Ant_VertexInfo(vd.id, vd.name, vd.v_type,
              distMsg._1.last,
              vd.backFromId,
              (distMsg._1 :+ vid, distMsg._2 + distMsg._3),
              mutable.Set())//只要更新过消息，sended失效，设为空
          }
        } else {
          if (vid == start_id) { //起始节点
            //println("初始化起始节点")
            Ant_VertexInfo(vd.id, vd.name, vd.v_type, -1L,
              vd.backFromId,
              (Array(vid), 0.0),vd.sended)
          } else {
            //println("初始化，不改变其他节点")
            vd
          }
        }
      },
      sendMsg = edgeTriplet => {
        //判断有环
        val srcVd = edgeTriplet.srcAttr
        /*//从起点发出的，单独处理
        if(edgeTriplet.srcId == start_id){
          println(s"开始节点${edgeTriplet.srcId} 给 顶点${edgeTriplet.dstId} 发送消息 ${srcVd.pathFromPre}成功")
          Iterator[(VertexId,(Array[Long], Double, Double))](
            (edgeTriplet.dstId,
              (Array(start_id), 0.0, edgeTriplet.attr.TC)
            ))
        }*/
        if (srcVd.pathFromPre._1.isEmpty || srcVd.pathFromPre._1.contains(edgeTriplet.dstId) ||
          srcVd.sended.contains(edgeTriplet.dstId)) { //会形成环,或者向某节点重复发送
          //println(s"顶点${edgeTriplet.srcId} 给 顶点${edgeTriplet.dstId} 发送消息 ${(srcVd.pathFromPre._1.mkString("Array(", ", ", ")"), srcVd.pathFromPre._2)}失败")
          Iterator.empty
        } else {
          srcVd.sended += edgeTriplet.dstId
          //println(s"顶点${edgeTriplet.srcId} 给 顶点${edgeTriplet.dstId} 发送消息 ${(srcVd.pathFromPre._1.mkString("Array(", ", ", ")"), srcVd.pathFromPre._2, edgeTriplet.attr.TC)}成功")
          Iterator[(VertexId, (Array[Long], Double, Double))](
            (edgeTriplet.dstId,
              (srcVd.pathFromPre._1, srcVd.pathFromPre._2, edgeTriplet.attr.TC)
            ))
        }
      },
      mergeMsg = (msg1, msg2) => {
        if (msg1._2 + msg1._3 >= msg2._2 + msg2._3)
          msg2
        else msg1
      }
    )
    //上面的并行操作未做记录，下面记录
    graphInit.getEnd_vertex.foreach(vid => {
      val count: Long = pregeledG.vertices.filter(vd => vd._1 == vid).count()//新图里有无结束节点
      if(count == 1) {//存在结束节点
        val links: Array[VertexId] = pregeledG.vertices.filter(vd => vd._1 == vid).map(vd => vd._2.pathFromPre._1).collect()(0)
        if (links.nonEmpty)
          recordForEndvertex(links, inputGraph)
      }
    })
    //输入输出一样
    (start_id, inputGraph)
  }
}

object Ant {
  def apply(graphInit: GraphInit, init_graph: Graph[Ant_VertexInfo, Ant_EdgeInfo],
            b_pherMuMap: Broadcast[mutable.Map[(VertexId, VertexId), Double]], limit: Double, algoSele: String): Ant = {
    val ant = new Ant(graphInit, init_graph, b_pherMuMap, limit, algoSele)
    ant
  }
}
