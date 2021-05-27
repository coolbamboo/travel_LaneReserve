package algorithm

import entity._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.{SparkConf, SparkContext}
import utils.Common
import utils.Common._

import java.util.Date
import scala.collection.mutable

object main{
  /**
   *
   * @param args 参数说明
   * args0：迭代次数（不超50次，可能某条边一次迭代会被加两次，故能较快达到pher_max）
   * args1：蚂蚁数（每次迭代启用）
   * args2：并行数
   * args3：“local”运行，还是其他运行方式
   * args4：通行的总时间（限制时间）
   * args5：N(输入的节点数，这是大图例要用的。只要输入的节点数大于12，就按大图自动生成)
   * args6: 算法选择，默认是common（普通的蚁群算法实现GxACO），或者pGxACO的算法
   * e.x.
   */
  def main(args: Array[String]): Unit = {
    val iter_num: Int = args(0).toInt
    val ant_num: Int = args(1).toInt
    val task_num = args(2).toInt
    val runStyle = args(3)
    val limit = args(4).toDouble
    val v_num = args(5).toInt
    val algo = args(6)

    val conf = new SparkConf().setAppName("travel_LRP")
    if (task_num > 0)
      conf.set("spark.default.parallelism", task_num.toString)
    else
      conf.set("spark.default.parallelism", "1")

    if ("local".equals(runStyle.trim)) {
      if (task_num > 0) {
        conf.setMaster(s"local[$task_num]") //local run
      } else {
        conf.setMaster("local[1]") //local run
      }
    }
    val sc = new SparkContext(conf)
    var graphInit : GraphInit = null
    if(v_num > 12){
      graphInit = GraphGene(sc, v_num)
    }else{
      graphInit = GraphSmall(sc)
    }
    //这是每只蚂蚁都要操作的地图
    val antsGraph: Graph[Ant_VertexInfo, Ant_EdgeInfo] = Common.init(graphInit.getInit_g)
    //程序开始，开始记录
    val record = new Record
    //begin时间
    val startTime = new Date().getTime
    //获取边信息素的集合(源点，目标点，信息素)
    val pherMap: Map[(VertexId, VertexId), Double] = antsGraph.triplets.map{ et =>
      (et.srcId, et.dstId) -> et.attr.pher
    }.collect().toMap
    val pherMuMap: mutable.Map[(VertexId, VertexId), Double] = collection.mutable.Map(pherMap.toSeq:_*)
    //最好的路径--累加器，输入结束节点和最大保留数
    val bestPathAccu = new BestPathAccumulator(graphInit.getEnd_vertex, bestnum, limit)
    sc.register(bestPathAccu, "bestPathAccumulator")
    //按迭代次数循环
    for(i <- 1 to iter_num){
      //每次迭代的开始时间
      val startTimeIter = new Date().getTime
      //广播信息素（变量可能比较大）
      val b_pherMuMap: Broadcast[mutable.Map[(VertexId, VertexId), Double]] = sc.broadcast(pherMuMap)
      /*val antsRDD = sc.parallelize(0 until ant_num, task_num).map{i =>
        val ant = Ant(graphSmall, antsGraph, b_pherMuMap, limit)
        ant
      }
      val dealedRDD = antsRDD.mapPartitions(ants => ants.map(ant =>{
        ant.start()
        ant.end_F.foreach(map =>{
          val k = map._1
          val vs = map._2
          vs.foreach(v =>{
            bestPathAccu.add((k, v))
          })
        })
        ant
      }))
      dealedRDD.count()//RDD执行*/
      for(_ <- 1 to ant_num){
        val ant = Ant(graphInit, antsGraph, b_pherMuMap, limit, algo)
        ant.start()
        ant.end_F.foreach(map =>{
          val k = map._1
          val vs = map._2
          vs.foreach(v =>{
            bestPathAccu.add((k, v))
          })
        })
      }

      //更新信息素
      algo match {
        case "common" =>
          bestPathAccu.value.foreach(map =>{
            val vs = map._2
            vs.foreach(v =>{
              //要更新信息素的边
              //Timeused不超过limit限制的，才更新
              if(v._1._2 <= limit) {
                val edgeUpPher: Array[(VertexId, VertexId)] = v._2.filter(_._2.chosen).map(_._1)//只要选为保留道的边的两端点
                edgeUpPher.foreach(edgeSelected => {
                  var pher = pherMuMap.getOrElse((edgeSelected._1, edgeSelected._2), pher_min)
                  pher = pher + pher_deta
                  if (pher > pher_max) pher = pher_max
                  pherMuMap.put((edgeSelected._1, edgeSelected._2), pher)
                })
              }
            })
          })
        //加强算法pGxACO，更新信息素与上不同，是整个链路上的边增加
        case _ =>
          bestPathAccu.value.foreach(map =>{
            val vs = map._2
            vs.foreach(v =>{
              //要更新信息素的边，整条链路的边更新
              //Timeused不超过limit限制的，才更新
              if(v._1._2 <= limit) {
                val edgeUpPher: Array[(VertexId, VertexId)] = v._2.map(_._1) //只要边的两端点
                edgeUpPher.foreach(edgeSelected => {
                  var pher = pherMuMap.getOrElse((edgeSelected._1, edgeSelected._2), pher_min)
                  pher = pher + pher_deta
                  if (pher > pher_max) pher = pher_max
                  pherMuMap.put((edgeSelected._1, edgeSelected._2), pher)
                })
              }
            })
          })
      }

      //每次迭代的结束时间
      val stopTimeIter = new Date().getTime
      record.time_everyiter = stopTimeIter - startTimeIter
      //打印信息素
      println(s"第${i}次循环后的信息素：")
      pherMuMap.foreach(entry => {
        println(entry._1 + "->" +entry._2)
      })
    }
    //end
    val stopTime = new Date().getTime
    record.time_runonce = stopTime - startTime
    //获取最好的终点所在的边和值并保存
    bestPathAccu.value.foreach{ map =>{
      val k = map._1
      val v = map._2
      val outputs = Output(iter_num, ant_num, task_num,
        runStyle, limit, v_num, algo,
        record, k, v)
      saveToLocal(outputs) }
    }
  }
}
