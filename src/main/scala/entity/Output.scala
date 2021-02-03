package entity

import utils.Common.{MiusumTimeused_LinkPathInfo, geneLinkedPath}

import scala.collection.mutable.ListBuffer

/**
 * results:
  * iter_num 迭代次数
  * ant_num 蚂蚁个数
  * task_num 任务数（并行数）
  * "local" running , or other running style 运行方式
  * limit：限制通行时间
 * v_num: 节点数
 * algo：算法选择，默认common
  * record 两个时间（全运行，每次迭代）
 * end_vertex：结束节点（可能多个）
 * miuSigma：miu值的求和
 * timeUsed：实际通行的时间
  * link：选中的链路（含是否被选为LR）
  */
class Output(iter_num: Int, ant_num: Int, task_num: Int,
             runStyle: String, limit: Double, v_num : Int, algo : String,
             record: Record, val end_vertex: Long, val miuSigma : Double, val timeUsed : Double, val link : String) extends Serializable {

  override def toString: String = {
    val sb = new StringBuilder("")
    sb.append(iter_num).append(",")
    sb.append(ant_num).append(",")
    sb.append(task_num).append(",")
    sb.append(runStyle).append(",")
    sb.append(limit).append(",")
    sb.append(v_num).append(",")
    sb.append(algo).append(",")

    sb.append(record.time_runonce).append(",")
    sb.append(record.time_everyiter).append(",")

    sb.append("结束节点：" + end_vertex)
    sb.append("miu值求和：" + miuSigma)
    sb.append("实际的时间：" + timeUsed)
    sb.append("链路（选中LR）：" + link)

    //sb.append("\r\n")
    sb.toString()
  }

}

object Output {

  def apply(iter_num: Int, ant_num: Int, task_num: Int,
            runStyle: String, limit: Double, v_num : Int, algo: String,
            record: Record, end_vertex: Long, results: ListBuffer[MiusumTimeused_LinkPathInfo]): Vector[Output] = {
    val outputs = results.map{ evolve =>
      //链路是排好序的
      val linkedPath = geneLinkedPath(evolve._2.map(x => (x._1, x._2.chosen))).mkString("Array(", ", ", ")")
      val output = new Output(iter_num, ant_num, task_num,
        runStyle, limit, v_num, algo,
        record, end_vertex, miuSigma = evolve._1._1, timeUsed = evolve._1._2, linkedPath)
      output
    }
    outputs.toVector
  }
}
