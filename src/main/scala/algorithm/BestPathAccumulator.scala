package algorithm

import org.apache.spark.util.AccumulatorV2
import utils.Common
import utils.Common.MiusumTimeused_LinkPathInfo

import scala.collection.mutable.ListBuffer

class BestPathAccumulator(val endnodes : Set[Long], val bestnum : Int, val limit : Double) extends AccumulatorV2[(Long, MiusumTimeused_LinkPathInfo)
  ,Map[Long, ListBuffer[MiusumTimeused_LinkPathInfo]]] {

  //Map的初始化函数
  val initfunc:
    (Map[Long, ListBuffer[MiusumTimeused_LinkPathInfo]], Long) => Map[Long, ListBuffer[MiusumTimeused_LinkPathInfo]] =
    (r:Map[Long, ListBuffer[MiusumTimeused_LinkPathInfo]], i:Long) =>
    r + (i -> ListBuffer[MiusumTimeused_LinkPathInfo]())
  //最好路径的动态初始化（根据结束点的集合动态生成）
  private val bestPaths: Map[Long, ListBuffer[MiusumTimeused_LinkPathInfo]] =
    endnodes.foldLeft(Map.empty[Long, ListBuffer[MiusumTimeused_LinkPathInfo]])(initfunc)


  override def isZero: Boolean = {
    var isZero = true
    bestPaths.foreach(x => {
      if(x._2.nonEmpty) {
        isZero = false
        return isZero
      }
    })
    isZero
  }

  override def copy(): AccumulatorV2[(Long, MiusumTimeused_LinkPathInfo) ,Map[Long, ListBuffer[MiusumTimeused_LinkPathInfo]]] = {
    val newAcc = new BestPathAccumulator(endnodes, bestnum, limit)
    newAcc.bestPaths.foreach{ kv =>
      val k = kv._1
      val v = kv._2
      v.appendAll(bestPaths.getOrElse(k,ListBuffer()))
    }
    newAcc
  }

  override def reset(): Unit = {
    bestPaths.foreach{kv =>
      val v = kv._2
      v.clear()
    }
  }

  override def add(input: (Long, MiusumTimeused_LinkPathInfo)): Unit = {
    val k = input._1
    val v = input._2
    v synchronized Common.addInBestPaths(bestPaths, k, v, bestnum, limit)
  }

  override def merge(other: AccumulatorV2[(Long, MiusumTimeused_LinkPathInfo),
    Map[Long, ListBuffer[MiusumTimeused_LinkPathInfo]]]): Unit = {
    other match {
      case o: BestPathAccumulator =>
        val otherBestPaths = o.bestPaths
        bestPaths.foreach {kv =>
          val k = kv._1
          val v = kv._2
          v.appendAll(otherBestPaths.getOrElse(k, ListBuffer()))
          if(v.length > bestnum) {
            val sliced = v.sortWith((a,b) => a._1._1 <= b._1._1).take(bestnum)
            v.clear()
            v.appendAll(sliced)
          }
        }
    }
  }

  override def value: Map[Long, ListBuffer[MiusumTimeused_LinkPathInfo]] = bestPaths
}