package entity

import utils.VertexEnum.VertexEnum

import scala.collection.mutable

class VertexInfo(val id:Long, val name:String, val v_type : VertexEnum) extends Serializable
class EdgeInfo(val id:Long, val lam:Double, val TC:Double, val TS:Double, val miu:Double, val pher:Double) extends Serializable

/**
 * 蚁群算法需要的点的信息
 *
 * @param preVid 前一个节点的id,默认-1(说明本节点蚂蚁未走到)
 * @param pher_chosen_Map 和后面节点形成的边的情况，Map（后面节点id，边的情况）
 * @param backFromId  从哪些节点退回来过，这些节点都不能再去了（说明这些节点走不通）
 *                    以下是并行算法需用的：
 * @param sourceVid 来自于哪个Connect component
 * @param pathFromPre 来自之前的路径（测试是否有环）
 * 其他参数默认都是-1
 */
case class Ant_VertexInfo(override val id:Long,
                          override val name:String,
                          override val v_type : VertexEnum,
                          preVid : Long ,
                          //pher_chosen_Map : mutable.Map[Long, Ant_EdgeInfo],
                          backFromId : mutable.Set[Long],
                          sourceVid : Long ,
                          pathFromPre : (Array[Long],Double)
                          ) extends VertexInfo(id, name, v_type)

/**
 * 蚁群需要的边的信息
 *
 * @param chosen 是否被选中做专用道（蚂蚁走过的路才被选中）
 */
case class Ant_EdgeInfo(override val id:Long,
                        override val lam:Double,
                        override val TC:Double,
                        override val TS:Double,
                        override val miu:Double,
                        override val pher:Double,
                        chosen:Boolean) extends EdgeInfo(id, lam, TC, TS, miu, pher)