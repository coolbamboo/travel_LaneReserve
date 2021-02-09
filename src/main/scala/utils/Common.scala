package utils

import entity._
import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

import java.util.concurrent.ThreadLocalRandom
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Common {
  val pher_min = 0.1 //最小的信息素
  val pher_max = 10 //最大的信息素
  val pher_deta = 0.1 //每次增加的信息素
  val bestnum = 1 //每个结束点的最佳路径（miu求和最小）的保留量

  private val sigBound = 6 //sigmoid算法中的上限，不要超过6

  type LinkPathInfo = Array[((Long,Long), Ant_EdgeInfo)]
  //type Miusum_LinkPathInfo = (Double, LinkPathInfo)
  //链路，srcid、dstid、chosen
  type PathChosen = ((Long,Long),Boolean)
  //结果输出，输出的两个值，miu和以及所用时间
  type MiusumTimeused = (Double, Double)
  type MiusumTimeused_LinkPathInfo = (MiusumTimeused, LinkPathInfo)

  /**
   * 按概率随机选某一个边
   * @param pherArr 每条边（含边srcid\dstid）信息素数组
   * @return 选中的边(带两个顶点id和信息素)
   */
  def computeProb(pherArr : Array[(Long, Long, Double)]): (Long, Long, Double) = {
    var selectedId = 0
    val total_pher = pherArr.map(_._3).sum
    val prob : Array[(Long, Long, Double)] = new Array(pherArr.length)
    for (j <- pherArr.indices) {
      prob(j) = (pherArr(j)._1 , pherArr(j)._2, pherArr(j)._3 / total_pher)
    }
    //choose
    var temp: Double = 0.0

    temp = randval(0,1) //0~1.0之间
    import scala.util.control.Breaks._
    breakable {
      for (i <- prob.indices) {
        temp = temp - prob(i)._3
        if (temp < 0.0) {
          selectedId = i
          break()
        }
      }
    }
    prob(selectedId)
  }


  /**
   * 0-1背包算法
   * @param graph 一条链路path
   * @param limit 给出的限制值
   * @return (sigmamiu，标注选中边信息的图含两端点id)
   */
  def package01(graph: Graph[VertexInfo, EdgeInfo], limit:Double):Graph[VertexInfo, EdgeInfo] = {
    null
  }

  /**
   * 隐式转换，对Ant_EdgeInfo进行排序
   * 按照miu小的排前
   * miu相等则TS大的排前
   */
  implicit val ord: Ordering[Ant_EdgeInfo] = new Ordering[Ant_EdgeInfo]() {
    override def compare(x: Ant_EdgeInfo, y: Ant_EdgeInfo): Int = {
      val xmiu = x.miu
      val ymiu = y.miu
      val xTS = x.TS
      val yTS = y.TS
      if (xmiu < ymiu) -1
      else if (xmiu > ymiu) 1
      else { //miu相等
        if (xTS > yTS) -1 //TS大的排在前
        else if (xTS < yTS) 1
        else 0
      }
    }
  }
  /**
   * 对一条路径做简单排序，求得的结果返回
   * @return (sigmamiu，标注选中边信息的图含两端点id)
   * @param graph 一条链路path
   * @param limit 给出的限制值
   */
  def simpleSort(graph: Graph[Ant_VertexInfo,Ant_EdgeInfo], limit:Double, sortAlgo: String)
            : MiusumTimeused_LinkPathInfo = {
    val up: Double = graph.edges.map(e => e.attr.TC).reduce(_+_)//上限(正常通行)
    val down: Double = graph.edges.map(e => e.attr.TS).reduce(_+_)//下限（在专用道通行）
    if(limit >= up) {//正常通行都可以达到限制的时间，返回全链路
      ((0.0, up), graph.triplets.map(et => ((et.srcId,et.dstId),et.attr)).collect())
    }else if(limit < down){//在专用道都无法达到限制时间，铁定超时
      val miusum = graph.edges.map(e => e.attr.miu).reduce(_+_) //miu求和
      val pathlink = graph.triplets.map(et =>
        ((et.srcId,et.dstId), Ant_EdgeInfo(et.attr.id, et.attr.lam, et.attr.TC, et.attr.TS, et.attr.miu, et.attr.pher,
          chosen = true))).collect() //全部选为专用道
      ((miusum, down), pathlink) //这条路线全设专用道都不够时间限制，但我们还是保留。是否用来pher减少（个人不建议整条路径降权重）？
    }else{
      //生成(源点，终点，边)的三元组
      val pathRDD: RDD[((VertexId, VertexId), Ant_EdgeInfo)] =
        graph.triplets.map(et => ((et.srcId,et.dstId),et.attr))
      val returnEdges = pathRDD.collect()
      //排序，根据算法要求排
      var sortedRDD: RDD[((VertexId, VertexId), Ant_EdgeInfo)] = null
      sortAlgo match {
        case "ratio" =>
          sortedRDD = pathRDD.sortBy( x=> {
            val edgeinfo: Ant_EdgeInfo = x._2
            edgeinfo.miu/(edgeinfo.TC - edgeinfo.TS)
          })
        case _ =>
          sortedRDD = pathRDD.sortBy(x=>x._2)
      }
      val sortedEdge = sortedRDD.collect()
      //从头遍历，看头几个能选中
      val num = getTopSortedNum(limit, sortedEdge)
      //选中的专用道边
      val chosenEdge = sortedEdge.take(num)
      //没选中做专用道边
      val noChosenEdge = sortedEdge.slice(num, sortedEdge.length)
      val src_dst = chosenEdge.map(_._1)//只要两点
      //要返回的边集合，其中选中的边设为true
      val setEdges: LinkPathInfo = returnEdges.map(x => {
        var chosen = false
        if (src_dst.contains(x._1)) {
          chosen = true
        }
        val edgeInfo = x._2
        (x._1, Ant_EdgeInfo(edgeInfo.id, edgeInfo.lam, edgeInfo.TC, edgeInfo.TS, edgeInfo.miu, edgeInfo.pher,
          chosen))
      })
      ((chosenEdge.map(_._2.miu).sum, chosenEdge.map(_._2.TS).sum + noChosenEdge.map(_._2.TC).sum), setEdges)
    }
  }

  private def getTopSortedNum(limit: Double, sortedEdge: LinkPathInfo):Int = {
    var returnNum = 0
    for (i <- 1 to sortedEdge.length) {
      val (head, tail) = sortedEdge.splitAt(i)
      if (tail.length > 0) {
        if (head.map(_._2.TS).sum + tail.map(_._2.TC).sum <= limit) {
          return i
        }
      }else if(tail.length == 0){
        if (head.map(_._2.TS).sum <= limit){
          return i
        }
        else return -1
      }
      returnNum = i
    }
    returnNum
  }

  /**
   * a到b的随机数
   * @param a 随机数下限
   * @param b 随机数上限（不含）
   * @return 随机数
   */
  def randval(a: Double, b:Double) :Double = {
    val r = ThreadLocalRandom.current().nextDouble(1.0)
    val value = a + (b - a) * r
    value
  }

  /**
   * 获得新的节点、边属性（初始化节点、边）
   * 要生成新的图仍然要有点和边的RDD，而不是在原有的基础上改完属性就行
   * @param initGraph 最初的地图
   */
  def init(initGraph : Graph[VertexInfo, EdgeInfo]): Graph[Ant_VertexInfo, Ant_EdgeInfo] = {
    val init_g = initGraph.mapVertices{(vid, vd) =>
      Ant_VertexInfo(vd.id, vd.name, vd.v_type, -1L, Set(), (Array.empty, 0.0), mutable.Set())
    }.mapEdges{ e =>
      Ant_EdgeInfo(e.attr.id, e.attr.lam, e.attr.TC, e.attr.TS, e.attr.miu, e.attr.pher, chosen = false)
    }
    /*//把边信息交给源点管理（某节点管理其与后续节点形成的边）
    val edgeinfoInVertex_g = init_g.aggregateMessages[Map[VertexId, Ant_EdgeInfo]](
      ctx => ctx.sendToSrc(Map(ctx.dstId -> ctx.attr)),
      (a,b) => a ++ b
    )
    val new_g = init_g.joinVertices(edgeinfoInVertex_g)(
      (vid, oldv, newv) => {
        oldv.pher_chosen_Map ++= newv //
        oldv
      }
    )
    new_g
    */
    //Graph(init_g.vertices, init_g.edges)
    init_g
  }

  /**
   * 向所求的最好的路径中加入新路径，有多个就替换数值最大的那个(比较的是miu的求和，即第一个变量)
   * @param bestPaths 最好的路径Map，k是终节点id，v是MiuSum_LinkPath的集合（不止一条链路，靠length_max决定）
   * @param k 终结点id
   * @param v 一条链路，看是否能存入bestPaths
   * @param length_max 最多存几条链路
   */
  def addInBestPaths(bestPaths: Map[Long, ListBuffer[MiusumTimeused_LinkPathInfo]],
                     k: Long, v: MiusumTimeused_LinkPathInfo, length_max: Int, limit: Double): Unit = {
    if(!bestPaths.contains(k)) return
    val value = bestPaths.getOrElse(k, ListBuffer())
    if(value.length < length_max) {
      value.append(v)
    }
    else {
      //if better than bestPaths,update
      //就比较第一个统计量Miusum的值
      var max_index = 0
      var maxmiu = -1.0
      var timeused = -1.0
      for (i <- value.indices) {
        if (value(i)._1._1 > maxmiu) {
          max_index = i
          maxmiu = value(i)._1._1
          timeused = value(i)._1._2
        }
      }
      //解要优先满足限制条件：limit
      //如果被替换和替换的timeused都超时了，那么就timeused小的替换timeused大的
      if(timeused > limit){
        if(v._1._2 < timeused) {
          value.update(max_index, v)
        }
        return
      }
      //如果替换和被替换都不超时
      if(v._1._2 <= limit) {
        if (v._1._1 < maxmiu) {
          value.update(max_index, v)
        }
        if (v._1._1 == maxmiu) { //Miusum相等，则比较Timeused
          if (v._1._2 < timeused) //Timeused小的更新
            value.update(max_index, v)
        }
        return
      }
    }
  }

  /**
   * 对输入的节点对排队
   * @param input 输入的节点对(含有是否被选为LR)
   * @return 排列好的一条链路（srcid，dstid，是否选中）
   */
  def geneLinkedPath(input : Array[PathChosen]) : Array[PathChosen] = {
    val result = ListBuffer[PathChosen]()
    var searchResult : PathChosen = ((0,0),false)
    //找到起始节点，起点0L
    val inputbuff = input.to[ListBuffer]
    val source: mutable.Seq[PathChosen] = inputbuff.filter(_._1._1 == 0)
    if(source.length != 1){
      throw new RuntimeException("起点0L不存在或者不止一个[生成链路]")
    }else{
      searchResult = source.head
      do {
        result.append(searchResult)
        val next = searchResult._1._2
        inputbuff -= searchResult//减掉已经有的链路
        searchResult = search(inputbuff, next)
      }
      while(searchResult != ((-1L, -1L),false))
    }
    result.toArray
  }

  private def search(input : ListBuffer[PathChosen], searchN1 : Long) : PathChosen = {
    val source = input.filter(_._1._1 == searchN1)
    if(source.length != 1){
      ((-1L, -1L),false)
    }else{
      source.head
    }
  }

  //把结果写到本地
  def saveToLocal(outputs : Vector[Output]): Unit ={
    import java.io.{BufferedWriter, FileOutputStream, IOException, OutputStreamWriter}
    var out : BufferedWriter = null
    try {
      out = new BufferedWriter(
        new OutputStreamWriter(
          new FileOutputStream("./results.csv", true)))
      outputs.foreach(output => {
        out.write(output.toString() + "\r\n")
      })
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally try
      out.close()
    catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }

  //四舍五入
  def randoff(r : Double) : Int = {
    val a = math.round(r)
    a.toInt
  }

  //保留几位小数
  def doubleKeepNum(r:Double, keep:Int) : Double = {
    r.formatted(s"%.${keep}f").toDouble
  }

  //随机获取正负1
  private def getOneOrMinusone: Int ={
    val rand = randval(0, 1)
    val decimal = BigDecimal.apply(rand)
    //四舍五入
    val result = decimal.setScale(1,BigDecimal.RoundingMode.HALF_UP).doubleValue()
    if(result >= 0.5) 1
    else -1
  }

  def generateOneOrZero(pher : Double) : Int = {
    val p = randval(0,1)
    var base = pher
    if(pher > sigBound) base = sigBound
    else base = base * getOneOrMinusone
    val sig = 1/(1 + math.pow(math.E, -1 * base))
    if(p < sig) 1
    else 0
  }

  /**
   * 根据一条链路生成图
   * @param links 链路数组(顺序)
   * @param inputG 地图
   * @return 链路数组对应的图
   */
  def linksToGraph(links: Array[VertexId], inputG: Graph[Ant_VertexInfo, Ant_EdgeInfo]) : Graph[Ant_VertexInfo, Ant_EdgeInfo] ={
    val vidArray = links.reverse
    val pres : Map[Long,Long] = fromLinksToPreMap(vidArray)
    val new_g: Graph[Ant_VertexInfo, Ant_EdgeInfo] = vidArray.foldLeft(inputG)((inputG, vid) => {
      if (pres.getOrElse(vid, -1L) != -1L) { //前置节点可以获得
        val vd: VertexRDD[Ant_VertexInfo] = inputG.vertices.filter(vd => vd._1 == vid)
        //替换单点属性（更新全图）
        val newg = inputG.joinVertices(vd) {
          (vid, old, newv) =>
            Ant_VertexInfo(newv.id, newv.name, newv.v_type,
              pres.getOrElse(vid, -1L), //设preVid
              newv.backFromId, newv.pathFromPre,newv.sended
            )
        }
        newg
      } else {
        inputG
      }
    })
    new_g
  }

  //获取节点vid对应的前置节点的vid
  private def fromLinksToPreMap(links : Array[Long]):Map[Long,Long] = {
    if(links.length > 0){
      val key = links.head
      val left = links.tail
      if(left.length > 0){
        Map(key -> left.head) ++ fromLinksToPreMap(left)
      }else{
        Map.empty
      }
    }else{
      Map.empty
    }
  }
}
