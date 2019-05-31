package algorithms
import breeze.numerics._
import scala.io.Source
import scala.util.Random
import scala.util.control.Breaks


/**
 * Created by zyy on 7/12/17.
 */
//K-means||
class Kmeans_P_P{
  var qos = new Array[Double](57)
  var Label = -1
  var weight = -1.0
  //var flag:Boolean = false
  def setLabel(x:Int):Unit={
    this.Label = x
  }
  def getLabel = Label
  def setweight(x:Double):Unit={
    this.weight = x
  }
  def getweight = weight
  def setQos(x:Array[Double]):Unit={
    this.qos = x
  }
  def getQos() = qos
}

class Cluster_K_P_P (k: Int,r: Int,l:Int,max_it:Int) {

  def Clone_M_Z(source:List[Kmeans_P_P]):List[Kmeans_P_P]={
    var re:List[Kmeans_P_P]=List()
    for(s<-source){
      val temp:Kmeans_P_P=new Kmeans_P_P()
      temp.Label=s.Label
      temp.weight=s.weight
      temp.qos=s.qos.clone()
      re=re:::List(temp)
    }
    re
  }

  def get_Label(center: List[Kmeans_P_P], pt: Kmeans_P_P): Int = {
    var label = 0
    var min_dis = 1e20
    for(i <- center){
      val sqDis:Double = get_sqdis(i.qos,pt.qos)
      if (sqDis < min_dis) {
        val dis = get_distance(i.qos,pt.qos)
        if(dis < min_dis){
          min_dis = dis
          label = i.Label
        }
      }
    }
    label
  }

  def itetators(data: List[Kmeans_P_P], center: List[Kmeans_P_P]): (List[Kmeans_P_P], List[Kmeans_P_P]) = {
    var dataLabel:Set[Int]=Set()
    var centLabel:Set[Int]=Set()

    for(tmp <- data) {
      val label = get_Label(center, tmp)
      dataLabel=dataLabel++Set(label)
      tmp.setLabel(label)
    }

    for(i <- data){
      print(i.Label+",")
    }
    println("------------------Label------------")

    for(i <- center) {
      val tmp = data.filter(x => x.Label == i.Label)
      centLabel=centLabel++Set(i.Label)
      println("temp length:"+tmp.length)
      var sum:Array[Double] = new Array[Double](57)
      val newqos:Array[Double] = new Array[Double](57)

      if(tmp.length == 0){
        for(l <- 0 until 57){
          newqos(l) = i.qos(l)
        }
      }
      else {
        for(us <- tmp) {
          sum = get_add(us.qos,sum)
        }

        for(l <- 0 until 57){
          newqos(l) = sum(l)/tmp.length
        }
      }

      i.setQos(newqos.clone())
    }
    println("WJTdatalabel:")
    println(dataLabel)
    println("ZYYcentlabel:")
    println(centLabel)

    (data, center)
  }

  def get_intermedia(data: List[Kmeans_P_P]): List[Kmeans_P_P] = {
    val len = data.length
    var center:List[Kmeans_P_P] = List()
    //var flag:Int = 0
    val ii = (Math.random() * len).toInt
    center = center ::: List(data(ii))
    for(it <- 0 until r) {
      var pro: List[Double] = List()
      val dis_2: Double = get_Set2Set_distance(data, center)
      for (i <- 0 until len) {
        val dis_1: Double = l * get_PointSet_distance(data(i), center)
        val p: Double = dis_1 / dis_2
        pro = pro ::: List(p)
      }
      val s: Set[Int] = proSel(pro, l)
      println("s=" + s.size)
      for (t <- s) {
        println("every s = " + t)
        center = center ::: List(data(t))
      }
    }

    println("intermediate set:"+center.length)
    center
  } //get intermediate set

  def recluster(Intermediate: List[Kmeans_P_P],data:List[Kmeans_P_P],it :Int): List[Kmeans_P_P] = {
    val buf = get_weight(data,Intermediate)// with label and weight intermediate

    var ll:List[Double] = List()
    for(i <- buf){
      ll = ll ::: List(i.weight)
    }
    println("weight number:"+ll.distinct.length)

    val len = buf.length
    var center:List[Kmeans_P_P] = List()
    val ii = (Math.random() * len).toInt
    center = center ::: List(buf(ii))
    while(center.length < k){
      var pro:List[Double] = List()
      val dis_2 = get_Set2Set(buf,center)
      println("------")
      for (i<- 0 until buf.length){
        val dis_1 = get_PointSet(buf(i),center)
        val p:Double = dis_1 / dis_2
        pro = pro ::: List(p)
      }
      val s:Set[Int] = proSel(pro,1)
      println("s="+s.size)
      for(t <- s){
        center = center ::: List(buf(t))
      }
    }
    println("center length:"+center.length)

    //ADD Label
    var f:Int = 0
    for(i <- center){
      i.setLabel(f)
      for(j<- 0 until i.qos.length){
        print(i.qos(j)+",")
      }
      println("++++++++++++++")
      println("----"+i.Label)
      f = f + 1
    }

    for(i <- center){
      print(i.Label+",")
    }
    println("---last last center number:"+center.length)

    var da = (buf,center)
    for(i <- 0 until it) {//the number of itetator
      da = itetators(da._1, da._2)
    }

    println("========================")
    for(i<-da._2){
      print(i.weight+",")
    }
    println("========================")

    for(i <- da._2){
      print("zhongxin Label"+i.Label+",")
    }
    println("Lable----------------")


    da._2

    //    val demo = Clone_M_Z(buf)
    //    //val demo = Intermediate
    //    val len:Int = demo.length
    //    val len_qos = demo(0).qos.length
    //    val arr:Array[Double] = new Array[Double](len_qos)
    //
    //    for(i<-demo){
    //      for(j <- 0 until len_qos){
    //        arr(j) = i.qos(j)*i.weight
    //      }
    //      i.setQos(arr.clone())
    //    }
    //
    //    var center:List[Kmeans_P_P] = List()
    //    val i = (Math.random() * len).toInt
    //    center =  center ::: List(demo(i))
    //    while (center.length < k) {
    //      var pro:List[Double] = List()
    //      val dis_2 = get_Set2Set_distance(demo,center)
    //      println("------")
    //      for (i<- 0 until demo.length){
    //        val dis_1 = get_PointSet_distance(demo(i),center)
    //        val p:Double = dis_1 / dis_2
    //        pro = pro ::: List(p)
    //      }
    //      val s:Set[Int] = proSel(pro,1)
    //      println("s="+s.size)
    //      for(t <- s){
    //        center = center ::: List(demo(t))
    //      }
    //    }
    //    //    while (center.length < k) {
    //    //      var max_p:Double = 0
    //    //      for (i<- 0 until demo.length){
    //    //        val dis_1 = get_PointSet_distance(demo(i),center)
    //    //        val p :Double = dis_1
    //    //        if(p > max_p){
    //    //          max_p = p
    //    //          flag = i
    //    //        }
    //    //      }
    //    //      center = center ::: List(demo(flag))
    //    //    }
    //    println("last center number"+center.length)
    //
    //    //ADD Label
    //    var f:Int = 0
    //    for(i <- center){
    //      i.setLabel(f)
    //      f += 1
    //    }
    //
    //    var da = (demo, center)
    //    for(i <- 0 until max_it) {//the number of itetator
    //      //println("迭代:"+i)
    //      da = itetators(da._1, da._2)
    //    }
    //
    //    for(q <- da._2){
    //      for(i <- 0 until len_qos){
    //        q.qos(i) = q.qos(i)/q.weight
    //      }
    //    }
    //
    //    for(i <- da._2){
    //      print("zhongxin Label"+i.Label+",")
    //    }
    //    println("Lable----------------")
    //
    //    da._2
  }

  def train(data: List[Kmeans_P_P],center:List[Kmeans_P_P]): (List[Kmeans_P_P], List[Kmeans_P_P]) = {
    var da = (data, center)
//    var demo_cen:List[Kmeans_P_P] = Clone_M_Z(center)
//    var demo_dat:List[Kmeans_P_P] = Clone_M_Z(data)
//    var log:Int = 0
//    var flagwjt:Boolean=true
//    while(flagwjt){
//      var dist:Double = 0.0
//      da = itetators(da._1, da._2)
//      log = log + 1
//      //dist = get_subSingle(demo_cen,da._2)
//      //dist = get_sub(demo_cen,da._2)
//      val converged:Boolean = get_every(demo_cen,da._2,1e-657)
//      if(converged){
//        flagwjt = false
//        println("00000log000000---------"+log)
//      }
//      else if(log > max_it){
//        flagwjt = false
//        println("00000log000000---------"+log)
//      }
//      println("dist="+dist)
//      demo_cen = Clone_M_Z(da._2)
//      demo_dat = Clone_M_Z(da._1)
//    }
//        for(i <- 0 until max_it) {//the number of itetator
//          da = itetators(da._1, da._2)
//        }
//    da = itetators(da._1, da._2)

//    收敛性
    var demo_cen:List[Kmeans_P_P] = Clone_M_Z(center)
    var demo_dat:List[Kmeans_P_P] = Clone_M_Z(data)
    var log:Int = 0
    var flagwjt:Boolean=true
    while(flagwjt){
      var dist:Double = 0.0
      da = itetators(da._1, da._2)
      log = log + 1
      val converged:Boolean = get_every(demo_cen,da._2,1e-64)
      if(converged){
        flagwjt = false
        println("00000log000000---------"+log)
      }
      else if(log > max_it){
        flagwjt = false
        println("00000log000000---------"+log)
      }
      println("dist="+dist)
      demo_cen = Clone_M_Z(da._2)
      demo_dat = Clone_M_Z(da._1)
    }

    da
  }

  def get_PointSet_distance(pt:Kmeans_P_P, data: List[Kmeans_P_P]):Double={//x&C:shortest distance(Point:x & Set:C)
  var min_dis = 1e20
    for(i <-0 until data.length){
      val sqdis = get_sqdis(pt.qos,data(i).qos)
      if(sqdis < min_dis){
        val  dis = get_distance(pt.qos,data(i).qos)
        if(dis < min_dis){
          min_dis = dis
        }
      }
    }
    min_dis
  }
  def get_PointSet(pt:Kmeans_P_P, data: List[Kmeans_P_P]):Double={
    var min_dis = 1e20
//    for(i <-0 until data.length){
//      val dis = pow(pt.weight - data(i).weight,2)
//      if(dis < min_dis){
//        min_dis = dis
//      }
//    }
    for(i <-0 until data.length){
      val sqdis = pt.weight*get_sqdis(pt.qos,data(i).qos)
      if(sqdis < min_dis){
        val  dis = pt.weight*get_distance(pt.qos,data(i).qos)
        if(dis < min_dis){
          min_dis = dis
        }
      }
    }

    min_dis
  }

  def get_Set2Set_distance(data:List[Kmeans_P_P],center:List[Kmeans_P_P]):Double={
    var sum:Double = 0
    for(i <- 0 until data.length){
      val dis = get_PointSet_distance(data(i),center)
      sum += dis
    }
    sum
  }
  def get_Set2Set(data:List[Kmeans_P_P],center:List[Kmeans_P_P]):Double={
    var sum:Double = 0
    for(i <- 0 until data.length){
      val dis = get_PointSet(data(i),center)
      sum += dis
    }
    sum
  }

  def get_distance(a: Array[Double], b: Array[Double]): Double = {
    (for {x <- a.indices
          p = pow(a(x) - b(x), 2)} yield p).sum
  }
  def get_sqdis(a: Array[Double], b: Array[Double]): Double = {
    val a1:Double = math.sqrt((for {x <- a.indices
                                    p = pow(a(x), 2)} yield p).sum)
    val b1:Double = math.sqrt((for {x <- b.indices
                                    p = pow(b(x), 2)} yield p).sum)
    val ab:Double = math.pow(a1 - b1, 2)
    ab
  }

  def get_add(a: Array[Double], b: Array[Double]): Array[Double] ={
    val l:Int =a.length
    val ad = new Array[Double](l)
    for(i <- 0 until l){
      ad(i) = a(i) + b(i)
    }
    ad
  }

  def proSel(m:List[Double],l:Int):Set[Int]={
    val c=changeP(m)
    val re:Set[Int]=sel(c,l)
    re
  }

  def changeP(m:List[Double]):List[Double]={
    var c:List[Double]=List()
    c=c:::List(m(0))
    for(i<-1 until m.length){
      val tem:Double=c(i-1)+m(i)
      c=c:::List(tem)
    }

    c
  }

  def sel(c:List[Double],l:Int):Set[Int]={
    val ran=new Random()
    var dataIndex:Set[Int]=Set()
    for(i<-0 until l){
      val p=ran.nextDouble()*l
      dataIndex=dataIndex++Set(judge(c,p))
    }
    dataIndex
  }

  def judge(c:List[Double],p:Double):Int={
    var flag:Boolean=true
    var index=0
    while (flag){
      if (p<c(0))
        flag=false
      else if(p>=c(index)&&p<c(index+1)){
        flag=false
        index=index+1
      }
      else{
        index=index+1
        if(index>=c.length-1)
          flag=false
      }

    }
    index
  }

  def get_weight(data:List[Kmeans_P_P],intermediate:List[Kmeans_P_P]):List[Kmeans_P_P]={
    //ADD intermediate Label
    var flag:Int = 0
    for(i <- intermediate){
      i.setLabel(flag)
      flag +=1
    }

    val len = data.length
    for(tmp <- data) {
      val label = get_Label(intermediate, tmp)
      tmp.setLabel(label)
      // println("--"+label)
    }
    //val w:Array[Int] = new Array[Int](len)
    for(i <- intermediate){
      i.weight = data.filter(x => x.Label == i.Label).length.toDouble
      // i.weight = (data.filter(x => x.Label == i.Label).length)/len.toDouble
    }
    for(i <- intermediate){
      print(i.weight+",")
    }
    println("weight-------------")
    intermediate
  }

  def get_every(a:List[Kmeans_P_P],b:List[Kmeans_P_P],c:Double):Boolean = {
    var dist:Double = 0
    var flag:Boolean = true
    var converged:Boolean = false
    for(i <- 0 until a.length ){
      if(a(i).Label == b(i).Label){
        //dist = get_sqdis(a(i).qos,b(i).qos)
        dist = get_distance(a(i).qos,b(i).qos)
        println("Dist:"+dist)
      }
      if(dist > c){
        flag = false
      }
    }
    println("---------------------")

    if(flag) {
      converged = true
    }
    converged
  }

}

object Scala_K_PP {

  def loaddata(path: String): List[Kmeans_P_P] = {

    val file: Array[String] = Source.fromFile(path).getLines.toArray
    println("the size of the dataSet is:" + file.length)
    var demo: List[Kmeans_P_P] = List()

    for (line <- file) {
      val a = line.mkString.split(",") // use the kongge to split
      val b: Kmeans_P_P = new Kmeans_P_P
      for (i <- 0 until a.length) {
        b.qos(i) = a.apply(i).toDouble
      }
      //demo = b :: demo
      demo = demo ::: List(b)
    }
    demo
  }

//    def main(args: Array[String]) {
//
//      val train = loaddata("/home/mycluster/IdeaProjects/DataSet/dataset/WJTnormalSPM_N")
//      for(ii <- 0 until 6){
//        // val out1 = new java.io.FileWriter("/home/zyy/IdeaProjects/clustering/K2_inter_set.txt",false)
//        val out  = new java.io.FileWriter("/home/mycluster/IdeaProjects/clustering_new/K_P_P_S_Label"+ii+".txt",false)
//        // val length = train.length
//        val k:Int= 50
//        val r:Int = 5
//        val l:Int = k * 2
//        val y = new Cluster_K_P_P(k,r,l,200)
//        val res = y.get_intermedia(train)
//        val len :Int = res.length
//        println("intermediate length"+len)
//        val res1 = y.recluster(res,train,100)
//        val res2 = y.train(train,res1)
//        val result = res2._1
//        for(i <- result){
//          val lab:Int = i.Label
//          println(lab)
//          out.write(lab+"\n")
//        }
//        out.close()
//      }
//
//
//
//      //    for(i<-0 until len) {
//      //      for(j<- res(i).qos){
//      //        if(j != res(i).qos.last) out1.write(j+",")
//      //        else out1.write(j+"\n")
//      //      }
//      //    }
//      //out1.close()
//    }



//
  def mainTest(args: String):Double= {


    val train = loaddata("/home/mycluster/IdeaProjects/DataSet/dataset/cloud_normal")
    val length = train.length
    val k:Int= 20
    val r:Int = 5
    val l:Int = k * 2
    println(k)
    val y = new Cluster_K_P_P(k,r,l,100)
    val res = y.get_intermedia(train)
    val len :Int = res.length
    println("intermediate length"+len)
    val res1 = y.recluster(res,train,100)
    val res2 = y.train(train,res1)
    val result1 = res2._1 //data
    val result2 = res2._2 //centroid

    //    //CP
    //    var demo2: Double = 0.0
    //    for (i <- result2) {
    //      var numb: Int = 0
    //      var demo: Double = 0.0
    //      for (j <- result1) {
    //        if (j.Label == i.Label) {
    //          numb = numb + 1
    //          demo += get_distance57(i.qos, j.qos)
    //        }
    //      }
    //      demo2 += (demo / numb)
    //      println("clustering number=" + numb)
    //    }
    //    println("CP value = " + demo2)

    //    //SP
    //    val s: Int = result2.size
    //    var sp: Double = 0.0
    //    for (ii <- 0 until s) {
    //      for (jj <- (ii + 1) until s) {
    //        sp += get_distance57(result2(ii).qos, result2(jj).qos)
    //      }
    //    }
    //    sp = sp * 2 / (s * s - s)
    //    println("the SP =" + sp)


    //    var flag2:Double = 0.0
    //    for(i <- result2){
    //      var numb:Int=0
    //      var demo:Double = 0.0
    //      for(j <- result1){
    //        if(j.Label==i.Label) {
    //          numb=numb+1
    //          demo += get_distance2(i.qos,j.qos)
    //        }
    //      }
    //      flag2 += (demo/numb)
    //    }
    //    println("Cost value = "+flag2)
    //    flag2

    val sum: Double = get_Set2Set_distance2(result1, result2)/length
    println("clustering cost:" + sum)
    sum
  }

  def get_PointSet_distance2(pt:Kmeans_P_P, data: List[Kmeans_P_P]):Double={//x&C:shortest distance(Point:x & Set:C)
  var min_dis = 1e20
    for(i <-0 until data.length){
//      val dis = get_distance2(pt.qos,data(i).qos)
//      if(dis < min_dis){
//        min_dis = dis
//      }
      val sqdis = get_sqdis2(pt.qos,data(i).qos)
      if(sqdis < min_dis){
        val dis = get_distance2(pt.qos,data(i).qos)
        if(dis < min_dis){
          min_dis =dis
        }
      }
    }
    min_dis
  }

  def get_Set2Set_distance2(data:List[Kmeans_P_P],center:List[Kmeans_P_P]):Double={
    var sum:Double = 0.0
    for(i <- 0 until data.length){
      val dis:Double = get_PointSet_distance2(data(i),center)
      sum += dis
    }
    sum
  }

  def get_distance2(a: Array[Double], b: Array[Double]): Double = {
    (for {x <- a.indices
          p = pow(a(x) - b(x), 2)} yield p).sum
  }
  def get_sqdis2(a: Array[Double], b: Array[Double]): Double = {
    val a1:Double = math.sqrt((for {x <- a.indices
                                    p = pow(a(x), 2)} yield p).sum)
    val b1:Double = math.sqrt((for {x <- b.indices
                                    p = pow(b(x), 2)} yield p).sum)
    val ab:Double = math.pow(a1 - b1 ,2)
    ab
  }


  def main(args: Array[String]) {
    var sum:Double = 0.0
    var min:Double = 1e20
    val statT = System.currentTimeMillis()
    for(i<- 0 until 5){
      val res:Double = mainTest("start")
      println("res="+res)
      sum += res
      if(res < min){
        min =res
      }
    }
    val endT = System.currentTimeMillis()
    val ave_T = (endT - statT)/5.0
    val average:Double = sum/5.0
    println("last sum="+sum+"min="+min+"average="+average+"average time ="+ave_T)

  }

}
