import breeze.linalg.sum
import breeze.numerics._
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}


import scala.collection.mutable.{ArrayBuffer, Map}

import scala.io.Source
import scala.reflect.io.Path
import scala.util.Random

/**
 * Created by zyy on 7/6/17.
 */
//K-mean++
class Kmeans_Plus{
  var qos = new Array[Double](10)
  var Label = -1
  //var flag:Boolean = false
  def setLabel(x:Int):Unit={
    this.Label = x
  }
  def getLabel = Label
  def setQos(x:Array[Double]):Unit={
    this.qos = x
  }
  def getQos() = qos
}


class Cluster_KP(max_it: Int, k: Int) {

  def Clone_M_Z(source:List[Kmeans_Plus]):List[Kmeans_Plus]={
    var re:List[Kmeans_Plus]=List()
    for(s<-source){
      val temp:Kmeans_Plus=new Kmeans_Plus()
      temp.Label=s.Label
      temp.qos=s.qos.clone()
      re=re:::List(temp)
    }
    re
  }

  def get_Label(center: List[Kmeans_Plus], pt: Kmeans_Plus): Int = {
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

  def itetators(data: List[Kmeans_Plus], center: List[Kmeans_Plus]): (List[Kmeans_Plus], List[Kmeans_Plus]) = {
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
      var sum:Array[Double] = new Array[Double](10)
      val newqos:Array[Double] = new Array[Double](10)

      if(tmp.length == 0){
        for(l <- 0 until 10){
          newqos(l) = i.qos(l)
        }
      }
      else {
        for(us <- tmp) {
          sum = get_add(us.qos,sum)
        }

        for(l <- 0 until 10){
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

  def train(data: List[Kmeans_Plus]): (List[Kmeans_Plus], List[Kmeans_Plus]) = {

    var center:List[Kmeans_Plus] = List()
    var flag:Int = 0
    val len = data.length
    val i = (Math.random() * len).toInt
    center =  center ::: List(data(i))
    while (center.length < k) {
      var pro:List[Double] = List()
      val dis_2 = get_Set2Set_distance(data,center)
      println("------")
      for (i<- 0 until len){
        val dis_1 = get_PointSet_distance(data(i),center)
        // println(dis_1, dis_2)
        val p:Double = dis_1 / dis_2
        pro = pro ::: List(p)
      }
      val s:Set[Int] = proSel(pro,1)
      println("s="+s.size)
      for(t <- s){
        center = center ::: List(data(t))
      }
    }

    //ADD Label
    for(i <- center){
      i.setLabel(flag)
      flag +=1
    }

    for(i <- center){
      print(i.getLabel+",")
    }
    println("last last center number"+center.length)


//    var da = (data, center)
//    for(i <- 0 until max_it) {//the number of itetator
//      da = itetators(da._1, da._2)
//    }

//    var da = (data, center)
//    da = itetators(da._1, da._2)


//    //收敛性
    var da = (data, center)
    var demo_cen:List[Kmeans_Plus] = Clone_M_Z(center)
    var demo_dat:List[Kmeans_Plus] = Clone_M_Z(data)
    var log:Int = 0
    var flagwjt:Boolean=true
    while(flagwjt){
      da = itetators(da._1, da._2)
      log = log + 1
      val converged:Boolean = get_every(demo_cen,da._2,1e-6)
      if(converged){
        flagwjt = false
        println("00000log000000---------"+log)
      }
      else if(log > max_it){
        flagwjt = false
        println("00000log000000---------"+log)
      }
      demo_cen = Clone_M_Z(da._2)
      demo_dat = Clone_M_Z(da._1)
    }

    for(eg<-center){
      val data_num = data.filter(x => x.Label == eg.Label).length
      print(data_num+",")
    }
    println("---------------------------------------------------------")

    da
  }

  def get_PointSet_distance(pt:Kmeans_Plus, data: List[Kmeans_Plus]):Double={//x&C:shortest distance(Point:x & Set:C)
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

  def get_Set2Set_distance(data:List[Kmeans_Plus],center:List[Kmeans_Plus]):Double={
    var sum:Double = 0
    for(i <- 0 until data.length){
      val dis = get_PointSet_distance(data(i),center)
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
    val ab:Double = math.pow(a1 - b1 ,2)
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

  def get_every(a:List[Kmeans_Plus],b:List[Kmeans_Plus],c:Double):Boolean = {
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

object Kmeans_P {
  def loaddata(path: String): List[Kmeans_Plus] = {

    val file: Array[String] = Source.fromFile(path).getLines.toArray
    println("the size of the dataSet is:" + file.length)
    var demo: List[Kmeans_Plus] = List()
    for (line <- file) {
      val a = line.mkString.split(",") // use the kongge to split
      val b: Kmeans_Plus = new Kmeans_Plus()
      val len:Int = a.length
      for (i <- 0 until len) {
        b.qos(i) = a.apply(i).toDouble
      }
      demo = demo ::: List(b)
    }
    demo
  }

//    def main(args: Array[String]):Unit = {
//      val train = loaddata("/home/mycluster/IdeaProjects/DataSet/dataset/gauss_normal")
//      val out = new java.io.FileWriter("/home/mycluster/IdeaProjects/clustering_new/K_P_G_label.txt", false)
//      val length = train.length
//      val k = Math.min(length,10)
//      println(k)
//      val y = new Cluster_KP(200, k)
//      val res = y.train(train)
//
//      val result = res._1
//      for (i <- result) {
//        val label: Int = i.Label
//        println(label)
//        out.write(label + "\n")
//      }
//
//      out.close()
//
//    }

//
  def mainTest(args: String):Double= {


    val train = loaddata("/home/mycluster/IdeaProjects/DataSet/dataset/cloud_normal")
    val length = train.length
    val k = Math.min(length,50)
    println(k)
    val y = new Cluster_KP(200, k)
    val res = y.train(train)

    val result1 = res._1 //data
    val result2 = res._2 //ce0ntroid
    //    val cen:Kmeans_Plus = new Kmeans_Plus()
    //    var sum1:Array[Double] = new Array[Double](10)
    //    for(te <- result2){
    //      sum1 = get_add2(te.qos,sum1)
    //    }
    //    val newqos = new Array[Double](10)
    //    for(l <- 0 until 10){
    //      newqos(l) = sum1(l)/result2.length
    //    }
    //    cen.setQos(newqos)
    //    var potential:Double = 0.0
    //    for(i <- result2){
    //      potential += get_distance2(i.qos,cen.qos)
    //    }
    //    println("expection="+2*potential)


    //
    //    var numb:Int=0
    //    for(i <- result2){
    //      for(j <- result1){
    //        if(j.Label==i.Label)
    //          numb=numb+1
    //      }
    //      println("clustering number="+numb)
    //    }


    //    //CP
    //    var demo2: Double = 0.0
    //    for (i <- result2) {
    //      var numb: Int = 0
    //      var demo: Double = 0.0
    //      for (j <- result1) {
    //        if (j.Label == i.Label) {
    //          numb = numb + 1
    //          demo += get_distance10(i.qos, j.qos)
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
    //        sp += get_distance10(result2(ii).qos, result2(jj).qos)
    //      }
    //    }
    //    sp = sp * 2 / (s * s - s)
    //    println("the SP =" + sp)


    //        var flag2:Double = 0.0
    //        for(i <- result2){
    //          var numb:Int=0
    //          var demo:Double = 0.0
    //          for(j <- result1){
    //            if(j.Label==i.Label) {
    //              numb=numb+1
    //              demo += get_distance2(i.qos,j.qos)
    //            }
    //          }
    //          flag2 += (demo/numb)
    //        }
    //        println("Cost value = "+flag2)
    //        flag2

    val sum: Double = get_Set2Set_distance2(result1, result2) / length
    println("clustering cost:" + sum)
    sum
  }

  def get_PointSet_distance2(pt:Kmeans_Plus, data: List[Kmeans_Plus]):Double={//x&C:shortest distance(Point:x & Set:C)
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

  def get_Set2Set_distance2(data:List[Kmeans_Plus],center:List[Kmeans_Plus]):Double={
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
  def get_distance10(a: Array[Double], b: Array[Double]): Double = {
    sqrt((for {x <- a.indices
               p = pow(a(x) - b(x), 2)} yield p).sum)
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












