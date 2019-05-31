import breeze.linalg.sum
import breeze.numerics._
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}


import scala.collection.mutable.{ArrayBuffer, Map}

import scala.io.Source
import scala.reflect.io.Path

/**
 * Created by zyy on 7/6/17.
 */
//The combination of improved covering algorithm and k-means algoritm
class F_KP{
  var qos = new Array[Double](10)
  var Label = -1
  def setLabel(x:Int):Unit={
    this.Label = x
  }
  def getLabel = Label
  def setQos(x:Array[Double]):Unit={
    this.qos = x
  }
  def getQos() = qos
}


class Cluster_F_KP(max_it: Int) {


  def Clone_F_K(source:List[F_KP]):List[F_KP]={
    var re:List[F_KP]=List()
    for(s<-source){
      val temp:F_KP=new F_KP()
      temp.Label=s.Label
      temp.qos=s.qos.clone()
      re=re:::List(temp)
    }
    re
  }

  def get_Label(center: List[F_KP], pt: F_KP): Int = {
    var label = 0
    var min_dis = 1e50
//    for (i <- 0 until center.length) {
//      val dis:Double = get_distance(center(i).qos,pt.qos)
//      if (dis < min_dis) {
//        min_dis = dis
//        label = i
//      }
//    }
//    for(i <- center){
//      val dis:Double = get_distance(i.qos,pt.qos)
//      if (dis <= min_dis) {
//        min_dis = dis
//        label = i.Label
//      }
//    }
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

  def iterators(data: List[F_KP], center: List[F_KP]): (List[F_KP], List[F_KP]) = {
    var dataLabel:Set[Int]=Set()
    var centLabel:Set[Int]=Set()

    for(tmp <- data) {
      val label = get_Label(center, tmp)
      dataLabel=dataLabel++Set(label)
      tmp.setLabel(label)
    }

//    for(i <- data){
//      print(i.Label+",")
//    }
//    println("------------------Label------------")

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

  def train(data: List[F_KP]): (List[F_KP], List[F_KP]) = {

    val flag:Array[Array[Double]]=Source.fromFile("/home/mycluster/IdeaProjects/clustering_new/Center_Fugai.txt").getLines().map(x=>x.split(',').map(x=>x.toDouble)).toArray
    var center:List[F_KP] = List()

    var num:Int = 0

    for(i <- flag){
      val f:F_KP = new F_KP
//      for(j <- 0 until i.length){
//        print(i(j)+",")
//      }
//      println("++++++++++++++++++++++++")

       f.setQos(i.clone())
       f.setLabel(num)

//      for(j <- 0 until i.length){
//        print(f.qos(j)+",")
//      }
//      println("------------------------------")

//      for(j <- 0 until i.length){
//        print(f.Label+",")
//      }
//      println("------------------------------")

      center = center ::: List(f)
      num += 1
    }

//    找到距离重心最近的数据点作为初始圆心
//    for(i <- center){
//      val arr = get_PointSet_qos(i,data)
//      i.setQos(arr)
//    }

//    for(i <- center){
//      for(j <- 0 until i.qos.length){
//        print(i.qos(j)+",")
//      }
//      println("++++++++++++++++++++++++")
//    }
    println("------center.length="+center.length)

//    var da = (data, center)
//    for(i <- 0 until max_it) {//the number of itetator
//      da = iterators(da._1, da._2)
//    }

    var da = (data, center)
    var demo_cen:List[F_KP] = Clone_F_K(center)
    var demo_dat:List[F_KP] = Clone_F_K(data)

    var log:Int = 0
    var flagwjt:Boolean=true

    while(flagwjt){
      da = iterators(da._1, da._2)
      log = log + 1
      println("--log--:"+log)
      val converged:Boolean = get_every(demo_cen,da._2,1e-6)
      if(converged){
        flagwjt = false
        println("00000log000000---------"+log)
      }
      else if(log > max_it){
        flagwjt = false
        println("00000log000000---------"+log)
      }
      demo_cen = Clone_F_K(da._2)
      demo_dat = Clone_F_K(da._1)
    }

//      da = iterators(da._1, da._2)
//
//    var demo_cen:List[F_KP] = Clone_F_K(center)
//    var demo_dat:List[F_KP] = Clone_F_K(data)
////    //测试中心
////    for(j <- demo_cen){
////      for(i <- j.qos){
////        print(i+",")
////      }
////      println("111111111111111111")
////    }
////    println("111111111111111111")
//
//
//    var log:Int = 0
//    var flagwjt:Boolean=true
//    while(flagwjt){
//      var dist:Double = 0.0
//      da = itetators(da._1, da._2)
//
//      //println("110110 Label:"+da._1(110110).Label)
//
////      //测试中心
////      for(j <- da._2){
////        for(i <- j.qos){
////          print(i+",")
////        }
////        println("111111111111111111")
////      }
////      println("111111111111111111")
//
////      for(i <- da._1){
////        print(i.Label+",")
////      }
////      println("-----------label-------------------")
//      log = log + 1
//      println("log:"+log)
//      //dist = get_sub(demo_cen,da._2)
//      //dist = get_subSingle(demo_cen,da._2)
//      val converged:Boolean = get_every(demo_cen, da._2,1e0log)
//      if(converged){
//        flagwjt = false
//        println("00000log000000---------"+log)
//      }
//      else if(log > max_it){
//        flagwjt = false
//        println("00000log000000---------"+log)
//      }
//      println("dist="+dist)
//      demo_cen = Clone_F_K(da._2)
//      demo_dat = Clone_F_K(da._1)
//    }

    da
  }

  def get_PointSet_distance(pt:F_KP, data: List[F_KP]):Double={//x&C:shortest distance(Point:x & Set:C)
  var min_dis = 1e20
    for(i <-0 until data.length){
      val sqdis = get_sqdis(pt.qos,data(i).qos)
      //val dis = get_sqdis(pt.qos,data(i).qos)
      if(sqdis < min_dis){
        val dis = get_distance(pt.qos,data(i).qos)
        if(dis < min_dis){
          min_dis = dis
        }
      }
     // println(dis )
    }
    min_dis
  }
  def get_PointSet_qos(pt:F_KP, data: List[F_KP]):Array[Double]={//x&C:shortest distance(Point:x & Set:C)
    var min_dis = 1e20
    var flag = 0
    for(i <-0 until data.length){
      val sqdis = get_sqdis(pt.qos,data(i).qos)
      if(sqdis < min_dis){
        val dis = get_distance(pt.qos,data(i).qos)
        if(dis < min_dis){
          min_dis = dis
          flag = i
        }
      }
      //
//       val dis = get_distance(pt.qos,data(i).qos)
//      if(dis < min_dis){
//        min_dis = dis
//      }
      //

    }
    data(flag).qos
  }

  def get_Set2Set_distance(data:List[F_KP],center:List[F_KP]):Double={
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

  def get_every(a:List[F_KP],b:List[F_KP],c:Double):Boolean = {
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




object Fugai_KP {

  def loaddata(path: String):List[F_KP] = {

    val file:Array[String] = Source.fromFile(path).getLines.toArray
    println("the size of the dataSet is:"+file.length)
    var demo:List[F_KP] = List()

    for(line<-file) {
      val a = line.mkString.split(",").map(x => x.toDouble)// use the kongge to split
      val b:F_KP = new F_KP()
      val len:Int = a.length
      for(i <- 0 until len) {
        b.qos(i) = a.apply(i).toDouble
      }
     // b.setQos(a)
      demo = demo ::: List(b)
    }
    demo
  }

//  def main(args: Array[String]) {
//    val train = loaddata("/home/mycluster/IdeaProjects/DataSet/dataset/gauss_normal")//gauss_normal
//    val out = new java.io.FileWriter("/home/mycluster/IdeaProjects/clustering_new/F_KP_GLabel.txt",false)
//    val length = train.length
//    // val k= Math.min(length,10)
//    //println(k)
//    //val y = new Cluster_KP(10, k)
//    val y = new Cluster_F_KP(100)
//    val res = y.train(train)
//
//    val r = res._2
//    for(i <- r){
//      for(j<-0 until i.qos.length){
//        print(i.qos(j)+",")
//      }
//      println("huanhang")
//    }
//
//    val result = res._1
//    for(i<-0 until result.length) {
//      val label:Int = result(i).getLabel
//      println(label)
//      out.write(label+"\n")
//    }
//    out.close()
//
//  }

  def mainTest(args: String):Double= {

    val train = loaddata("/home/mycluster/IdeaProjects/DataSet/dataset/cloud_normal")
    val out = new java.io.FileWriter("/home/mycluster/IdeaProjects/clustering_new/F_KP_CLabel.txt",false)

    val length = train.length
    val y = new Cluster_F_KP(100)
    val res = y.train(train)

    val result1 = res._1//data
    val result2 = res._2//centroid

        for(i<-0 until result1.length) {
          val label:Int = result1(i).getLabel
          //println(label)
          out.write(label+"\n")
        }
        out.close()

    for(i <- result1){
      println("Label="+i.Label)
    }

   // var numb:Int=0

//    //CP
//    var demo2:Double = 0.0
//    for(i <- result2){
//      var numb:Int=0
//      var demo:Double = 0.0
//      for(j <- result1){
//        if(j.Label==i.Label) {
//          numb=numb+1
//          demo += get_distance10(i.qos,j.qos)
//        }
//      }
//      demo2 += (demo/numb)
//      println("clustering number="+numb)
//    }
//    println("CP value = "+demo2)

//    //SP
//    val s:Int = result2.size
//    var sp:Double = 0.0
//    for(ii <- 0 until s){
//      for(jj <- (ii+1) until s){
//        sp += get_distance10(result2(ii).qos,result2(jj)qos)
//      }
//    }
//    sp = sp*2/(s*s-s)
//    println("the SP ="+sp)

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

    val sum:Double = get_Set2Set_distance2(result1,result2) / length
    println("clustering cost:"+sum)
    sum

  }


  def get_PointSet_distance2(pt:F_KP, data: List[F_KP]):Double={//x&C:shortest distance(Point:x & Set:C)
  var min_dis = 1e20
    for(i <-0 until data.length){
//      val dis = get_distance2(pt.qos,data(i).qos)
//     // val dis = get_sqdis2(pt.qos,data(i).qos)
//      if(dis < min_dis){
//        min_dis = dis
//      }
      val sqdis = get_sqdis2(pt.qos,data(i).qos)
      if(sqdis < min_dis){
        val  dis = get_distance2(pt.qos,data(i).qos)
        if(dis < min_dis){
          min_dis = dis
        }
      }
    }
    min_dis
  }

  def get_Set2Set_distance2(data:List[F_KP],center:List[F_KP]):Double={
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
    for(i<- 0 until 1){
      val res:Double = mainTest("start")
      println("res="+res)
      sum += res
      if(res < min){
        min =res
      }
    }
    val endT = System.currentTimeMillis()
    val ave_T = (endT - statT)/1.0
    val average:Double = sum/1.0
    println("last sum="+sum+"min="+min+"avege="+average+"average time ="+ave_T)

  }


}
