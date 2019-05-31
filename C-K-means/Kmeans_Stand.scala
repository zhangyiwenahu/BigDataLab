/**
 * Created by zyy on 7/9/17.
 */
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

//standard K-means
class Kmeans_S{
  var qos = new Array[Double](10)
  //Enter the dimension of the dataset
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

class Cluster_KS(max_it: Int, k: Int) {

  def Clone_MZ(source:List[Kmeans_S]):List[Kmeans_S]={
    var re:List[Kmeans_S]=List()
    for(s<-source){
      val temp:Kmeans_S=new Kmeans_S()
      temp.Label=s.Label
      temp.qos=s.qos.clone()
      re=re:::List(temp)
    }
    re
  }

  def get_Label(center: List[Kmeans_S], pt: Kmeans_S): Int = {
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

  def itetators(data: List[Kmeans_S], center: List[Kmeans_S]): (List[Kmeans_S], List[Kmeans_S]) = {
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


//  def train(data: Array[User]): (Array[User], Array[User]) = {
//    val center = new Array[User](k)
//    for(i <- 0 until k) {
//      center(i) = data(i)
//    }
//    var da = (data, center)
//    for(i <- 0 until max_it) {//the number of itetator
//      //println("迭代:"+i)
//      da = itetators(da._1, da._2)
//    }
//    da
//  }

  def train(data: List[Kmeans_S]): (List[Kmeans_S], List[Kmeans_S]) = {
    var flag:Int = 0
    //随机获得k个中心
    var center:List[Kmeans_S] = List()
    val len = data.length
    for(i<- 0 until k){
      val rand:Int = (Math.random() * len).toInt
      center = center ::: List(data(rand))
    }

    //ADD Label
    for(ii <- center){
      ii.setLabel(flag)
      flag += 1
    }

    for(i <- center){
      print(i.getLabel+",")
    }
    println("last last center number"+center.length)


    //    var da = (data, center)
//    for(i <- 0 until max_it) {//the number of iterator
//      da = itetators(da._1, da._2)
//    }
//    收敛性
    var da = (data, center)
    var demo_cen:List[Kmeans_S] = Clone_MZ(center)
    var demo_dat:List[Kmeans_S] = Clone_MZ(data)

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
      demo_cen = Clone_MZ(da._2)
      demo_dat = Clone_MZ(da._1)
    }
    da
  }

  def get_every(a:List[Kmeans_S],b:List[Kmeans_S],c:Double):Boolean = {
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

  def get_distance(a: Array[Double], b: Array[Double]): Double = {
    (for {x <- a.indices
          p = pow(a(x) - b(x), 2)} yield p).sum
  }
  def get_sqdis(a: Array[Double], b: Array[Double]): Double = {
    val a1:Double = math.sqrt((for {x <- a.indices
                                    p = pow(a(x), 2)} yield p).sum)
    val b1:Double = math.sqrt((for {x <- b.indices
                                    p = pow(b(x), 2)} yield p).sum)
    val ab:Double = math.pow(a1 - b1,2)
    //println("0000001111:"+ab)
    //math.sqrt(ab)
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


}




object Kmeans_Stand {

  def loaddata(path: String):List[Kmeans_S] = {

    val file:Array[String] = Source.fromFile(path).getLines.toArray
    println("the size of the dataSet is:"+file.length)
    var demo:List[Kmeans_S] = List()

    for(line<-file) {
      val a = line.mkString.split(",")// use the kongge to split
      val b:Kmeans_S = new Kmeans_S
      val len:Int = a.length
      for(i <- 0 until len){
        b.qos(i) = a.apply(i).toDouble
      }
      demo = demo ::: List(b)
    }
    demo
  }


  def main(args: Array[String]) {
    val train = loaddata("/home/mycluster/IdeaProjects/DataSet/dataset/cloud_normal")
    val out = new java.io.FileWriter("/home/mycluster/IdeaProjects/clustering_new/KS_C_Label.txt",false)
    val length = train.length
    val k= Math.min(length,4)
    println(k)
    val y = new Cluster_KS(200, k)
    val res = y.train(train)

    val result = res._1
    for(i<-0 until result.length) {
      val label:Int = result(i).getLabel
      println(label)
      out.write(label+"\n")
    }

    for(i <- res._2){
      val num_num:Int = result.filter(x => x.getLabel == i.getLabel).length
      println("num_num:="+num_num)
    }



    out.close()

  }

//  def mainTest(args: String):Double= {
//
//
//    val train = loaddata("/home/mycluster/IdeaProjects/DataSet/dataset/cloud_normal")
//    val length = train.length
//    val k = Math.min(length,4)
//    println(k)
//    val y = new Cluster_KS(200, k)
//    val res = y.train(train)
//
//    val result1 = res._1 //data
//    val result2 = res._2 //centroid
//    //    val cen:Kmeans_Plus = new Kmeans_Plus()
//    //    var sum1:Array[Double] = new Array[Double](10)
//    //    for(te <- result2){
//    //      sum1 = get_add2(te.qos,sum1)
//    //    }
//    //    val newqos = new Array[Double](10)
//    //    for(l <- 0 until 10){
//    //      newqos(l) = sum1(l)/result2.length
//    //    }
//    //    cen.setQos(newqos)
//    //    var potential:Double = 0.0
//    //    for(i <- result2){
//    //      potential += get_distance2(i.qos,cen.qos)
//    //    }
//    //    println("expection="+2*potential)
//
//
//    //
//    //    var numb:Int=0
//    //    for(i <- result2){
//    //      for(j <- result1){
//    //        if(j.Label==i.Label)
//    //          numb=numb+1
//    //      }
//    //      println("clustering number="+numb)
//    //    }
//
//
//    //    //CP
//    //    var demo2: Double = 0.0
//    //    for (i <- result2) {
//    //      var numb: Int = 0
//    //      var demo: Double = 0.0
//    //      for (j <- result1) {
//    //        if (j.Label == i.Label) {
//    //          numb = numb + 1
//    //          demo += get_distance10(i.qos, j.qos)
//    //        }
//    //      }
//    //      demo2 += (demo / numb)
//    //      println("clustering number=" + numb)
//    //    }
//    //    println("CP value = " + demo2)
//
//    //    //SP
//    //    val s: Int = result2.size
//    //    var sp: Double = 0.0
//    //    for (ii <- 0 until s) {
//    //      for (jj <- (ii + 1) until s) {
//    //        sp += get_distance10(result2(ii).qos, result2(jj).qos)
//    //      }
//    //    }
//    //    sp = sp * 2 / (s * s - s)
//    //    println("the SP =" + sp)
//
//
//    //        var flag2:Double = 0.0
//    //        for(i <- result2){
//    //          var numb:Int=0
//    //          var demo:Double = 0.0
//    //          for(j <- result1){
//    //            if(j.Label==i.Label) {
//    //              numb=numb+1
//    //              demo += get_distance2(i.qos,j.qos)
//    //            }
//    //          }
//    //          flag2 += (demo/numb)
//    //        }
//    //        println("Cost value = "+flag2)
//    //        flag2
//
////    val sum: Double = get_Set2Set_distance2(result1, result2) / length
//    val sum: Double = get_Set2Set_distance2(result1, result2) / length
//    println("clustering cost:" + sum)
//    sum
//  }

  def get_PointSet_distance2(pt:Kmeans_S, data: List[Kmeans_S]):Double={//x&C:shortest distance(Point:x & Set:C)
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

  def get_Set2Set_distance2(data:List[Kmeans_S],center:List[Kmeans_S]):Double={
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

//  def main(args: Array[String]) {
//    var sum:Double = 0.0
//    var min:Double = 1e20
//    val statT = System.currentTimeMillis()
//    for(i<- 0 until 5){
//      val res:Double = mainTest("start")
//      println("res="+res)
//      sum += res
//      if(res < min){
//        min =res
//      }
//    }
//    val endT = System.currentTimeMillis()
//    val ave_T = (endT - statT)/5.0
//    val average:Double = sum/5.0
//    println("last sum="+sum+"min="+min+"average="+average+"average time ="+ave_T)
//
//  }


}
