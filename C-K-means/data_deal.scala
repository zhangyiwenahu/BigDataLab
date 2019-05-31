package algorithms

import breeze.numerics._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

import scala.io.Source

/**
 * Created by zyy on 8/3/17.
 */
object data_deal {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("covering").setMaster("local[*]")
    val sc = new SparkContext(conf)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.Server").setLevel(Level.OFF)
    val data: Array[Array[Double]] = Source.fromFile("/home/zyy/IdeaProjects/clustering/src/wine_data.txt").getLines().map(x => x.split(',').map(x => x.toDouble)).toArray
    val out = new java.io.FileWriter("/home/zyy/IdeaProjects/clustering/wine_label.txt", false)

    val l = data.length
    val ll = data.head.length
    println("Array length=" + ll)
    //val max_P: Double = max_Point(data)
    //println("max="+max_P)
    //val data0:Array[Array[Double]] = wjtArrArrD(data,max_P)
    for(i <- data){
//      for(j <- 1 until ll){
//        if(j != (ll-1)){
//          out.write(i(j)+",")
//        }
//        else out.write(i(j)+"\n")
//      }
      out.write(i(0).toInt+"\n")
    }
    out.close()

//    val demoWjt:Array[Array[Double]] = Array(Array(1,2,3,4),Array(1,1,1,1),Array(5,2,3,4))
//    val max:Double= max_Point(demoWjt)
//    val re=wjtArrArrD(demoWjt,max)
//    for(l<-re){
//      for(v<-l)
//        print(v+"\t")
//      println()
//    }


    //    val buf: Array[(Long, (Array[Double], Int))] = new Array[(Long, (Array[Double], Int))](l)
//    for (i <- 0 until l) {
//      buf(i) = (i.toLong, (data0(i), -1))
//    }

  }


  def max_Point(data:Array[Array[Double]]):Double={
    var max: Double = 0
    for(i <- data){
     val temp :Double = get_distance2(i)
      if(max < temp){
        max = temp
      }
    }
    max
  }

  def get_distance2(a: Array[Double]): Double = {
    (for {x <- a.indices
          p = pow(a(x), 2)} yield p).sum
  }

  def wjtArrArrD(data:Array[Array[Double]],max_P:Double):Array[Array[Double]]={
    val subLen:Int=data.head.length
    val re:Array[Array[Double]]=new Array[Array[Double]](data.length)
    for(i<-0 until data.length){
      val temp:Array[Double]=new Array[Double](subLen+1)
      for(j<-0 until subLen)
        temp(j)=data(i)(j)
      temp(subLen) = sqrt(max_P - get_distance2(data(i)))
      re(i)=temp
    }
    re
  }


}
