/**
 * Created by zyy on 8/1/17.
 */
package algorithms
import breeze.numerics._
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.Map
import scala.io.Source
import scala.util.Random
import breeze.numerics.{abs, sqrt, pow}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object Test_Covering_Stand {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("covering").setMaster("local[*]")
    val sc = new SparkContext(conf)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.Server").setLevel(Level.OFF)
    val data: Array[Array[Double]] = Source.fromFile("/home/zyy/IdeaProjects/clustering/src/kdd10%.txt").getLines().map(x => x.split(' ').map(x => x.toDouble)).toArray
    val l = data.length
    val buf: Array[(Long, (Array[Double], Int))] = new Array[(Long, (Array[Double], Int))](l)
    for (i <- 0 until l) {
      buf(i) = (i.toLong, (data(i), -1))
    }// Add Index
    val out = new java.io.FileWriter("/home/zyy/IdeaProjects/clustering/F_KDD10_label.txt", false)
    //val startTime= System.nanoTime()
    val result = process(IndexedRDD(sc.parallelize(buf)).cache())
    val L: Int = result.map(x => x._1).collect().distinct.length
    println(L + "class number")
    val temp:Array[(Int,Long)] = result.sortBy(x => x._2).collect()
    for (i <- 0 until l) {
      out.write(temp(i)._1 + "\n")
    }

    out.close()
    sc.stop()
  }

  def process(train: IndexedRDD[Long, (Array[Double], Int)]): RDD[(Int, Long)] = {
    val out0 = new java.io.FileWriter("/home/zyy/IdeaProjects/clustering/Center_KDD_Fugai.txt", false)
    var pre = train
    var i = 0
    val R: Double = 0.3
    var center: (Long, Array[Double]) = null
    var center_l = Map[Int,Array[Double]]()


    while (!(pre.filter(x => x._2._2 == -1)).isEmpty()) {
      println("center")
      var radius: Double = 0.0
      val data1 = pre.filter(x => x._2._2 == -1).map(x => (x._1, x._2._1)) //uncovered data
      println("data1.count() = " + data1.count())

      if (center == null) {
        center = get_center(data1)
        //radius = get_radius_first(data1, center)
        //val cc = get_centroid(data1)
        radius = get_radius_first(data1, center)// Get first radius
      }
      else {
        val c = center._2
        center = data1.map(x => (x, get_distance(x._2, c))).sortBy(x => x._2, ascending = false).first()._1
        radius = get_radius(data1, center)
      }
      //center_l += (i -> center._2)
      println("radius")
      //radius = get_radius(data1, center)
      var covering0 = get_covering(data1, center, radius).cache()

      // C = covering0.map(x => x._1).collect().toSet

      //var CC: Int = covering0.count().toInt
      var CC: Int = 0
      var temp00: Int = 0

      if (i == 0) {
        center = get_centroid(covering0)
        radius = get_radius_first(covering0, center)
        covering0 = get_covering(data1, center, radius).cache()
        CC = covering0.count().toInt
        while ((CC - temp00) > 0) {
          temp00 = CC
          println("temp=" + CC)
          CC = 0
          println("C=" + CC)
          center = get_centroid(covering0)
          radius = get_radius_first(covering0, center)
          covering0 = get_covering(data1, center, radius).cache()
          CC = covering0.count().toInt
          println("New_C=" + CC + "---" + (CC - temp00))
        }
      }
      else {
        center = get_centroid(covering0)
        radius = get_radius(covering0, center)
        covering0 = get_covering(data1, center, radius).cache()
        CC = covering0.count().toInt
        while ((CC - temp00) > 0) {
          temp00 = CC
          println("temp=" + CC)
          CC = 0
          println("C=" + CC)
          center = get_centroid(covering0)
          radius = get_radius(covering0, center)
          covering0 = get_covering(data1, center, radius).cache()
          CC = covering0.count().toInt
          println("New_C=" + CC + "---" + (CC - temp00))
        }
      }


      center_l += (i -> center._2)
      println("covering0 length=" + covering0.count())

      val user = covering0.map(x => x._1).collect()
      val qos = pre.multiget(user).map(x => x._1 ->(x._2._1, i))
      pre = pre.multiput(qos)
      /* IndexedRDD主要提供了三个接口：
     multiget: 获取一组Key的Value
     multiput: 更新一组Key的Value
     delete: 删除一组Key的Value*/
      i += 1
    }//clustering Ending

    while (get_R(center_l)._3 >= R) {
      //println("**")
      val m = get_R(center_l)
      val tail = pre.filter(x => x._2._2 == m._2).map(x => x._1 ->(x._2._1, m._1)).collect().toMap
      pre = pre.multiput(tail)
      val data = pre.filter(x => x._2._2 == m._1).map(x => (x._1, x._2._1))
      val center = get_center(data)
      center_l += (m._1 -> center._2)
      center_l.remove(m._2)
    }//granulate analysis

    for (cen <- center_l) {
      println("covering label=" + cen._1)
      val ll: Int = cen._2.length
      for (ii <- 0 until ll) {
        print("covering centroid=" + cen._2(ii) + ",")
      }
      println("+++++++++++++++++++=")
      for (j <- 0 until ll) {
        if (j != (ll - 1))
          out0.write(cen._2(j) + ",")
        else out0.write(cen._2(j) + "\n")
      }
    } //print covering centroids:center_l
    out0.close()

//    //MSE
//    var sum0: Double = 0.0
//    for (ii <- center_l) {
//      val buf_data = pre.filter(x => x._2._2 == ii._1).map(x => (x._1, x._2._1))
//      println("data numbers" + buf_data.count())
//      val temp: Double = get_distanceM_M(buf_data, ii)
//      println("temp=" + temp)
//      sum0 += temp
//    }
//    sum0 = sum0/train.count()
//    println("the mse cost=" + sum0)
//
//    //separate
//    var sum1: Double = 0.0
//    for (ii <- center_l) {
//      val buf_data = pre.filter(x => x._2._2 == ii._1).map(x => (x._1, x._2._1))
//      println("data numbers" + buf_data.count())
//      val temp: Double = get_distanceM_M_separate(buf_data, ii)
//      println("temp=" + temp)
//      sum1 += temp
//    }
//    println("the cost=" + sum1)
//
//    //CP
//    var CP: Double = 0.0
//    for (ii <- center_l) {
//      val buf_data = pre.filter(x => x._2._2 == ii._1).map(x => (x._1, x._2._1))
//      val temp_cp: Double = get_distanceS_P(buf_data, ii)
//      println("temp_CP = " + temp_cp)
//      CP += temp_cp
//    }
//    println("the CP =" + CP)
//
//    //SP
//    val s: Int = center_l.size
//    var sample: List[Array[Double]] = List()
//    for (ii <- center_l) {
//      sample = sample ::: List(ii._2)
//    }
//    var sp: Double = 0.0
//    for (ii <- 0 until s) {
//      for (jj <- (ii + 1) until s) {
//        sp += get_distance(sample(ii), sample(jj))
//      }
//    }
//    sp = sp * 2 / (s * s - s)
//    println("the SP =" + sp)

    val res = pre.map(x => (x._2._2, x._1))
    res
  }

  def get_distanceM_M(a: RDD[(Long, Array[Double])], b: (Int, Array[Double])): Double = {
    var sum: Double = 0.0
    sum = a.map(x => get_distance2(x._2, b._2)).sum()
    sum
  }
  def get_distanceM_M_separate(a: RDD[(Long, Array[Double])], b: (Int, Array[Double])): Double = {
    val len = a.count()
    var sum: Double = 0.0
    sum = a.map(x => get_distance2(x._2, b._2)).sum() / len.toDouble
    sum
  }
  def get_distanceS_P(a: RDD[(Long, Array[Double])], b: (Int, Array[Double])): Double = {
    val len = a.count()
    var sum: Double = 0.0
    sum = a.map(x => get_distance(x._2, b._2)).sum() / len.toDouble
    sum
  }

  def get_distance2(a: Array[Double], b: Array[Double]): Double = {
    (for {x <- a.indices
          p = pow(a(x) - b(x), 2)} yield p).sum
  }
  def get_distance(a: Array[Double], b: Array[Double]): Double = {
    sqrt((for {x <- a.indices
               p = pow(a(x) - b(x), 2)} yield p).sum)
  }

  def get_centroid(data: RDD[(Long, Array[Double])]): (Long, Array[Double]) = {
    val qos = data.map(x => x._2)
    val m = qos.count()
    val n = qos.first().length
    val c = (for {i <- 0 until n
                  p = qos.map(x => x(i)).sum() / m.toDouble
    } yield p).toArray
    (-1, c)
  }
  def get_center(data: RDD[(Long, Array[Double])]): (Long, Array[Double]) = {
    val qos = data.map(x => x._2)
    val m = qos.count()
    val n = qos.first().length
    val c = (for {i <- 0 until n
                  p = qos.map(x => x(i)).sum() / m.toDouble
    } yield p).toArray
    data.map(x => (x, get_distance(x._2, c))).sortBy(x => x._2).map(x => x._1).first()
  }

  def get_radius_first(data: RDD[(Long, Array[Double])], center: (Long,Array[Double])): Double = {
    //    val len = data.count().toDouble-1
    //    if(len == 0) 0.0
    //    else{
    val m = data.filter(x => x != center).map(x => (x, get_distance(x._2, center._2)))
    val n = m.map(x => x._2)
    val min = n.min()
    val max = n.max()
    val p = m.map(x => (x, ((max - x._2) / (max - min)))).collect()

    val ran = new Random()
    var dataNum: Int = 0
    var rSum: Double = 0.0

    for (line <- p) {
      var tempP: Double = ran.nextDouble()

      //    if(dataNum>=200)
      //      tempP=10

      if (tempP <= line._2) {
        dataNum = dataNum + 1
        rSum = rSum + line._1._2
      }
    }
    println("dataNum=" + dataNum + "\t\trSum" + rSum / dataNum + "\t\tmax=" + max + "\t\t" + min)
    rSum / dataNum
    // }
  }
  def get_radius(data: RDD[(Long, Array[Double])], center: (Long, Array[Double])): Double = {
    val c = center._2
    val l = data.count().toDouble - 1
    if (l == 0) 0.0
    else data.filter(x => x != center).map(x => get_distance(x._2, c)).sum() / l
  }
  def get_radius_centroid(data: RDD[(Long, Array[Double])], center: (Long, Array[Double])): Double = {
    val c = center._2
    val l = data.count().toDouble
    if (l == 0) 0.0
    else data.map(x => get_distance(x._2, c)).sum() / l
  }

  def get_covering(data: RDD[(Long, Array[Double])], center: (Long, Array[Double]), radius: Double): RDD[(Long, Array[Double])] = {
    val c = center._2
    data.filter(x => get_distance(x._2, c) <= radius)
  }

  def get_R(train: Map[Int, Array[Double]]): (Int, Int, Double) = {
    var i: Int = 0
    var j: Int = 0
    var S: Double = 0

    for (p <- train.keySet) {
      for (q <- train.keySet if q > p) {
        //          var a: Double = 0
        //          var b1: Double = 0
        //          var b2: Double = 0
        //          for(i <-0 until train(q).length){
        //            a += train(p)(i)*train(q)(i)
        //            b1 += train(p)(i)*train(p)(i)
        //            b2 += train(q)(i)*train(q)(i)
        //          }
        //          b1 = sqrt(b1)
        //          b2 = sqrt(b2)
        //          val d = a / (b1 * b2)
        var d = 1 / (1 + get_distance(train(p), train(q)))
        //d = d * 10
        print("class similarity = "+d+",")
        if (d > S) {
          S = d
          i = p
          j = q
        }
      }
    }
    println("sssssssssssssss")
    (i, j, S)
  }


}


