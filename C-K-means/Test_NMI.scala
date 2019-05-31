package algorithms

import breeze.numerics.sqrt

import scala.io.Source
import scala.collection.mutable.Map
/**
 * Created by zyy on 8/3/17.
 */
object Test_NMI {
  def main(args: Array[String]) {
    val real = Source.fromFile("/home/mycluster/IdeaProjects/DataSet/dataset/Abalone_new_deal_label").getLines().map(x => x.toInt).toArray
    val test = Source.fromFile("/home/mycluster/IdeaProjects/clustering_new/F_G_label.txt").getLines().map(x => x.toInt).toArray

    // val A:Array[Int] = Array(1,1,1,1,1,1,2,2,2,2,2,2,3,3,3,3,3)
   // val B:Array[Int] = Array(1,2,1,1,1,1,1,2,2,2,2,3,1,1,3,3,3)
    val ex = new NMI
    println(ex.NMI(real, test))
    //println(ex.NMI(A, B))
    //    val a:Array[Int]=Array(1,2,1,1,1,1,2,2,2,2,3,1,1,3,3,3)
    //    val b:Array[Int]=Array(1,2,2,1,1,1,1,2,2,2,1,3,3,3,3,3)
    //    val c:Map[Int,Int]=wjtMapIndex(a,b)
    //    val d:Map[(Int,Int),Int]=wjtMapKey(a,b)
    //    println(c)
    //    println(d)
    //
    //  }
    //
    //  def wjtMapIndex(real:Array[Int],test:Array[Int]):Map[Int,Int]={
    //    var re:Map[Int,Int]=Map()
    //    val lenth:Int=real.length
    //    for(i<-0 until lenth){
    //      if(real(i)==test(i)){
    //        if(re.contains(real(i)))
    //          re(real(i))=re(real(i))+1
    //        else
    //          re=re++Map(real(i)->1)
    //      }
    //    }
    //    re
    //  }
    //
    //  def wjtMapKey(real:Array[Int],test:Array[Int]):Map[(Int,Int),Int]={
    //
    //    val distinctReal:Array[Int]=real.distinct
    //    val distinctTest:Array[Int]=test.distinct
    //
    //    var re:Map[(Int,Int),Int]=Map()
    //    val lenth:Int=real.length
    //
    //    for(i<-0 until lenth){
    //      val key:(Int,Int)=(real(i),test(i))
    //      if(re.contains(key))
    //        re(key)=re(key)+1
    //      else
    //        re=re++Map(key->1)
    //    }
    //
    //    for(r<-distinctReal)
    //      for(t<-distinctTest){
    //        val key:(Int,Int)=(r,t)
    //        if(!re.contains(key))
    //          re=re++Map(key->0)
    //      }
    //
    //    re
    //  }

  }
}

class NMI {
  def NMI(real: Array[Int], test: Array[Int]): Double = {
    val L1 = real.distinct
    val L2 = test.distinct
    val len1: Int = real.length
    val len2: Int = test.length
    println(L1.length + "," + L2.length)

    var I: Double = 0.0
    var p1 = Map[Int,Double]()
    var p2 = Map[Int,Double]()
    var H_1: Double = 0.0
    var H_2: Double = 0.0

    for (i <- L1) {
      var p: Double = 0.0
      var num: Int = 0
      for (j <- real) {
        if (i == j) {
          num += 1
        }
      }
      p = num / len1.toDouble
      p1 += (i -> p)
    }


    for (i <- L2) {
      var p: Double = 0.0
      var num: Int = 0
      for (j <- test) {
        if (i == j) {
          num += 1
        }
      }
      p = num / len2.toDouble
      p2 += (i -> p)
    }


    var p12: Map[(Int, Int),Int] = Map()
    p12 = wjtMapKey(real, test)

    for(i <- L1){
      for(j <- L2){
        var temp:Double =0.0
        if(p12(i,j) == 0){
          temp = 0
        }
        else{
          //print("p12((i,j))/len1"+p12((i,j))/len1.toDouble+"(p12(i,j)/len1) / (p1(i)*p2(j))"+(p12(i,j)/len1.toDouble) / (p1(i)*p2(j))+"----------")
         // println("------------")
          temp = (p12((i,j))/len1.toDouble)*((math.log((p12(i,j)/len1.toDouble) / (p1(i)*p2(j))))/math.log(2))
        }
        println("temp"+temp)
        I += temp

      }
    }



    for (i <- p1) {
      H_1 -= i._2 * math.log(i._2)/math.log(2)
    }

    for (i <- p2) {
      H_2 -= i._2 * math.log(i._2)/math.log(2)
    }
    println("H_1"+H_1+"H_2"+H_2+"sqrt(H_1*H_2)"+sqrt(H_1*H_2))


    val re:Double = I / sqrt(H_1*H_2)

    return re
  }


  def wjtMapKey(real:Array[Int],test:Array[Int]):Map[(Int,Int),Int]={

    val distinctReal:Array[Int]=real.distinct
    val distinctTest:Array[Int]=test.distinct

    var re:Map[(Int,Int),Int]=Map()
    val lenth:Int=real.length

    for(i<-0 until lenth){
      val key:(Int,Int)=(real(i),test(i))
      if(re.contains(key))
        re(key)=re(key)+1
      else
        re=re++Map(key->1)
    }

    for(r<-distinctReal)
      for(t<-distinctTest){
        val key:(Int,Int)=(r,t)
        if(!re.contains(key))
          re=re++Map(key->0)
      }
    re
  }

}



