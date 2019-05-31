package algorithms
import breeze.numerics._
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._
import org.apache.hadoop.hive.ql.optimizer.NonBlockingOpDeDupProc
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.Map
import scala.io.Source
import scala.util.Random
import breeze.numerics.{abs, sqrt, pow}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}


/**
 * Created by zyy on 7/25/17.
 */
//improved covering algorithm
object Test_split_merge {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("covering").setMaster("local[*]")
    val sc = new SparkContext(conf)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.Server").setLevel(Level.OFF)
    //gauss_normal
    val data: Array[Array[Double]] = Source.fromFile("/home/mycluster/IdeaProjects/DataSet/dataset/Iris_deal.txt").getLines().map(x => x.split(',').map(x => x.toDouble)).toArray
    val l = data.length
    val buf: Array[(Long, (Array[Double], Int))] = new Array[(Long, (Array[Double], Int))](l)
    for (i <- 0 until l) {
      buf(i) = (i.toLong, (data(i), -1))
    }
    val out = new java.io.FileWriter("/home/mycluster/IdeaProjects/clustering_new/F_C_label.txt", false)
    val result = process(IndexedRDD(sc.parallelize(buf)).cache(),2,100,3)//拆分数目; k=0,表示不事先指定k值,事先指定也可以
    val L: Int = result.map(x => x._1).collect().distinct.length
    println(L + "class number")
    val temp = result.sortBy(x => x._2).collect()
    for (i <- 0 until l) {
      out.write(temp(i)._1 + "\n")
    }

    out.close()
    sc.stop()
  }

  def process(train: IndexedRDD[Long, (Array[Double], Int)],split_num:Int,merge_inter:Int,k:Int): RDD[(Int, Long)] = {
    val out0 = new java.io.FileWriter("/home/mycluster/IdeaProjects/clustering_new/Center_Fugai.txt", false)
    var pre:IndexedRDD[Long, (Array[Double], Int)] = train.cache()
    val len_pre:Double=pre.count().toDouble
    //val min_number:Double = len_pre / 3
    var i = 0
    var center: (Long, Array[Double]) = null
    var center_l = Map[Int,Array[Double]]()

    while (!((pre.filter(x => x._2._2 == -1)).isEmpty())){
      println("center")
      var radius: Double = 0.0
      val data1 = pre.filter(x => x._2._2 == -1).map(x => (x._1, x._2._1)).cache() //uncovered data
      println("data1.count() = " + data1.count())

      //            if (center == null) {
      //             //center = get_centroid(data1)//第一个圆心直接为重心
      //             //radius = get_radius_centroid(data1, center)
      //             center = get_center(data1)
      //            }
      //            else {
      //              val c = center._2
      //              center = data1.map(x => (x, get_distance(x._2, c))).sortBy(x => x._2, ascending = false).first()._1
      //              // radius = get_radius_weight(data1, center)
      //             // center = get_max_center(data1,center_l) //距离所用已聚类的中心最远的点
      //              }

      center = get_center(data1)//圆心都为距离重心最近的，不是距离上一个簇最近的点

      println("radius")
      radius = get_radius_weight(data1, center)
      var covering0 = get_covering(data1, center, radius).cache()
      println("covering0---------------:"+covering0.count())

      center = get_centroid(covering0)
      radius = get_radius_centroid(data1,center)
      var covering1 = get_covering(data1,center,radius).cache()
      while(!covering1.subtractByKey(covering0).isEmpty()){
        covering0 = covering1
        center = get_centroid(covering0)
        radius = get_radius_centroid(data1,center)
        covering1 = get_covering(data1,center,radius).cache()
      }
      // -----------------------------Test0--------------------------------

      println("covering0 length=" + covering0.count())
      center_l += (i -> center._2)

      val user1 = covering0.map(x => x._1).collect()
      val qos = pre.multiget(user1).map(x => x._1 ->(x._2._1, i))
      pre = pre.multiput(qos)
      i += 1
    }//clustering Ending

    var intermediate_cent:Int = 0
    var merge:Int = 0
    //细分类
    var times_split = 0
    while(times_split < split_num){
      println("----------------times_split:="+times_split)

      var dim_arr:Map[Int,(Int,Double)] = Map[Int,(Int, Double)]()
      var center_d_i:Map[Int,Double] = Map[Int,Double]()
      var center_radius:Double=0

      for(c<-center_l){
        val m = c._1
        val pre_differ:IndexedRDD[Long, (Array[Double], Int)]=pre.filter(x => x._2._2 == m).cache()
        val d_i_sum=pre_differ.map(x=>get_distance(c._2,x._2._1)).sum()
        val d_i=d_i_sum/pre_differ.count().toDouble
        center_d_i += (m->d_i)
        center_radius += d_i_sum
        val every_dim:Array[Array[Double]]=get_every_dim(pre_differ)
        val len_every_dim:Int=every_dim.length
        val stand:Array[Double]=new Array[Double](len_every_dim)
        for(eg <- 0 until len_every_dim){
          stand(eg)=get_delete(every_dim(eg),c._2(eg))
        }
        val dim_max:(Int,Double)=get_array_max(stand)
        dim_arr += (m->dim_max)
      }
      center_radius=center_radius/len_pre

      //println("center_l size:="+center_l.size)
      val temp_all:Map[Int,(Int,Double)]=get_class_max(dim_arr,center_d_i,center_radius,pre,len_pre,center_l.size,k)//center label,max dim label,max dim value
      //find the suitable center to split

      for(temp <- temp_all){

        val n = center_l.keySet.max+1
        var center_l_split = Map[Int,Array[Double]]()
        val t = temp._1 //Label
        //println("max lable:"+n+",t:="+t+",max value"+temp._2._2)

        var pre_split:IndexedRDD[Long, (Array[Double], Int)] = pre.filter(x => x._2._2 == t).cache()

        val flag0:Array[Double]=update0(center_l(t),temp._2)//center-

        val flag1:Array[Double]=update1(center_l(t),temp._2)//center+

        center_l.remove(t)
        center_l_split += (n -> flag0)
        center_l_split += ((n+1) -> flag1)
        pre_split=updateData(pre_split,center_l_split).cache()
        //val filter_data=pre_split.filter(x => x._2._2==n).cache()
        //val user0=filter_data.map(x => x._1).collect()
        val user0=pre_split.filter(x => x._2._2==n).map(x => (x._1,x._2._1)).cache()
        val user00=user0.map(x => x._1).collect()
        if(user00.length==0){
          center_l_split.remove(n)
          println("0000:="+n)
        } else{
          val c_0 = get_centroid(user0)
          center_l_split += (n -> c_0._2)
          val qos_split0 = pre_split.multiget(user00).map(x => x._1 ->(x._2._1, n))
          pre = pre.multiput(qos_split0)
        }
        //var user1=pre_split.subtract(filter_data).map(x => x._1).collect()
        val user1=pre_split.filter(x => x._2._2==(n+1)).map(x => (x._1,x._2._1)).cache()
        val user11=user1.map(x => x._1).collect()
        if(user11.length==0){
          center_l_split.remove(n+1)
          println("0001:="+(n+1))
        } else{
          val c_1 = get_centroid(user1)
          center_l_split += ((n+1) -> c_1._2)
          val qos_split1 = pre_split.multiget(user11).map(x => x._1 ->(x._2._1, n+1))
          pre = pre.multiput(qos_split1)
        }

        for(eg <- center_l_split){
          center_l += eg
        }

      }


      times_split += 1
    }

    intermediate_cent = center_l.size
    println("center_l intermediate_cent length:"+intermediate_cent)
    //细分类-层次聚类中分裂
    merge = intermediate_cent-k
    if(k!=0){
      if(merge<=0){
        println("Sorry,the number of split is too small,Please enter again!!! ")
      } else{
        //    聚合类：类数目最小的进行合并

                var times = 0
                while(times < merge){
                  val flag:(Int,Array[Double]) = get_min(pre,center_l)
                  val m = get_Single2Set_R(center_l , flag)
                  val tail = pre.filter(x => x._2._2 == m._2).map(x => x._1 ->(x._2._1, m._1)).collect().toMap
                  pre = pre.multiput(tail)
                  val data = pre.filter(x => x._2._2 == m._1).map(x => (x._1, x._2._1))
                  val center = get_centroid(data)
                  center_l += (m._1 -> center._2)
                  center_l.remove(m._2)
                  times += 1
                }

//        if(merge<merge_inter){
//          var times = 0
//          while(times < merge){
//            val flag:(Int,Array[Double]) = get_min(pre,center_l)
//            val m = get_Single2Set_R(center_l , flag)
//            val tail = pre.filter(x => x._2._2 == m._2).map(x => x._1 ->(x._2._1, m._1)).collect().toMap
//            pre = pre.multiput(tail)
//            val data = pre.filter(x => x._2._2 == m._1).map(x => (x._1, x._2._1))
//            val center = get_centroid(data)
//            center_l += (m._1 -> center._2)
//            center_l.remove(m._2)
//            times += 1
//          }
//        } else {
//          val beishu:Int = merge/merge_inter
//          val yushu:Int = merge%merge_inter
//          var time_bs:Int = 0
//          var time_ys:Int = 0
//          while(time_bs<beishu){
//            val flag:Map[Int,Array[Double]] = get_min_set(pre,center_l,merge_inter)
//            for(eg <- flag){
//              val m = get_Single2Set_R(center_l , eg)
//              val tail = pre.filter(x => x._2._2 == m._2).map(x => x._1 ->(x._2._1, m._1)).collect().toMap
//              pre = pre.multiput(tail)
//              val data = pre.filter(x => x._2._2 == m._1).map(x => (x._1, x._2._1))
//              val center = get_centroid(data)
//              center_l += (m._1 -> center._2)
//              center_l.remove(m._2)
//            }
//            println("time_bs:="+time_bs)
//            time_bs += 1
//          }
//          while(time_ys<yushu){
//            val flag:(Int,Array[Double]) = get_min(pre,center_l)
//            val m = get_Single2Set_R(center_l , flag)
//            val tail = pre.filter(x => x._2._2 == m._2).map(x => x._1 ->(x._2._1, m._1)).collect().toMap
//            pre = pre.multiput(tail)
//            val data = pre.filter(x => x._2._2 == m._1).map(x => (x._1, x._2._1))
//            val center = get_centroid(data)
//            center_l += (m._1 -> center._2)
//            center_l.remove(m._2)
//            println("time_ys:="+time_ys)
//            time_ys += 1
//          }
//        }//else


        //最相似的进行合并
//                        var times:Int = 0
//                        while (times < merge){
//                          val m = get_R(center_l)
//                          val tail = pre.filter(x => x._2._2 == m._2).map(x => x._1 ->(x._2._1, m._1)).collect().toMap
//                          pre = pre.multiput(tail)
//                          val data = pre.filter(x => x._2._2 == m._1).map(x => (x._1, x._2._1))
//                          val center = get_centroid(data)
//                          //val center = get_centroid(data)
//                          center_l += (m._1 -> center._2)
//                          center_l.remove(m._2)
//                          times += 1
//                        }//granulate analysis

      }
    }


    println("center_l length:"+center_l.size)

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

    for(ii <- center_l){
      val num_data = pre.filter(x => x._2._2 == ii._1).count()
      print(num_data+",")
    }
    println("---------------------------------------------------")
    //    //Cost
    //    val cost:Double = get_Set2Set_distance2(pre,center_l) / len
    //    println("cost:="+cost)
    println("merge number:="+merge+"=====================")


    val res = pre.map(x => (x._2._2, x._1))
    res
  }



  def get_PointSet_distance2(pt:(Long, (Array[Double], Int)), data: Map[Int,Array[Double]]):Double={//x&C:shortest distance(Point:x & Set:C)
  var min_dis = 1e20
    for(i <- data.keySet){
      val sqdis = get_sqdis(pt._2._1,data(i))
      if(sqdis < min_dis){
        val  dis = get_distance2(pt._2._1,data(i))
        if(dis < min_dis){
          min_dis = dis
        }
      }
    }
    min_dis
  }
  def get_Set2Set_distance2(data:IndexedRDD[Long, (Array[Double], Int)],center: Map[Int,Array[Double]]):Double={
    val sum:Double = data.map(x => get_PointSet_distance2(x,center)).sum()
    sum
  }

  def get_distanceM_M(a: RDD[(Long, Array[Double])], b: (Int, Array[Double])): Double = {
    var sum: Double = 0.0
    sum = a.map(x => get_distance2(x._2, b._2)).sum()
    sum
  }


  def get_distance2(a: Array[Double], b: Array[Double]): Double = {
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
    println("m and n:"+m+","+n)
    val c = (for {i <- 0 until n
                  p = qos.map(x => x(i)).sum() / m.toDouble
    } yield p).toArray
    data.map(x => (x, get_distance(x._2, c))).sortBy(x => x._2).first()._1
  }

  def get_radius_weight(data: RDD[(Long, Array[Double])], center: (Long,Array[Double])): Double ={
    //    val c = center._2
    //    val l = data.count().toDouble - 1
    //    if (l == 0) 0.0
    //    else data.filter(x => x != center).map(x => get_distance(x._2, c)).sum() / l
    var rr:Double = 0
    val l = data.count().toDouble - 1
    if(l == 0){
      rr = 0.0
    } else if(l == 1){
      rr = data.filter(x => x != center).map(x =>get_distance(x._2,center._2)).sum()
    } else {
      val data_dis = data.filter(x => x != center).map(x =>get_distance(x._2,center._2)).cache()//每个数据点与圆心距离
      val sum_dis:Double = data_dis.sum()//距离之和
      if(sum_dis == 0){
        rr = 0//重复的数据点半径为0
      }
      else {
        val data_weight = data_dis.map(x => (x,(1-x/sum_dis))).cache()//（距离,权重）
        val final_sum:Double = data_weight.map(x => x._2).sum()
        val data_final_weight = data_weight.map(x => x._1*(x._2/final_sum)).cache()
        rr = data_final_weight.sum()
      }
    }
    rr
  }
  def get_radius_centroid(data: RDD[(Long, Array[Double])], center: (Long,Array[Double])): Double ={
    var rr:Double = 0
    val l = data.count().toDouble
    if(l == 1){
      rr = 0
    } else {
      val data_dis = data.map(x =>get_distance(x._2,center._2)).cache()//每个数据点与圆心距离
      //      val len = data_dis.count().toDouble
      //      if(len == 2){
      //        val ff:Array[Array[Double]] = data.map(x => x._2).collect()
      //        for(ii <- ff){
      //          for(j <- 0 until ii.length){
      //            print(ii(j)+",")
      //          }
      //          println("huanhang")
      //        }
      //        val sum_dis:Double = data_dis.sum()//距离之和
      //        println("sum_dis:="+sum_dis)
      //        val data_weight = data_dis.map(x => (x,(1-x/sum_dis)))//（距离,权重）
      //        val final_sum:Double = data_weight.map(x => x._2).sum()
      //        println("final_sum:="+final_sum)
      //        val data_final_weight = data_weight.map(x => x._1*(x._2/final_sum))
      //        rr = data_final_weight.sum()
      //      } else {
      val sum_dis:Double = data_dis.sum()//距离之和
      if(sum_dis == 0){
        rr = 0
      }
      else{
        val data_weight = data_dis.map(x => (x,(1-x/sum_dis))).cache()//（距离,权重）
        val final_sum:Double = data_weight.map(x => x._2).sum()
        val data_final_weight = data_weight.map(x => x._1*(x._2/final_sum)).cache()
        rr = data_final_weight.sum()
      }
      //      }
    }
    rr

    //          val c = center._2
    //          val l = data.count().toDouble
    //          if (l == 1.0) 0.0
    //          else data.map(x => get_distance(x._2, c)).sum() / l
  }
  def get_radius(data: RDD[(Int,Array[Double])], center: (Int, Array[Double])): Double = {
    val c = center._2
    val l = data.count().toDouble - 1
    if (l == 0) 0.0
    else data.filter(x => x != center).map(x => get_distance(x._2, c)).sum() / l
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
        var d:Double = 1 / (1 + get_distance(train(p), train(q)))
        //d = d * 10
        //print("class similarity = "+d+","+"---")
        if (d > S) {
          S = d
          i = p
          j = q
        }
      }
    }
    (i, j, S)
  }

  def get_Single2Set_R(train: Map[Int, Array[Double]],pt:(Int,Array[Double])): (Int,Int, Double) = {
    var i: Int = 0
    val j: Int = pt._1
    var S: Double = 0
    for (p <- train.keySet if p != j) {
      val d:Double = 1 / (1 + get_distance(train(p),pt._2 ))
      if (d > S) {
        S = d
        i = p
      }
    }
    (i,j, S)

  }

  def get_min_set(data: IndexedRDD[Long, (Array[Double], Int)], c: Map[Int, Array[Double]],merge:Int): Map[Int,Array[Double]] = {
    var re: Map[Int,Array[Double]] = Map[Int,Array[Double]]()
    var temp:List[(Double,(Int,Array[Double]))] = List()
    var length: Double = 0
    for(t <- c){
      length = data.filter(x => x._2._2 == t._1).count().toDouble
      temp = (length,t)::temp
    }
    val t:List[(Double,(Int,Array[Double]))] = temp.sortBy(x => x._1)
    for(ii<- 0 until merge){
      println("----:"+t(ii)._1)
    }
    for(eg <- 0 until merge){
      re += temp(eg)._2
    }
    re
  }
  def get_min(data: IndexedRDD[Long, (Array[Double], Int)], c: Map[Int, Array[Double]]): (Int,Array[Double]) = {
    var re: (Int,Array[Double]) = null
    var min:Double = 1e20
    var length: Double = 0
    for(t <- c){
      length = data.filter(x => x._2._2 == t._1).count().toDouble
      if(length < min){
        min = length
        re = t
      }
    }
    println("min length:"+min)
    re
  }



  def get_delete(p:Array[Double],c:Double):Double={
    val d:Int=p.length
    var re:Double=0.0
    for(eg <- 0 until d){
      re += math.pow(p(eg)-c,2)
    }
    math.sqrt(re/d)
  }

  def get_every_dim(data:IndexedRDD[Long, (Array[Double], Int)]):Array[Array[Double]]= {//RDD[(Long,Array[Double])]
  val data_buf: Array[Array[Double]] = data.values.keys.collect()
    val len: Int = data_buf.length
    val d: Int = data_buf(0).length
    val re: Array[Array[Double]] = new Array[Array[Double]](d)
    val arr: Array[Double] = new Array[Double](len)
    for (eg <- 0 until d) {
      for (eg2 <- 0 until len) {
        arr(eg2) = data_buf(eg2)(eg)
      }
      re(eg) = arr.clone()
    }
    re
  }

  def get_array_max(p:Array[Double]):(Int,Double)={
    var max:Double=0.0
    var lab:Int=0
    for(eg<-0 until p.length){
      if(p(eg)>max){
        max=p(eg)
        lab=eg
      }
    }
    (lab,max)
  }

  def get_max(data: IndexedRDD[Long, (Array[Double], Int)], c: Map[Int,(Int,Double)]): (Int,(Int,Double)) = {
    var re: (Int,(Int,Double)) = null
    var max:Double = 0
    var length: Double = 0
    for(t <- c){
      length = data.filter(x => x._2._2 == t._1).count().toDouble
      if(length > max){
        max = length
        re = t
      }
    }
    println("max length:"+max+",label:="+re._1)
    re
  }

  def get_class_max(all_class:Map[Int,(Int,Double)],d_i:Map[Int,Double],d_all:Double,data:IndexedRDD[Long, (Array[Double], Int)],min_number:Double,c_size:Int,k_value:Int):Map[Int,(Int,Double)]={
    var re:Map[Int,(Int,Double)] = Map[Int,(Int,Double)]()
    val ave:Double=all_class.par.map(x=>x._2._2).sum/all_class.size
    re += get_max(data,all_class)

    for(p <- all_class){
      val value = p._2._2
      //      println("similarity:="+value)
      //      println("ave:="+ave/3)
      if(value>ave/2){
//      if(value>0.01){
        val len_c:Double=data.filter(x => x._2._2==p._1).count().toDouble
        if( ((d_i(p._1)>d_all) && (len_c > min_number / k_value)) || (len_c > 2*min_number/k_value) || (c_size < k_value / 2)){
          re += p
          println("Test!!"+p._1)
        }
      }
      //println("class label:="+p._1+"max Dim:="+value+"ave:="+ave+",d_all:="+d_all+"d_i(p._1):="+d_i(p._1))
    }
    re
  }


  def update0(c:Array[Double],dim_max:(Int,Double)):Array[Double]={
    val len:Int = c.length
    val re:Array[Double] = new Array[Double](len)
    for(eg <- 0 until len if eg!=dim_max._1){
      re(eg)=c(eg)
    }
    re(dim_max._1) = c(dim_max._1) - dim_max._2
    re
  }

  def update1(c:Array[Double],dim_max:(Int,Double)):Array[Double]={
    val len:Int = c.length
    val re:Array[Double] = new Array[Double](len)
    for(eg <- 0 until len if eg!=dim_max._1){
      re(eg)=c(eg)
    }
    re(dim_max._1) = c(dim_max._1) + dim_max._2
    re
  }

  //更新数据
  def updateData(dataSet:IndexedRDD[Long,(Array[Double],Int)],centSet:Map[Int,Array[Double]]):IndexedRDD[Long,(Array[Double],Int)]={//RDD[(Long,Array[Double],Int)]
  val upDataSet:IndexedRDD[Long,(Array[Double],Int)]=IndexedRDD(dataSet.map(x=>{
      val temp:(Long,(Array[Double],Int))=(x._1,(x._2._1,neal(x,centSet)))
      temp
    }))
    upDataSet
  }

  //寻找最近的中心，返回索引
  def neal(item:(Long,(Array[Double],Int)),centSet:Map[Int,Array[Double]]):Int={
    var min:Double=1e9
    var index:Int=(-1)
    for(c<-centSet.keySet){
      val dist:Double=get_distance2(item._2._1,centSet(c))
      if(dist<min){
        min=dist
        index=c
      }
    }
    index
  }

}



