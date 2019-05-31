/**
 * Created by wujintao on 17-8-27.
 */
package algorithms
import breeze.numerics._
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._
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

//C_K-means
object C_K {
  def main (args: Array[String]){
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.Server").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("C-K").setMaster("local[*]")
    //val conf = new SparkConf().setAppName("C-K")
    val sc = new SparkContext(conf)

    val dataSet:Array[Array[Double]]=Source.fromFile("//home/mycluster/IdeaProjects/DataSet/dataset/Iris_deal.txt").getLines().map(x=>x.split(',').map(x=>x.toDouble)).toArray
    //val dataSet:Array[Array[Double]]=sc.textFile("hdfs://192.168.1.11:9000/czyy/house_norm").map(x=>x.split(',').map(x=>x.toDouble)).toArray
    val l = dataSet.length

    val buf: Array[(Long,Array[Double], Int)] = new Array[(Long, Array[Double], Int)](l)
    for (i <- (0 until l).par) {
      buf(i) = (i.toLong, dataSet(i).clone(), -1)
    }

    val rddData=sc.parallelize(buf,10).cache()
    val rddData_init=IndexedRDD(rddData.map(x=>(x._1,(x._2,x._3)))).cache()
    val D:Int=rddData.first()._2.length
    println("dimension:="+D)


    val time1=System.currentTimeMillis()
    val t=runKMtest(rddData,200,D,rddData_init,11,20)
    val time2=System.currentTimeMillis()
    println("C-K time is "+(time2-time1))
    println("C-K Iter:="+t._3)

    //    val da=t._1.collect()
    //    for(temp<-da){
    //      print(temp._1+",")
    //      printArr(temp._2)
    //      println(","+temp._3)
    //    }

  }

  //运行kmeans
  def runKMtest(dataS:org.apache.spark.rdd.RDD[(Long,Array[Double],Int)],Iter:Int,D:Int,init_data:IndexedRDD[Long, (Array[Double], Int)],split_num:Int,k:Int):(org.apache.spark.rdd.RDD[(Long,Array[Double],Int)],Array[(Long,Array[Double],Int)],Int)={
    var rddData=dataS
    //val D:Int=dataS.first()._2.length

    // val init_data=dataS.map(x=>(x._1,x._2)).cache()

    // val init_cent:Array[Array[Double]]=process(init_data)
    // val k:Int=init_cent.length
    val time1=System.currentTimeMillis()
    val init_cent:Array[Array[Double]] = process(init_data,D,split_num,k)
    val time2=System.currentTimeMillis()
    println("Covering time is "+(time2-time1))
//    val kk:Int=init_cent.length
//    println("center size:="+kk+"===================")

    var centSet:Array[(Long,Array[Double],Int)]=new Array[(Long, Array[Double], Int)](k)

    var labelIndex:Int=0
    for(c<-init_cent){
      val tempC:(Long,Array[Double],Int)=(-1,c,labelIndex)
      centSet(labelIndex)=tempC
      labelIndex=labelIndex+1
    }

    var tempCent:Array[(Long,Array[Double],Int)]=initTemCent(D,k)
    var iter:Int=0
    var flag:Boolean=true

    while(flag){
      rddData=updateData(rddData,centSet)
      centSet=updateCent(rddData,centSet)

      //single cost
      val sub:Double=computSubSingle(tempCent,centSet)

      if(sub<Math.pow(0.001,2)||iter>Iter)
        flag=false
      else {
        tempCent=centSet
        iter=iter+1
      }
    }
    val re=(rddData,centSet,iter+1)
    re
  }

  //
  def process(train: IndexedRDD[Long, (Array[Double], Int)],D:Int,split_num:Int,k:Int): Array[Array[Double]] = {
    var pre = train
    val len_pre:Double=pre.count().toDouble
    var i = 0
    var center: (Long,Array[Double]) = null
    var center_l = Map[Int,Array[Double]]()

    while (!((pre.filter(x => x._2._2 == -1)).isEmpty())){
      var radius: Double = 0.0
      val data1 = pre.filter(x => x._2._2 == -1).map(x => (x._1, x._2._1)).cache()
      center = get_center(data1,D)//圆心都为距离重心最近的，不是距离上一个簇最近的点
      radius = get_radius_weight(data1, center)

      var covering0 = get_covering(data1, center, radius).cache()

      //      var CC: Int = 0
      //      var temp00: Int = 0
      //      CC = covering0.count().toInt

      //-----------------------------Test0--------------------------------
      //      while ((CC - temp00) > 0) {
      //        temp00 = CC
      //        CC = 0
      //        center = get_centroid(covering0)
      //        radius = get_radius_centroid(pre, center)
      //        covering0 = get_covering(pre, center, radius).cache()
      //        CC = covering0.count().toInt
      //      }
      center = get_centroid(covering0,D)
      radius = get_radius_centroid(data1,center)
      var covering1 = get_covering(data1,center,radius).cache()
      //      while(covering1.subtractByKey(covering0).count().toInt > 0){
      while(!covering1.subtractByKey(covering0).isEmpty()){
        covering0 = covering1
        center = get_centroid(covering0,D)
        radius = get_radius_centroid(data1,center)
        covering1 = get_covering(data1,center,radius).cache()
      }
      // -----------------------------Test0--------------------------------

      center_l += (i -> center._2)

      val user1 = covering0.map(x => x._1).collect()
      val qos = pre.multiget(user1).map(x => x._1 ->(x._2._1, i))
      pre = pre.multiput(qos)
      i += 1
    }//clustering Ending


    var intermediate_cent:Int = 0
    var merge:Int = 0
    //细分类-覆盖方式
    //细分类-覆盖方式
    var times_split = 0
    while(times_split < split_num){

      var dim_arr:Map[Int,(Int,Double)] = Map[Int,(Int, Double)]()
      var center_d_i:Map[Int,Double] = Map[Int,Double]()
      var center_radius:Double=0

      for(c<-center_l){
        val m = c._1
        val pre_differ:IndexedRDD[Long, (Array[Double], Int)]=pre.filter(x => x._2._2 == m).cache()
        val d_i_sum=pre_differ.map(x=>distance(c._2,x._2._1)).sum()
        val d_i=d_i_sum/pre_differ.count().toDouble
        center_d_i += (m->d_i)
        center_radius += d_i_sum
        val every_dim:Array[Array[Double]]=get_every_dim(pre_differ,D)
        val stand:Array[Double]=new Array[Double](D)
        for(eg <- 0 until D){
          stand(eg)=get_delete(every_dim(eg),c._2(eg))
        }
        val dim_max:(Int,Double)=get_array_max(stand)
        dim_arr += (m->dim_max)
      }
      center_radius=center_radius/len_pre

      val temp_all:Map[Int,(Int,Double)]=get_class_max(dim_arr,center_d_i,center_radius,pre)//center label,max dim label,max dim value
      //find the suitable center to split

      for(temp <- temp_all){

        val n = center_l.keySet.max+1
        var center_l_split = Map[Int,Array[Double]]()
        val t = temp._1 //Label
        //println("max lable:"+n+",t:="+t+",max value"+temp._2._2)

        var pre_split:IndexedRDD[Long, (Array[Double], Int)] = pre.filter(x => x._2._2 == t).cache()

        val flag0:Array[Double]=update0(center_l(t),temp._2)//center-

        val flag1:Array[Double]=update1(center_l(t),temp._2)//center+


        //        for(ii<- 0 until flag0.length){
        //          print(flag0(ii)+",")
        //        }
        //        println("===============")
        //        for(ii<- 0 until flag1.length){
        //          print(flag1(ii)+",")
        //        }
        //        println("===============")

        center_l.remove(t)
        center_l_split += (n -> flag0)
        center_l_split += ((n+1) -> flag1)
        pre_split=updateData_covering(pre_split,center_l_split).cache()
        //val filter_data=pre_split.filter(x => x._2._2==n).cache()
        //val user0=filter_data.map(x => x._1).collect()
        val user0=pre_split.filter(x => x._2._2==n).map(x => x._1).collect()
        if(user0.length==0){
          center_l_split.remove(n)
          println("0000:="+n)
        } else{
          val qos_split0 = pre_split.multiget(user0).map(x => x._1 ->(x._2._1, n))
          pre = pre.multiput(qos_split0)
        }
        //var user1=pre_split.subtract(filter_data).map(x => x._1).collect()
        val user1=pre_split.filter(x => x._2._2==(n+1)).map(x => x._1).collect()
        if(user1.length==0){
          center_l_split.remove(n+1)
          println("0001:="+(n+1))
        } else{
          val qos_split1 = pre_split.multiget(user1).map(x => x._1 ->(x._2._1, n+1))
          pre = pre.multiput(qos_split1)
        }

        for(eg <- center_l_split){
          center_l += eg
        }

      }

      times_split += 1
    }

    intermediate_cent = center_l.size
    //细分类-层次聚类中分裂
    merge = intermediate_cent-k
    if(k!=0){
      if(merge<=0){
        println("Sorry,the number of split is too small,Please enter again!!! ")
      }
      else{
        //    聚合类：类数目最小的进行合并
                var times = 0
                while(times < merge){
                  val flag:(Int,Array[Double]) = get_min(pre,center_l)
                  val m = get_Single2Set_R(center_l , flag)
                  val tail = pre.filter(x => x._2._2 == m._2).map(x => x._1 ->(x._2._1, m._1)).collect().toMap
                  pre = pre.multiput(tail)
                  val data = pre.filter(x => x._2._2 == m._1).map(x => (x._1, x._2._1))
                  val center = get_centroid(data,D)
                  center_l += (m._1 -> center._2)
                  center_l.remove(m._2)
                  times += 1
                }


        //最相似的进行合并
//        var times:Int = 0
//        while (times < merge){
//          val m = get_R(center_l)
//          val tail = pre.filter(x => x._2._2 == m._2).map(x => x._1 ->(x._2._1, m._1)).collect().toMap
//          pre = pre.multiput(tail)
//          val data = pre.filter(x => x._2._2 == m._1).map(x => (x._1, x._2._1))
//          val center = get_centroid(data,D)
//          //val center = get_centroid(data)
//          center_l += (m._1 -> center._2)
//          center_l.remove(m._2)
//          times += 1
//        }//granulate analysis

      }
    }


    val num_c:Int=center_l.size
    val out0:Array[Array[Double]]=new Array[Array[Double]](num_c)
    var flag_label:Int=0
    for(eg<-center_l.keySet){
      out0(flag_label)=center_l(eg).clone()
      flag_label += 1
    }
    out0
  }


  //更新数据
  def updateData(dataSet:org.apache.spark.rdd.RDD[(Long,Array[Double],Int)],centSet:Array[(Long,Array[Double],Int)]):org.apache.spark.rdd.RDD[(Long,Array[Double],Int)]={
    val upDataSet=dataSet.map(x=>{
      val temp:(Long,Array[Double],Int)=(x._1,x._2,neal(x,centSet))
      temp
    }).cache()
    upDataSet
  }

  //寻找最近的中心，返回索引
  def neal(item:(Long,Array[Double],Int),centSet:Array[(Long,Array[Double],Int)]):Int={
    var min:Double=1e9
    var index:Int=(-1)
    for(c<-centSet){
      //      val sq_dist:Double=distanceOther(item._2,c._2)
      //      if(sq_dist<min){
      //        val dist:Double=distance2(item._2,c._2)
      //        if(dist<min){
      //          min=dist
      //          index=c._3
      //        }
      //      }
      val dist:Double=distance2(item._2,c._2)
      if(dist<min){
        min=dist
        index=c._3
      }

    }
    index
  }

  //计算距离
  def distance(qos1:Array[Double],qos2:Array[Double]):Double={
    val lenth:Int=qos1.length
    var sum:Double=0.0
    for(i<- (0 until lenth))
      sum=sum+Math.pow((qos1(i)-qos2(i)),2.0)
    sum=Math.sqrt(sum)
    sum
  }

  //更新聚类中心
  def updateCent(dataSet:org.apache.spark.rdd.RDD[(Long,Array[Double],Int)],centSet:Array[(Long,Array[Double],Int)]):Array[(Long,Array[Double],Int)]={
    val temp=dataSet.groupBy(_._3).cache()
    val temp1:Array[(Long,Array[Double],Int)]=temp.map(x=>(0.toLong,aveCent(x._2),x._1)).collect()
    temp1
  }

  //均值求聚类中心
  def aveCent(dataLabelSet:Iterable[(Long,Array[Double],Int)]):Array[Double]={
    val size:Int=dataLabelSet.size
    val re=dataLabelSet.reduce((x,y)=>add(x,y))._2.map(_*1.0/size)
    re
  }

  //数组相加
  def add(a:(Long,Array[Double],Int),b:(Long,Array[Double],Int)):(Long,Array[Double],Int)={
    val re:Array[Double]=new Array[Double](a._2.length)
    for(i<-0 until a._2.length)
      re(i)=a._2(i)+b._2(i)
    (0,re,a._3)
  }

  //数组输出
  def printArr(a:Array[Double]):Unit={
    for(l<-a)
      print(l+"\t")
  }

  //计算cost
  def computCost(item:(Long,Array[Double],Int),centSet:Array[(Long,Array[Double],Int)]):Double={
    val c=centSet.filter(x=>x._3==item._3)(0)
    val dist:Double=distance2(item._2,c._2)
    dist
  }

  //初始化tempCent
  def initTemCent(D:Int,k:Int):Array[(Long,Array[Double],Int)]={
    val centSet:Array[(Long,Array[Double],Int)]=new Array[(Long, Array[Double], Int)](k)
    for(i<-0 until k){
      val tempQos:Array[Double]=new Array[Double](D)
      val tempCent:(Long,Array[Double],Int)=(0,tempQos,i%k)
      centSet(i)=tempCent
    }
    centSet
  }

  //计算中心点的距离//distance2 vs distance
  def computSubSingle(tempCent:Array[(Long,Array[Double],Int)],cent:Array[(Long,Array[Double],Int)]):Double={
    var maxDist:Double=1e-9
    for(i<-0 until cent.length){
      val c=tempCent.filter(x=>x._3==cent(i)._3)(0)
      val dist=distance2(cent(i)._2,c._2)
      if(dist>maxDist){
        maxDist=dist
      }
    }
    maxDist
  }

  //多个距离计算
  def distanceOther(qos1:Array[Double],qos2:Array[Double]):Double={
    val lenth:Int=qos1.length
    var sum:Double=0.0

    //非欧式距离
    var source1:Double=0.0
    var source2:Double=0.0
    for(i<-0 until lenth){
      source1=source1+Math.pow(qos1(i),2.0)
      source2=source2+Math.pow(qos2(i),2.0)
    }
    sum=Math.pow(Math.sqrt(source1)-Math.sqrt(source2),2)
    sum
  }

  //计算距离平方
  def distance2(qos1:Array[Double],qos2:Array[Double]):Double={
    val lenth:Int=qos1.length
    var sum:Double=0.0
    for(i<-(0 until lenth))
      sum=sum+Math.pow((qos1(i)-qos2(i)),2.0)
    sum
  }

  def get_centroid(data: RDD[(Long,Array[Double])],D:Int):(Long,Array[Double]) = {
    val buf = data.values.cache()
    val m = buf.count()
    val n = D
    val c = (for {i <- (0 until n).par
                  p = buf.map(x => x(i)).sum() / m.toDouble
    } yield p).toArray
    (-1,c)
  }
  def get_center(data:org.apache.spark.rdd.RDD[(Long,Array[Double])],D:Int): (Long,Array[Double]) = {
    val buf = data.values.cache()
    val m = buf.count()
    val n = D
    val c = (for {i <- (0 until n).par
                  p = buf.map(x => x(i)).sum() / m.toDouble
    } yield p).toArray
    data.map(x => (x, distance(x._2, c))).sortBy(x => x._2).first()._1
  }

  def get_radius_weight(data:org.apache.spark.rdd.RDD[(Long,Array[Double])], center: (Long,Array[Double])): Double ={
    val data_buf=data.cache()
    var rr:Double = 0
    val l = data_buf.count().toDouble - 1
    if(l == 0){
      rr = 0.0
    } else if(l == 1){
      rr = data_buf.filter(x => x != center).map(x =>distance(x._2,center._2)).sum()
    } else {
      val data_dis = data_buf.filter(x => x != center).map(x =>distance(x._2,center._2)).cache()//每个数据点与圆心距离
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
    //
    //    if(l == 0){
    //      rr = 0.0
    //    } else if(l == 1){
    //      rr = data.filter(x => x != center).map(x =>distance(x._2,center._2)).sum()
    //    } else{
    //      val data_dis=data.filter(x => x != center).map(x =>distance(x._2,center._2)).cache()//每个数据点与圆心距离
    //      val sum_dis:Double = data_dis.sum()//距离之和
    //      if(sum_dis==0){
    //        rr=0
    //      }
    //      else {
    //        val data_weight = data_dis.map(x => x/sum_dis).cache()//（权重）
    //        val data_dis_sort:Array[Double] = data_dis.sortBy(x => x).collect()//距离
    //        val data_weight_sort:Array[Double] =data_weight.sortBy(x => x,ascending = false).collect()//权重
    //        for(eg <- 0 until data_dis_sort.length){
    //          rr += data_dis_sort(eg)*data_weight_sort(eg)
    //        }
    ////        val data_dis_sort = data_dis.sortBy(x => x).cache()
    ////        val data_weight_sort =data_weight.sortBy(x => x,ascending = false).cache()
    ////        val data_zip = data_dis_sort.zip(data_weight_sort)
    ////        rr = data_zip.map(x=>x._1*x._2).sum()
    //      }
    //    }
    rr

    //    val c = center._2
    //    val l = data.count().toDouble - 1
    //    if (l == 0) 0.0
    //    else data.filter(x => x != center).map(x => distance(x._2, c)).sum() / l
  }
  def get_radius_centroid(data: RDD[(Long,Array[Double])], center: (Long,Array[Double])): Double ={
    val data_buf=data.cache()
    var rr:Double = 0
    val l = data_buf.count().toDouble
    if(l == 1){
      rr = 0
    } else {
      val data_dis = data_buf.map(x =>distance(x._2,center._2)).cache()//每个数据点与圆心距离
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
    }
    ////    if(l==1){
    ////      rr=0
    ////    }
    ////    else{
    ////      val data_dis=data.filter(x => x != center).map(x =>distance(x._2,center._2)).cache()//每个数据点与圆心距离
    ////      val sum_dis:Double = data_dis.sum()//距离之和
    ////      if(sum_dis==0){
    ////        rr=0
    ////      }
    ////      else{
    ////        val data_weight = data_dis.map(x => x/sum_dis)//（距离,权重）
    ////        val data_dis_sort:Array[Double] = data_dis.sortBy(x => x).collect()//距离
    ////        val data_weight_sort:Array[Double] =data_weight.sortBy(x => x,ascending = false).collect()//权重
    ////        for(eg <- 0 until data_dis_sort.length){
    ////          rr += data_dis_sort(eg)*data_weight_sort(eg)
    ////        }
    ////      }
    ////    }
    //
    rr

    //    val c = center._2
    //    val l = data.count().toDouble
    //    if (l == 1.0) 0.0
    //    else data.map(x => distance(x._2, c)).sum() / l
  }

  def get_covering(data:org.apache.spark.rdd.RDD[(Long,Array[Double])], center: (Long,Array[Double]), radius: Double): RDD[(Long,Array[Double])] = {
    val c = center._2
    data.filter(x => distance(x._2, c) <= radius)
  }

  def get_max(data: IndexedRDD[Long, (Array[Double], Int)], c: Map[Int, Array[Double]]): (Int,Array[Double]) = {
    var re: (Int,Array[Double]) = null
    var max:Double = 0
    var length: Double = 0
    for(t <- c){
      length = data.filter(x => x._2._2 == t._1).count().toDouble
      if(length > max){
        max = length
        re = t
      }
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
    re
  }

  def get_R(train: Map[Int, Array[Double]]): (Int, Int, Double) = {
    var i: Int = 0
    var j: Int = 0
    var S: Double = 0

    for (p <- train.keySet) {
      for (q <- train.keySet if q > p) {

        val d:Double = 1 / (1 + distance(train(p), train(q)))
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
      val d:Double = 1 / (1 + distance(train(p),pt._2 ))
      if (d > S) {
        S = d
        i = p
      }
    }
    (i,j, S)
  }

  def get_delete(p:Array[Double],c:Double):Double={
    val d:Int=p.length
    var re:Double=0.0
    for(eg <- 0 until d){
      re += math.pow(p(eg)-c,2)
    }
    math.sqrt(re/d)
  }

  def get_every_dim(data:IndexedRDD[Long, (Array[Double], Int)],d:Int):Array[Array[Double]]= {//RDD[(Long,Array[Double])]
  val data_buf: Array[Array[Double]] = data.values.keys.collect()
    val len: Int = data_buf.length
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

  def get_class_max(all_class:Map[Int,(Int,Double)],d_i:Map[Int,Double],d_all:Double,data:IndexedRDD[Long, (Array[Double], Int)]):Map[Int,(Int,Double)]={
    var re:Map[Int,(Int,Double)] = Map[Int,(Int,Double)]()
    val ave:Double=all_class.par.map(x=>x._2._2).sum/all_class.size

    for(p <- all_class){
      val value = p._2._2
      if(value>ave/2){
        if((d_i(p._1)>d_all) || data.filter(x => x._2._2==p._1).count().toDouble > data.count().toDouble/6){
          re += p
          println("Test!!"+p._1)
        }
      }
      //println("class label:="+p._1+"max Dim:="+value+"ave:="+ave+",d_all:="+d_all+"d_i(p._1):="+d_i(p._1))
    }
    re
  }

  def updateData_covering(dataSet:IndexedRDD[Long,(Array[Double],Int)],centSet:Map[Int,Array[Double]]):IndexedRDD[Long,(Array[Double],Int)]={//RDD[(Long,Array[Double],Int)]
  val upDataSet:IndexedRDD[Long,(Array[Double],Int)]=IndexedRDD(dataSet.map(x=>{
      val temp:(Long,(Array[Double],Int))=(x._1,(x._2._1,neal_covering(x,centSet)))
      temp
    }))
    upDataSet
  }

  def neal_covering(item:(Long,(Array[Double],Int)),centSet:Map[Int,Array[Double]]):Int={
    var min:Double=1e9
    var index:Int=(-1)
    for(c<-centSet.keySet){
      val dist:Double=distance2(item._2._1,centSet(c))
      if(dist<min){
        min=dist
        index=c
      }
    }
    index
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

}
