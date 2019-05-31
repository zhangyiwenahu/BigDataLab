/**
 * Created by wujintao on 17-10-12.
 */
package algorithms
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.plans.logical.Sample
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Random
import scala.collection.mutable.Map


object zyyRDDScalaKmeans {

  def main (args: Array[String]){
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.Server").setLevel(Level.OFF)

    val dataSet:Array[Array[Double]]=Source.fromFile("/home/zyy/IdeaProjects/clustering/src/dataset/Iris_deal.txt").getLines().map(x=>x.split(',').map(x=>x.toDouble)).toArray
    val l = dataSet.length
    val data:Array[(Long,Array[Double],Int)]=new Array[(Long,Array[Double],Int)](l)
    for(i <- 0 until l)
      data(i) = (i.toLong,dataSet(i),-1)


    val conf = new SparkConf().setAppName("RDDspark").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rddData=sc.parallelize(data)

    val t=zyyKmeansN(rddData,3,100,10)//data set,k,iterations,runs
    val da=t._1.collect()
    for(temp<-da){
      print(temp._1+",")
      printArr(temp._2)
      println(","+temp._3)
    }

    //demo.happy()
  }

  //运行N次Kmeans
  def zyyKmeansN(dataSet:org.apache.spark.rdd.RDD[(Long,Array[Double],Int)],k:Int,Iter:Int,N:Int):(org.apache.spark.rdd.RDD[(Long,Array[Double],Int)],Array[(Long,Array[Double],Int)],Int)={
    var bestData:(org.apache.spark.rdd.RDD[(Long,Array[Double],Int)],Array[(Long,Array[Double],Int)],Int)=null
    var bestCost:Double=1e9

    for(i<-0 until N){
      val tempDataSet:org.apache.spark.rdd.RDD[(Long,Array[Double],Int)]=dataSet
      val temp:(org.apache.spark.rdd.RDD[(Long,Array[Double],Int)],Array[(Long,Array[Double],Int)],Int)=runKMtest(tempDataSet,k,Iter)
      val sum:Double=temp._1.map(x=>computCost(x,temp._2)).reduce(_+_)
      println("this the\t"+(i+1)+"th\ttest, the iter is\t"+temp._3+"\tcost="+sum/10000)
      if(sum<bestCost){
        bestData=temp
        bestCost=sum
      }
    }
    println(bestCost)
    return bestData
  }

  //运行kmeans
  def runKMtest(dataS:org.apache.spark.rdd.RDD[(Long,Array[Double],Int)],k:Int,Iter:Int):(org.apache.spark.rdd.RDD[(Long,Array[Double],Int)],Array[(Long,Array[Double],Int)],Int)={
    var rddData=dataS
    val D:Int=dataS.first()._2.length

    val ran=new Random()
    val n=ran.nextInt(10000)

    val sample:Array[(Long,Array[Double],Int)]=plusPreInitK(dataS,k)

    var centSet:Array[(Long,Array[Double],Int)]=new Array[(Long, Array[Double], Int)](k)

    var labelIndex:Int=0
    for(c<-sample){
      val tempC:(Long,Array[Double],Int)=(c._1,c._2,labelIndex)
      centSet(labelIndex)=tempC
      labelIndex=labelIndex+1
    }

    var tempCent:Array[(Long,Array[Double],Int)]=initTemCent(D,k)
    var iter:Int=0
    var flag:Boolean=true

    while(flag){
      rddData=updateData(rddData,centSet)
      centSet=updateCent(rddData,centSet)

      //sum cost
      //val sub:Double=computSub(tempCent,centSet)

      //single cost
      val sub:Double=computSubSingle(tempCent,centSet)

      if(sub<Math.pow(1e-3,2)||iter>Iter)
        flag=false
      else {
        tempCent=centSet
        iter=iter+1
      }
    }
    val re=(rddData,centSet,iter+1)
    re
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
//  def neal(item:(Long,Array[Double],Int),centSet:Array[(Long,Array[Double],Int)]):Int={
//    var min:Double=1e9
//    var index:Int=(-1)
//    for(c<-centSet){
//      val dist:Double=distance(item._2,c._2)
//      if(dist<min){
//        min=dist
//        index=c._3
//      }
//    }
//    index
//  }
  def neal(item:(Long,Array[Double],Int),centSet:Array[(Long,Array[Double],Int)]):Int={
    var min:Double=1e9
    var index:Int=(-1)
    for(c<-centSet){
      val sq_dist:Double=sq_distance(item._2,c._2)
      if(sq_dist<min){
        val dis = distance(item._2,c._2)
        if(dis < min){
          min=dis
          index=c._3
        }
      }
    }
    index
  }

  //计算距离平方
  def distance2(qos1:Array[Double],qos2:Array[Double]):Double={
    val lenth:Int=qos1.length
    var sum:Double=0.0
    for(i<-0 until lenth)
      sum=sum+Math.pow((qos1(i)-qos2(i)),2.0)
    sum
  }

  //计算距离
  def distance(qos1:Array[Double],qos2:Array[Double]):Double={
    val lenth:Int=qos1.length
    var sum:Double=0.0
    for(i<-0 until lenth)
      sum=sum+Math.pow((qos1(i)-qos2(i)),2.0)
    sum=Math.sqrt(sum)
    sum
  }

  //计算2范数
  def sq_distance(qos1:Array[Double],qos2:Array[Double]):Double={
    val lenth:Int=qos1.length
    var sum:Double=0.0
    val a1:Double=math.sqrt((for {x <- qos1.indices
                                  p = math.pow(qos1(x),2)} yield p).sum)
    val b1:Double=math.sqrt((for {x <- qos1.indices
                                  p = math.pow(qos2(x),2)} yield p).sum)
    sum=math.pow(a1-b1,2)
    sum
  }


  //更新聚类中心
  def updateCent(dataSet:org.apache.spark.rdd.RDD[(Long,Array[Double],Int)],centSet:Array[(Long,Array[Double],Int)]):Array[(Long,Array[Double],Int)]={
    val temp=dataSet.groupBy(_._3)
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

  //计算中心点的距离
  def computSub(tempCent:Array[(Long,Array[Double],Int)],cent:Array[(Long,Array[Double],Int)]):Double={
    var sum:Double=0.0
    for(i<-0 until cent.length){
      val c=tempCent.filter(x=>x._3==cent(i)._3)(0)
      val dist=zyyRDDKmeansTest.distanceOther(cent(i)._2,c._2)
      sum=sum+dist
    }
    sum
  }

  //计算中心点的距离
  def computSubSingle(tempCent:Array[(Long,Array[Double],Int)],cent:Array[(Long,Array[Double],Int)]):Double={
    var maxDist:Double=1e-9
    for(i<-0 until cent.length){
      val c=tempCent.filter(x=>x._3==cent(i)._3)(0)
      val dist=zyyRDDKmeansTest.distanceOther(cent(i)._2,c._2)
      if (dist>maxDist)
        maxDist=dist
    }
    maxDist
  }

  //plius 选初始中心
  def plusPreInitK(dataS:org.apache.spark.rdd.RDD[(Long,Array[Double],Int)],k:Int):Array[(Long,Array[Double],Int)]={
    val re:ArrayBuffer[(Long,Array[Double],Int)]=new ArrayBuffer[(Long, Array[Double], Int)]()

    val ran=new Random()
    val n=ran.nextInt(10000)

    val sample:Array[(Long,Array[Double],Int)]=dataS.takeSample(true,Parameters2.R,n)

    re ++=sample

    for(i<-1 until Parameters2.L){

      val tempDataS=dataS.map(x=>(x._1,x._2,x._3,computData_Cent_min(x,re,i*Parameters2.R))).cache()
      val dominate:Double=tempDataS.map(x=>x._4).reduce(_+_)
      val tempDataS2=tempDataS.map(x=>(x._1,x._2,x._3,x._4,x._4/dominate)).cache()
      val tempCent=tempDataS2.filter(x=>{
        val ran=new Random()
        val p:Double=ran.nextDouble()
        if (p<x._5)
          true
        else
          false
      }).cache()

      if(tempCent.count()>Parameters2.R){
        val n=ran.nextInt(10000)
        val c:Array[(Long,Array[Double],Int)]=tempCent.takeSample(true,Parameters2.R,n).map(x=>(x._1,x._2,x._3))
        re ++=c
      }
      else {
        val n=ran.nextInt(10000)
        val c:Array[(Long,Array[Double],Int)]=dataS.takeSample(true,Parameters2.R-tempCent.count().toInt,n)
        re ++=tempCent.map(x=>(x._1,x._2,x._3)).collect()
        re ++=c
      }

    }

    val rlData:Array[(Long,Array[Double],Int)]=distinct(re)
    var s:Set[Long]=Set()
    for(data<- rlData){
      s=s++Set(data._1)
    }
    val weight:Array[(Long,Int)]=dataS.map(x=>(computData_Cent_min_Index(x,rlData),1)).reduceByKey(_+_).collect()
    var mp:Map[Long,Int]=Map()
    for(w<-weight){
      mp=mp++Map(w._1->w._2)
    }
    val rddrldata:org.apache.spark.rdd.RDD[(Long,Array[Double],Int,Int)]=dataS.filter(x=>s.contains(x._1)).map(x=>(x._1,x._2,x._3,mp(x._1))).cache()

    val centPlusplus:Array[(Long,Array[Double],Int)]=plusPreInitKPlus(rddrldata,k)
    val re2=runKMtestPlus(rddrldata.map(x=>(x._1,x._2,x._3)).cache(),k,30,centPlusplus)

    re2
  }

  //计算数据到中心的最小距离
  def computData_Cent_min(item:(Long,Array[Double],Int),currentCents:ArrayBuffer[(Long,Array[Double],Int)],currentI:Int):Double={
    var minre:Double=1e9
    for(i<-0 until currentI){
      val dist2:Double=distance2(item._2,currentCents(i)._2)
      if(dist2<minre)
        minre=dist2
    }
    minre
  }

  //计算数据到中心的最小距离索引
  def computData_Cent_min_Index(item:(Long,Array[Double],Int),currentCents:Array[(Long,Array[Double],Int)]):Long={
    var minre:Double=1e9
    var index:Long=0
    for(i<-0 until currentCents.length){
      val dist2:Double=distance2(item._2,currentCents(i)._2)
      if(dist2<minre){
        minre=dist2
        index=currentCents(i)._1
      }
    }
    index
  }

  //distinct
  def distinct(re:ArrayBuffer[(Long,Array[Double],Int)]):Array[(Long,Array[Double],Int)]={
    var s:Set[Long]=Set()
    val temp:ArrayBuffer[(Long,Array[Double],Int)]=new ArrayBuffer[(Long, Array[Double], Int)]()
    for(i<-0 until re.length){
      if (!s.contains(re(i)._1)){
        temp +=re(i)
        s=s++Set(re(i)._1)
      }
    }
    return temp.toArray
  }

  //plus 选初始中心
  def plusPreInitKPlus(dataS:org.apache.spark.rdd.RDD[(Long,Array[Double],Int,Int)],k:Int):Array[(Long,Array[Double],Int)]={
    val re:Array[(Long,Array[Double],Int)]=new Array[(Long, Array[Double],Int)](k)

    val ran=new Random()
    val n=ran.nextInt(10000)

    val sample:Array[(Long,Array[Double],Int)]=dataS.takeSample(true,1,n).map(x=>(x._1,x._2,x._3))

    re(0)=sample(0)
    var s:Set[Long]=Set()
    s=s++Set(re(0)._1)

    for(i<-1 until k){
      val tempDataS=dataS.map(x=>(x._1,x._2,x._3,x._4,computData_Cent_min_Plus(x,re,i))).cache()
      val dominate:Double=tempDataS.map(x=>x._4*x._5).reduce(_+_)
      val tempDataS2=tempDataS.map(x=>(x._1,x._2,x._3,x._4,x._4*x._5/dominate)).cache()
      val tempCent=tempDataS2.filter(x=>{
        val ran=new Random()
        val p:Double=ran.nextDouble()
        if (p<x._5)
          true
        else
          false
      }).cache()

      if(tempCent.count()>0){
        val n=ran.nextInt(10000)
        val c:Array[(Long,Array[Double],Int)]=tempCent.takeSample(true,1,n).map(x=>(x._1,x._2,x._3))
        //val c:(Long,Array[Double],Int,Double,Double)=tempCent.first()
        //re(i)=(c._1,c._2,c._3)
        re(i)=c(0)
      }
      else {
        val n=ran.nextInt(10000)
        val c:Array[(Long,Array[Double],Int)]=dataS.takeSample(true,1,n).map(x=>(x._1,x._2,x._3))
        re(i)=c(0)
      }

      while(s.contains(re(i)._1)){
        if(tempCent.count()>0){
          val n=ran.nextInt(10000)
          val c:Array[(Long,Array[Double],Int)]=tempCent.takeSample(true,1,n).map(x=>(x._1,x._2,x._3))
          //val c:(Long,Array[Double],Int,Double,Double)=tempCent.first()
          //re(i)=(c._1,c._2,c._3)
          re(i)=c(0)
        }
        else {
          val n=ran.nextInt(10000)
          val c:Array[(Long,Array[Double],Int)]=dataS.takeSample(true,1,n).map(x=>(x._1,x._2,x._3))
          re(i)=c(0)
        }
      }

    }

    re
  }

  //计算数据到中心的最小距离
  def computData_Cent_min_Plus(item:(Long,Array[Double],Int,Int),currentCents:Array[(Long,Array[Double],Int)],currentI:Int):Double={
    var minre:Double=1e9
    for(i<-0 until currentI){
      val dist2:Double=distance2(item._2,currentCents(i)._2)
      if(dist2<minre)
        minre=dist2
    }
    minre
  }

  //运行kmeans
  def runKMtestPlus(dataS:org.apache.spark.rdd.RDD[(Long,Array[Double],Int)],k:Int,Iter:Int,sample:Array[(Long,Array[Double],Int)]):Array[(Long,Array[Double],Int)]={
    var rddData=dataS
    val D:Int=dataS.first()._2.length

    var centSet:Array[(Long,Array[Double],Int)]=new Array[(Long, Array[Double], Int)](k)

    var labelIndex:Int=0
    for(c<-sample){
      val tempC:(Long,Array[Double],Int)=(c._1,c._2,labelIndex)
      centSet(labelIndex)=tempC
      labelIndex=labelIndex+1
    }

    var tempCent:Array[(Long,Array[Double],Int)]=initTemCent(D,k)
    var iter:Int=0
    var flag:Boolean=true

    while(flag){
      rddData=updateData(rddData,centSet)
      centSet=updateCent(rddData,centSet)
      //println(centSet.length)

      //sum cost
      //val sub:Double=computSub(tempCent,centSet)

      //single cost
      val sub:Double=computSubSingle(tempCent,centSet)

      if(sub<Math.pow(0.01,2)||iter>Iter)
        flag=false
      else {
        tempCent=centSet
        iter=iter+1
      }
    }
    val re=(rddData,centSet,iter+1)
    val cent=centSet
    centSet
  }



}
