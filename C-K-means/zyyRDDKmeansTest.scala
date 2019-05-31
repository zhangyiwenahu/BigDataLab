/**
 * Created by wujintao on 17-8-27.
 */
package algorithms
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
import scala.util.Random
import org.apache.log4j.{Level, Logger}

object zyyRDDKmeansTest {
  def main (args: Array[String]){
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.Server").setLevel(Level.OFF)

    val dataSet:Array[Array[Double]]=Source.fromFile("/home/zyy/IdeaProjects/clustering/src/dataset/gauss_normal").getLines().map(x=>x.split(',').map(x=>x.toDouble)).toArray
    val l = dataSet.length
    val data:Array[(Long,Array[Double],Int)]=new Array[(Long,Array[Double],Int)](l)
    for(i <- 0 until l)
      data(i) = (i.toLong,dataSet(i),-1)


    val conf = new SparkConf().setAppName("RDDspark").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rddData=sc.parallelize(data)

    val t=zyyKmeansN(rddData,20,100,1)
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

    val start_Time = System.currentTimeMillis()

    for(i<-0 until N){
      val tempDataSet:org.apache.spark.rdd.RDD[(Long,Array[Double],Int)]=dataSet
      val temp:(org.apache.spark.rdd.RDD[(Long,Array[Double],Int)],Array[(Long,Array[Double],Int)],Int)=runKMtest(tempDataSet,k,Iter)
      println("this the\t"+(i+1)+"th\ttest, the iter is\t"+temp._3)
      val sum:Double=temp._1.map(x=>computCost(x,temp._2)).reduce(_+_)

      println("cost="+sum/10000)

      if(sum<bestCost){
        bestData=temp
        bestCost=sum
      }
    }

    val end_Time = System.currentTimeMillis()
    val time = end_Time - start_Time
    println("K-means Time:="+time)

    return bestData
  }

  //运行kmeans
  def runKMtest(dataS:org.apache.spark.rdd.RDD[(Long,Array[Double],Int)],k:Int,Iter:Int):(org.apache.spark.rdd.RDD[(Long,Array[Double],Int)],Array[(Long,Array[Double],Int)],Int)={
    var rddData=dataS
    val D:Int=dataS.first()._2.length

    val ran=new Random()
    val n=ran.nextInt(10000)

    val sample:Array[(Long,Array[Double],Int)]=dataS.takeSample(true,k,n)
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
    })
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
      val dist=distanceOther(cent(i)._2,c._2)
      sum=sum+dist
    }
    sum
  }

  //计算中心点的距离
  def computSubSingle(tempCent:Array[(Long,Array[Double],Int)],cent:Array[(Long,Array[Double],Int)]):Double={
    var maxDist:Double=1e-9
    for(i<-0 until cent.length){
      val c=tempCent.filter(x=>x._3==cent(i)._3)(0)
      val dist=distanceOther(cent(i)._2,c._2)
      if (dist>maxDist)
        maxDist=dist
    }
    maxDist
  }

  //多个距离计算
  def distanceOther(qos1:Array[Double],qos2:Array[Double]):Double={
    val lenth:Int=qos1.length
    var sum:Double=0.0

    /*
    //欧式距离计算
    for(i<-0 until lenth)
      sum=sum+Math.pow((qos1(i)-qos2(i)),2.0)
    sum=Math.sqrt(sum)*/

    //非欧式距离
    var source1:Double=0.0
    var source2:Double=0.0
    for(i<-0 until lenth){
      source1=source1+Math.pow(qos1(i),2.0)
      source2=source2+Math.pow(qos2(i),2.0)
    }
      sum=Math.abs(Math.sqrt(source1)-Math.sqrt(source2))
    sum
  }

  //计算距离平方
  def distance2(qos1:Array[Double],qos2:Array[Double]):Double={
    val lenth:Int=qos1.length
    var sum:Double=0.0
    for(i<-0 until lenth)
      sum=sum+Math.pow((qos1(i)-qos2(i)),2.0)
    sum
  }


}
