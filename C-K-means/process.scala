import scala.io.Source

/**
 * Created by mycluster on 17-2-13.
 */
object process {
  def main(args: Array[String]) {
    val a=(1 to 138493).map(x=> x.toString)
    val b=(1 to 27278).map(x=> x.toString)
    val data= Source.fromFile("/home/mycluster/IdeaProjects/clustering/src/ratings2.csv").getLines().map(x=>x.split(','))
    //val user= data.map(x=> x(0))
    //val item= data.map(x=> x(1))
    //println(user.size+","+item.size)
    val s_user= for{i<-a
                     p=math.random
                     if p<0.2
                        buf=i} yield buf
    println(s_user.size)
    val s_item= for{i<-b
                     p=math.random
                     if p<0.5
                        buf=i} yield buf
    println(s_item.size)
    val result= for{i<-data
                    if s_user.contains(i(0)) && s_item.contains(i(1))
                        buf=i.mkString(",")} yield buf
    println("33")
    val out = new java.io.FileWriter("/home/mycluster/IdeaProjects/clustering/src/train.txt",true)
    for(i<-result)  {
      out.write(i+'\n')
    }
    out.close()
  }
}
