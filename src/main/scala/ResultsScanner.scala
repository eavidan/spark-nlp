import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.ViewMkString

/**
  * Created by eavidan on 15/05/16.
  */
object ResultsScanner {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ex2").setMaster("local[*]")//.set("spark.default.parallelism", "20")
    val sc = new SparkContext(conf)
    val tmp = sc.textFile("resources/5/word2vecCntCosSim.txt").map{ x =>
      val str = x.replaceAll("\\(","").replaceAll("\\)","")
      val pattern = "([a-zA-Z]+),([a-zA-Z]+),(.*)"
      val regex = pattern.r
        val regex(w1, w2, sim) = str
        (w1, w2, sim.toDouble)
    }
      .filter(x => x._3 > 0.988 && x._3 < 0.99)
      //.filter(x => x._1.equals("illusion") && x._2.equals("paragraph"))
      .take(10)
    println(tmp.toList)
  }

}
