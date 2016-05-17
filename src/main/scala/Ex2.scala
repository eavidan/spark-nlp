import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.immutable.IndexedSeq

/**
  * Created by eavidan on 18/04/16.
  */
object Ex2 {

  def main(args: Array[String]) {
    val vectorSize = 20000
    val windowSize = 5
    val conf = new SparkConf().setAppName("ex2").setMaster("local[*]").set("spark.default.parallelism", "20")
    val sc = new SparkContext(conf)
    val freqWordsLcl: Map[String, Int] = sc.textFile("resources/freq_list.txt")
      .map { x => // parse (word, count) from file
        val pattern = "([A-Za-z]+)\\t+([0-9]+)"
        val regex = pattern.r
        if (x.matches(pattern)) {
          val regex(word, count) = x
          (word, count.toInt)
        }
        else ("", 0)
      }
      .sortBy(x => x._2)  // sort by count
      .keys
      .top(vectorSize)  // get top [vector size] words
      .zipWithIndex // add index to each word in vector
      .toMap  // save as map of word and its index

    val freqWords = sc.broadcast(freqWordsLcl).value  // map of word and its index

    val resFreqWordsLcl: Map[Int, String] = freqWords.map(x => x._2 -> x._1) + (vectorSize -> "")
    val resFreqWords = sc.broadcast(resFreqWordsLcl).value  // map of index and its word

    val simlexLcl: List[String] = sc.textFile("resources/simlex_words.txt").flatMap(line => line.split(" ")).collect().toList
    val simlex = sc.broadcast(simlexLcl).value  // list of all simlex words

    val corpus: RDD[String] = sc.textFile("resources/wacky_wiki_corpus_en1.words.parsed")//.filter(x => x.contains("iron") || x.contains("bird"))

    val simlex999: RDD[((String, String), (String, Double))] = sc.textFile("resources/SimLex-999.txt").map(line => line.split("\\t"))
      .flatMap{
        case Array(w1, w2, pos, sim) => List(((w1, w2), (pos, sim.toDouble)) ,((w2, w1), (pos, sim.toDouble)))
        case _ => None
      }

    val sentences: RDD[String] = corpus.map(
      _.replaceAll("""\d""", "<!DIGIT!>")
        .trim.split(' ')
    ).flatMap(x => x)

    val wordCountLcl: Map[String, Int] = sentences.flatMap(x => x.split(" ")).map(x => (x, 1)).reduceByKey(_ + _).collect.toMap
    //sc.objectFile("resources/wordCount.obj").collect().toMap
    val wordCount = sc.broadcast(wordCountLcl).value

    // calculate count vectors for each word on each node
    val wordsVectorsByCnt: RDD[(String, Array[Int])] = sentences.mapPartitions(wordsToVecByCnt(vectorSize, windowSize, freqWords, simlex))

    // aggregate counts and calculate Norm across all nodes
    val word2vecCnt: RDD[(String, (Array[Int], Double))] = //sc.objectFile("resources/5/word2vecCnt.obj")
    wordsVectorsByCnt
      .reduceByKey{ case (v1, v2) => // aggregate counts
        v1.zip(v2).map(x => x._1 + x._2)
    }.map{ case (w, v) => {
      // add Norm
      val norm = Math.sqrt(v.map(x => x * x).sum)
      norm match {  // filter words with Norm = 0
        case 0.0 => None
        case _ => Some(w, (v, norm))
      }
    }
    }.flatMap(x => x)
      .cache()

    //word2vecCnt.saveAsObjectFile("resources/5/word2vecCnt.obj")

    val contextCnt: Array[Int] = word2vecCnt.map(x => x._2._1).reduce((a, b) => a.zip(b).map(x => x._1 + x._2))
    val N = contextCnt.sum  // sum(Aij)
    val wN = wordCount.values.sum.toFloat // num of words/contexts in corpus
    val beta = 2

    val word2vecPPMI: RDD[(String, (IndexedSeq[Double], Double))] = //sc.objectFile("resources/5/word2vecPPMI.obj")
      word2vecCnt.map{ case (word, (v, _)) =>
      val vec = v.indices.map { i =>
        val Aij = v(i).toDouble
        if (Aij > 0) {
          val context = resFreqWords(i)
          val pWord = wordCount(word) / wN // Pr(word = i)
          val pContext = wordCount(context) / wN // Pr(context = j)
          val pWordContext = (beta + Aij).toFloat / (beta*wN + N) // Pr(word=i, context=j)
          val ppmi = Math.log(pWordContext / (pWord * pContext))
          Math.max(0, ppmi)
        }
        else 0
      }
      val norm: Double = Math.sqrt(vec.map(x => x*x).sum)
      (word, (vec, norm))
    }.filter(_._2._2 > 0) // filter words with Norm = 0

    word2vecPPMI.saveAsObjectFile("resources/5/word2vecPPMI.obj")

    // cosine similarity

    val word2vecPPMICosSim: RDD[((String, String), Double)] = word2vecPPMI.cartesian(word2vecPPMI).map{ case ((w1, (v1, n1)), (w2, (v2, n2))) =>
      val product = v1.zip(v2).map { case (x, y) => x*y }.sum
      val sim = if (n1 * n2 != 0) product / (n1 * n2) else 0
      ((w1, w2), sim)
    }.cache()

    word2vecPPMICosSim.saveAsTextFile("resources/5/word2vecPPMICosSim.txt")

    val word2vecCntCosSim: RDD[((String, String), Double)] = word2vecCnt.cartesian(word2vecCnt).map { case ((w1, (v1, n1)), (w2, (v2, n2))) =>
      val product = v1.zip(v2).map { case (x, y) => x*y }.sum
      val sim = if (n1 * n2 != 0) product / (n1 * n2) else 0
      ((w1, w2), sim)
    }.cache()

    //word2vecCntCosSim.saveAsTextFile("resources/5/word2vecCntCosSim.txt")

    val ppmiSimSimlex: RDD[((String, String), (Double, (String, Double)))] = word2vecPPMICosSim.join(simlex999)
    val ppmiCorrelation: Double = Statistics.corr(ppmiSimSimlex.map(_._2._1), ppmiSimSimlex.map(_._2._2._2), "spearman")
    val ppmiCorrelationV: Double = Statistics.corr(ppmiSimSimlex.filter{ case ((w1 ,w2),(_, (pos, _))) => pos == "V" }.map(_._2._1), ppmiSimSimlex.filter{ case ((w1 ,w2),(_, (pos, _))) => pos == "V" }.map(_._2._2._2), "pearson")
    val ppmiCorrelationN: Double = Statistics.corr(ppmiSimSimlex.filter{ case ((w1 ,w2),(_, (pos, _))) => pos == "N" }.map(_._2._1), ppmiSimSimlex.filter{ case ((w1 ,w2),(_, (pos, _))) => pos == "N" }.map(_._2._2._2), "pearson")
    val ppmiCorrelationA: Double = Statistics.corr(ppmiSimSimlex.filter{ case ((w1 ,w2),(_, (pos, _))) => pos == "A" }.map(_._2._1), ppmiSimSimlex.filter{ case ((w1 ,w2),(_, (pos, _))) => pos == "A" }.map(_._2._2._2), "pearson")

    println(ppmiCorrelation, ppmiCorrelationV, ppmiCorrelationN, ppmiCorrelationA)

    val cntSimSimlex: RDD[((String, String), (Double, (String, Double)))] = word2vecCntCosSim.join(simlex999)
    val cntCorrelation: Double = Statistics.corr(cntSimSimlex.map(_._2._1), cntSimSimlex.map(_._2._2._2), "spearman")
    val cntCorrelationV: Double = Statistics.corr(cntSimSimlex.filter{ case ((w1 ,w2),(_, (pos, _))) => pos == "V" }.map(_._2._1), cntSimSimlex.filter{ case ((w1 ,w2),(_, (pos, _))) => pos == "V" }.map(_._2._2._2), "pearson")
    val cntCorrelationN: Double = Statistics.corr(cntSimSimlex.filter{ case ((w1 ,w2),(_, (pos, _))) => pos == "N" }.map(_._2._1), cntSimSimlex.filter{ case ((w1 ,w2),(_, (pos, _))) => pos == "N" }.map(_._2._2._2), "pearson")
    val cntCorrelationA: Double = Statistics.corr(cntSimSimlex.filter{ case ((w1 ,w2),(_, (pos, _))) => pos == "A" }.map(_._2._1), cntSimSimlex.filter{ case ((w1 ,w2),(_, (pos, _))) => pos == "A" }.map(_._2._2._2), "pearson")

    println(cntCorrelation, cntCorrelationV, cntCorrelationN, cntCorrelationA)
    println(ppmiCorrelation, ppmiCorrelationV, ppmiCorrelationN, ppmiCorrelationA)

  }

  def wordsToVecByCnt(vectorSize: Int, windowSize: Int, freqWords: Map[String, Int], simlex: List[String]): (Iterator[String]) => Iterator[(String, Array[Int])] = {
    words => {
      var vectors: Map[String, Array[Int]] = Map()
      words.sliding(windowSize).foreach { context =>
        val word: String = context(windowSize / 2)
        if (simlex.contains(word)) {
          if (!vectors.contains(word)) {
            vectors += word -> Array.fill(vectorSize)(0)
          }
          context.foreach {
            x => if (freqWords.contains(x) && !x.equals(word))
              vectors(word)(freqWords(x)) += 1
          }
        }
      }
      println("*************** Done vectors by count ******************")
      vectors.toList.toIterator
    }
  }
}
