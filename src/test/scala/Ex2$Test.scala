

/**
  * Created by eavidan on 13/05/16.
  */
class Ex2$Test extends FunSuite {
  val freqWords: Map[String, Int] = Map("medium" -> 1, "bad" -> 2, "good" -> 3, "big" -> 4, "small" -> 5, "tiny" -> 6)
  val simlex = List("dog", "cat", "mouse", "rabbit", "small")
  val words = Iterator("your", "small", "dog", "is" ,"a", "big", "cat", "with", "big", "teeth")

  test("wordsToVecByCnt should return vectors for words based on counts") {
    val wordsToVec = Ex2.wordsToVecByCnt(7, 5, freqWords, simlex)
    val actual: Iterator[(String, Array[Int])] = wordsToVec(words)
    val expected = Iterator(("dog", Array(0,0,0,0,0,1)), ("cat", Array(0,0,0,0,2,0)))
    actual.zip(expected).foreach{ case ((w1, v1), (w2, v2)) =>
      assert(w1 === w2)
      v1.zip(v2).foreach( x => assert(x._1 === x._2))
    }
  }

}
