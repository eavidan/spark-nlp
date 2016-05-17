
name := "spark-nlp"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.0"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.6.0"

libraryDependencies += "com.medallia.word2vec" % "Word2VecJava" % "0.10.3"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.2.5"