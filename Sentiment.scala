// Sentiment Class
package com.databricks.hanvitha
import java.util.Properties
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import scala.collection.JavaConversions._

import scala.collection.mutable.ListBuffer

object Sentiment {
  val nlp = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")
    props
  }

  def detectSentiment(message: String): SENTIMENT_TYPE = {
    val pipeline = new StanfordCoreNLP(nlp)
    val annotation = pipeline.process(message)
    var sentiments: ListBuffer[Double] = ListBuffer()
    var sizes: ListBuffer[Int] = ListBuffer()
    var longest = 0
    var mainSentiment = 0

    for (tweet <- annotation.get(classOf[CoreAnnotations.SentencesAnnotation])) {
      val tree = tweet.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])
      val sentiment = RNNCoreAnnotations.getPredictedClass(tree)
      val partText = tweet.toString
      if (partText.length() > longest) {
        mainSentiment = sentiment
        longest = partText.length()
      }
      sentiments += sentiment.toDouble
      sizes += partText.length
    }
    val averageSentiment:Double = {
      if(sentiments.size > 0) sentiments.sum / sentiments.size
      else -1
    }
    val sentimentsW = (sentiments, sizes).zipped.map((sentiment, size) => sentiment * size)
    var sentimentWeight = sentimentsW.sum / (sizes.fold(0)(_ + _))
    if(sentiments.size == 0) {
      mainSentiment = -1
      sentimentWeight = -1
    }
    /*      1 -> negative      2 -> neutral      3 -> positive      */
    sentimentWeight match {
      case s if s <= 0.0 => UNDEFINED
      case s if s < 2.0 => NEGATIVE
      case s if s < 3.0 => NEUTRAL
      case s if s < 5.0 => POSITIVE
      case s if s > 5.0 => UNDEFINED
    }
  }
  trait SENTIMENT_TYPE
  case object NEGATIVE extends SENTIMENT_TYPE
  case object NEUTRAL extends SENTIMENT_TYPE
  case object POSITIVE extends SENTIMENT_TYPE
  case object UNDEFINED extends SENTIMENT_TYPE
}

