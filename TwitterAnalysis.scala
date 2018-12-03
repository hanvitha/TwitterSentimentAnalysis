// Q4
// COMMAND ----------

import java.time.format.DateTimeFormatter
import com.databricks.hanvitha.Sentiment
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.util.Try

val consumerKey = "yourConsumerKey"
val consumerSecret = "yourConsumerSecret"
val accessToken = "yourAccessToken"
val accessTokenSecret = "youraccessTokenSecret"
val filters = Array("#trump","#usa","festival","thanksgiving")

System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
System.setProperty("twitter4j.oauth.accessToken", accessToken)
System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

val ssc = new StreamingContext(sc, Seconds(5))
val tweets = TwitterUtils.createStream(ssc, None, filters)
tweets.print()
tweets.foreachRDD{(rdd, time) =>
 rdd.map(t => {
   Map(
     "Tweet" -> t.getText,
     "Sentiment" -> Sentiment.detectSentiment(t.getText).toString
   )
 }).saveAsTextFile("/FileStore/twitter/")
}

ssc.start()
println("Started analysing")
ssc.awaitTermination()
println("Done!!!")

