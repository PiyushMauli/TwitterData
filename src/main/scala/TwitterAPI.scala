import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.Status

object TwitterAPI {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("CountingSheep")
      .set("spark.driver.allowMultipleContexts","true");
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(conf, Seconds(1))

    val cb = new ConfigurationBuilder
    cb.setDebugEnabled(true).setOAuthConsumerKey("VvsGUkCCUJjkylh0YnlOyU6GJ")
      .setOAuthConsumerSecret("HDis7Gb2yLzN4DkdgduBKUQv4BllD0SRpMw9Nbd0NoQ0mESu7V")
      .setOAuthAccessToken("963790698402103297-VQyyd3dT0nvrtnCCtHi3YJKD3nG0cBi")
      .setOAuthAccessTokenSecret("vPiBFmzZBcBNPv5eT3xflknMN9rJiVIr2wokiIMXuTH7l ")

    val auth = new OAuthAuthorization(cb.build)
    val tweets = TwitterUtils.createStream(ssc, Some(auth))

    //val tweets = TwitterUtils.createStream(ssc, None)


    //val statuses = tweets.map(status => status.getText())
    //statuses.print()

    //println(tweets);
    //    tweets.saveAsTextFiles("tweets", "json")

    //private val CHECKPOINT_PATH = "checkpoint"
//    def startApp(ssc: StreamingContext): Unit = {
//      ssc.checkpoint(CHECKPOINT_PATH)
//      ssc.start()
//      ssc.awaitTermination()
//    }

    val statuses = tweets.map(status => status.getText)
    val tweetWords = statuses.flatMap(tweetText => tweetText.split(" "))
    val hashtags = tweetWords.filter(word => word.startsWith("#"))
    val hashtagKeyValues = hashtags.map(hashtag => (hashtag, 1))
    //5 minute window sliding every one second
    val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))
    val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy({ case (_, count) => count }, ascending = false))
    sortedResults.print

//    ssc.start()
//    ssc.awaitTermination()
  }
}
