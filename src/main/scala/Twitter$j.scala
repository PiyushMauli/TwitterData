//import java.util.stream.Collectors
//
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import twitter4j.TwitterFactory
//import twitter4j.conf.ConfigurationBuilder
//
//object Twitter$j {
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
//
//    val conf = new SparkConf()
//      .setMaster("local[*]")
//      .setAppName("CountingSheep")
//      .set("spark.driver.allowMultipleContexts","true");
//    val sc = new SparkContext(conf)
//
//    val ssc = new StreamingContext(conf, Seconds(1))
//
//    val cb=new ConfigurationBuilder()
//    cb.setDebugEnabled(true)
//      .setOAuthConsumerKey("VvsGUkCCUJjkylh0YnlOyU6GJ")
//      .setOAuthConsumerSecret("HDis7Gb2yLzN4DkdgduBKUQv4BllD0SRpMw9Nbd0NoQ0mESu7V")
//      .setOAuthAccessToken("963790698402103297-VQyyd3dT0nvrtnCCtHi3YJKD3nG0cBi")
//      .setOAuthAccessTokenSecret("vPiBFmzZBcBNPv5eT3xflknMN9rJiVIr2wokiIMXuTH7l");
//
//    val tf=new TwitterFactory(cb.build())
//
//    val tw=tf.getInstance()
//
//    val single=TwitterFactory.getSingleton()
//
//    val status=single.getHomeTimeline()
//
//    for(x <= status){
//      println()
//    }
//
//    println(twList);
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
