
import java.io.FileInputStream
import java.nio.charset.StandardCharsets
import java.util.Properties

import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object TweetTwitter {
  def main(args: Array[String]): Unit = {
    val pyFile=args(0);
    val outputFile=args(1);

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("CountingSheep")
      .set("spark.driver.allowMultipleContexts","true");
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(1))

   sc.hadoopConfiguration.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    sc.hadoopConfiguration.set("fs.file.impl", classOf[LocalFileSystem].getName)


    val prop=new Properties()

    val input=new FileInputStream("/home/user/Twitter_Live/src/main/scala/twitter.properties");

    prop.load(input);

    System.setProperty("twitter4j.oauth.consumerKey" , prop.getProperty("twitter4j.oauth.consumerKey"));
    System.setProperty("twitter4j.oauth.consumerSecret",prop.getProperty("twitter4j.oauth.consumerSecret"));
    System.setProperty("twitter4j.oauth.accessToken", prop.getProperty("twitter4j.oauth.accessToken"));
    System.setProperty("twitter4j.oauth.accessTokenSecret", prop.getProperty("twitter4j.oauth.accessTokenSecret"));

    val filters = Array("modi","")
    val stream=TwitterUtils.createStream(ssc,None);


    stream.map(
      x=> new String((x.getText).getBytes(StandardCharsets.US_ASCII),StandardCharsets.US_ASCII).replace("?","")//(x.getText).getBytes(StandardCharsets.UTF_8)
    ).foreachRDD(
      y=>
        y.pipe(pyFile).coalesce(1).saveAsTextFile(outputFile)
    )

    ssc.start()
    ssc.awaitTermination()

  }
}