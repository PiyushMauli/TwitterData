
import java.io.FileInputStream
import java.nio.charset.StandardCharsets
import java.util.Properties

import com.amazonaws.services.s3.internal.Constants
import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.jets3t.service.Jets3tProperties
import org.uncommons.maths.statistics.DataSet



object jets3s {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("CountingSheep")
      .set("spark.driver.allowMultipleContexts","true");
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(conf, Seconds(1))


    val prop=new Properties()

    val input=new FileInputStream("/home/user/Twitter_Live/src/main/scala/twitter.properties");

    prop.load(input);

    System.setProperty("twitter4j.oauth.consumerKey" , prop.getProperty("twitter4j.oauth.consumerKey"));
    System.setProperty("twitter4j.oauth.consumerSecret",prop.getProperty("twitter4j.oauth.consumerSecret"));
    System.setProperty("twitter4j.oauth.accessToken", prop.getProperty("twitter4j.oauth.accessToken"));
    System.setProperty("twitter4j.oauth.accessTokenSecret", prop.getProperty("twitter4j.oauth.accessTokenSecret"));

    val filters = Array("modi","")
    val stream=TwitterUtils.createStream(ssc,None);

    //    val hashTag=stream.flatMap(status => status.getText.split("\n"))

    //    val top=hashTag.map((_ , 1)).reduceByKeyAndWindow(_ + _,Seconds(20)).map{case(topic,count)=>(count,topic)}.transform(_.sortByKey(false))


    //    val listRDD=List(top);
    //    val dataRDD=sc.makeRDD(listRDD,1)
    //
    //    val pipeRDD=dataRDD.pipe("/home/user/Twitter_Live/src/main/tweets_prediction.py");
    //
    //    pipeRDD.collect.foreach(println)



    //    top.foreachRDD(rdd =>{
    //     val topList=rdd.take(1)
    //     val listRDD=List(topList+" "+"/home/user/Twitter_Live/src/main/model.pkl")
    //         val dataRDD = sc.makeRDD(listRDD,1)
    //
    //       val pipeRDD = dataRDD.pipe("/home/user/Twitter_Live/src/main/tweets_prediction.py");
    //
    //       pipeRDD.collect.foreach(println)
    //
    //     })


    stream.map(
      x=> new String((x.getText).getBytes(StandardCharsets.US_ASCII),StandardCharsets.US_ASCII).replace("?","")//(x.getText).getBytes(StandardCharsets.UTF_8)
    ).foreachRDD(
      y=>
        y.pipe("/home/user/Twitter_Live/src/main/tweets_prediction.py").coalesce(1).saveAsTextFile("s3://spark.program/Piyush ")
    )

    //    stream.foreachRDD(rdd => new String((rdd.getText).getBytes(StandardCharsets.US_ASCII),StandardCharsets.US_ASCII).replace("?","")
    ////      val topList=rdd.take(1).map(x=> x.getText)
    //
    //      topList.foreach(staus =>
    //
    //        .pipe("/home/user/Twitter_Live/src/main/tweets_prediction.py").collect().foreach(println)))
    ////          rdd.foreach(y=>
    ////            sc.parallelize((y.getText),1).pipe("/home/user/Twitter_Live/src/main/tweets_prediction.py")
    ////              .collect().foreach(println)
    ////          )
    //
    //
    ////      topList.foreach({
    ////        case (tag) =>println("%s".format(tag.getText))
    ////
    ////      })
    //    })


    ssc.start()
    ssc.awaitTermination()


  }
}