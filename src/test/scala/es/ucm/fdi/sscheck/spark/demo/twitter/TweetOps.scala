package es.ucm.fdi.sscheck.spark.demo.twitter

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration

import twitter4j.Status

/** Test subject
*  
*  Adapted from http://ampcamp.berkeley.edu/3/exercises/realtime-processing-with-spark-streaming.html
* */
object TweetOps {
  def getHashtags(tweets : DStream[Status]) : DStream[String] = {
    val statuses = tweets.map(status => status.getText())
    val words = statuses.flatMap(status => status.split(" "))
    val hashtags = words.filter(word => word.startsWith("#"))
    hashtags.print()
    hashtags
  }
    
  /** Counts the hashtags accumulated in a sliding window with 
   *  size windowSize times the batch interval
   * */
  def countHashtags(tweets : DStream[Status], 
                    batchInterval : Duration, windowSize : Int) : DStream[(String, Int)] = {
    val hashtags = getHashtags(tweets)
    val (windowDuration, windowInterval) = (batchInterval * windowSize,  batchInterval)
    val counts = hashtags.map(tag => (tag, 1))
                         .reduceByKeyAndWindow(_ + _, _ - _, windowDuration, windowInterval)
    counts.checkpoint(batchInterval*100000)
    counts.print()
    counts
  }
    
  /** Get the 10 most popular hashtags in the last 5 minutes
   *  
   *  
   *  
   */
  def getTopHastag(tweets : DStream[Status], 
                        batchInterval : Duration, windowSize : Int) : DStream[String] = {
    val counts = countHashtags(tweets, batchInterval, windowSize)
    val topHashtag = counts.map { case(tag, count) => (count, tag) }
                           .transform(rdd => { 
                                val sorted = rdd.sortByKey(false)
                                rdd.sparkContext.parallelize(sorted.take(1).map(_._2))
                             })
    topHashtag.foreachRDD(rdd =>
      println(s"Top hashtag: ${rdd.take(1).mkString(",")}")
    )
    topHashtag
  }
}