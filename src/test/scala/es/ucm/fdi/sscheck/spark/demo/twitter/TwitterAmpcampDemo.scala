package es.ucm.fdi.sscheck.spark.demo.twitter

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.ScalaCheck
import org.specs2.Specification
import org.specs2.matcher.ResultMatchers
import org.scalacheck.{Gen, Prop}
import twitter4j.Status
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.DStream._
import scalaz.syntax.std.boolean._
import es.ucm.fdi.sscheck.gen.UtilsGen
import es.ucm.fdi.sscheck.prop.tl.{Formula,DStreamTLProperty}
import es.ucm.fdi.sscheck.prop.tl.Formula._
import es.ucm.fdi.sscheck.gen.BatchGen
import es.ucm.fdi.sscheck.gen.BatchGenConversions._
import es.ucm.fdi.sscheck.gen.PDStreamGenConversions._
import es.ucm.fdi.sscheck.matcher.specs2.RDDMatchers._
import org.scalacheck.Gen.const

/**
 * Properties for the Twitter example from http://ampcamp.berkeley.edu/3/exercises/realtime-processing-with-spark-streaming.html  
 * 
 * NOTE we cannot use import Mockito through its integration in Specs2 with org.specs2.mock.Mockito because Spark throws 
 * object not serializable (class: org.specs2.mock.mockito.TheMockitoMocker$$anon$1, value: org.specs2.mock.mockito.TheMockitoMocker$$anon$1@3290b1a6)
 * 
 * NOTE: Even though there is some repeated snippets, code has not been refactored so each
 * property can be self contained
 */
@RunWith(classOf[JUnitRunner])
class TwitterAmpcampDemo   
  extends Specification 
  with DStreamTLProperty
  with ResultMatchers
  with ScalaCheck {
  
  // Spark configuration
  override def sparkMaster : String = "local[*]"
  val batchInterval = Duration(500) 
  override def batchDuration = batchInterval
  override def defaultParallelism = 4
  override def enableCheckpointing = true
  
  def is = 
    sequential ^ s2"""
      - where $getHashtagsOk
      - where $getHashtagsReferenceImplementationOk
      - where $countHashtagsOk
      - where $hashtagsAreAlwasysCounted
      - where $alwaysOnlyOneTopHashtag
      - where $sparkTopUntilScalaTop
      """   
  
  /** Check that TweetOps.getHashtags only returns the hashtags 
   *  that are generated at the generator
   * */
  def getHashtagsOk = {
    type U = (RDD[Status], RDD[String])
    val hashtagBatch = (_ : U)._2
    
    val numBatches = 5
    val possibleHashTags = List("#spark", "#scala", "#scalacheck")
    val tweets = BatchGen.ofNtoM(5, 10, 
                                TwitterGen.tweetWithHashtags(possibleHashTags)
                                )
    val gen = BatchGen.always(tweets, numBatches)
    
    val formula : Formula[U] = always { 
      at(hashtagBatch){ hashtags =>
        hashtags.count > 0 and
        ( hashtags should foreachRecord(possibleHashTags.contains(_)) ) 
      }
    } during numBatches

    forAllDStream[Status,String](
      gen)(
      TweetOps.getHashtags)(
      formula)
  }.set(minTestsOk = 10).verbose
  
  
  val hashtagRe = """#\S+""".r
  /** Get the expected hashtags for a RDD of Status, as defined by 
   *  the matching with hashtagRe
   */
  def getExpectedHashtagsForStatuses(statuses: RDD[Status]): RDD[String] = 
    statuses.flatMap { status => hashtagRe.findAllIn(status.getText)}
  
  /** Here we use the "reference implementation" technique
   *  (see "ScalaCheck: The Definitive Guide" chap 4.3) to check the implementation
   *  of TweetOps.getHashtags based on flatMap with split and then filter with startsWith, 
   *  against a regexp based specification. Also we get a more thorough test than in 
   *  getHashtagsOk, because we use a different 
   *  randomly generated set of hashtags for each batch of each test case
   * */
  def getHashtagsReferenceImplementationOk = {
    type U = (RDD[Status], RDD[String])    
    val (numBatches, maxHashtagLength) = (5, 8)

    val tweets = BatchGen.ofNtoM(5, 10, TwitterGen.tweetWithHashtagsOfMaxLen(maxHashtagLength))                            
    val gen = BatchGen.always(tweets, numBatches)
    
    val formula: Formula[U] = alwaysR[U] { case (statuses, hashtags) => 
      hashtags.subtract(getExpectedHashtagsForStatuses(statuses)) isEmpty
    } during numBatches

    forAllDStream[Status,String](
      gen)(
      TweetOps.getHashtags)(
      formula)
  }.set(minTestsOk = 10).verbose
  
  /**
   *  Test TweetOps.countHashtags by generating an scenario where the
   *  hashtag "#spark" is generated for some time, and then the hashtag
   *  "#scala" is generated for another time, and asserting some formulas
   *  for that escenario that express the expected count behaviour 
   *  
   *  Here we convert from wall-clock time into logical time by defining the
   *  window duration and slide as a multiple of the batch interval. Note
   *  no expressivity is loss because DStream.window already imposes the
   *  constraint of using multiples of the batch interval for the window
   *  duration and slide 
   * */
  def countHashtagsOk = {
    type U = (RDD[Status], RDD[(String, Int)])
    val countBatch = (_ : U)._2
    
    val windowSize = 3
    val (sparkTimeout, scalaTimeout) = (windowSize * 4, windowSize * 2)
    val (sparkTweet, scalaTweet) = 
      (TwitterGen.tweetWithHashtags(List("#spark")), TwitterGen.tweetWithHashtags(List("#scala"))) 
    val (sparkBatchSize, scalaBatchSize) = (2, 1)
    val gen = BatchGen.always(BatchGen.ofN(sparkBatchSize, sparkTweet), sparkTimeout) ++  
              BatchGen.always(BatchGen.ofN(scalaBatchSize, scalaTweet), scalaTimeout)
    
   /* 
    * Note the following formula is false because it is only after some time that the
    * count for "#spark" reaches 2 * windowSize
    * 
    *  val formula : Formula[U] = always { 
         at(countBatch)(_ should existsRecord(_ == ("#spark", 6)))
      } during (scalaTimeout - 2)
    */
    def countNHashtags(hashtag : String)(n : Int)  = 
      at(countBatch)(_ should existsRecord(_ == (hashtag, n : Int)))
    val (countNSparks, countNScalas) = (countNHashtags("#spark")_, countNHashtags("#scala")_)
    val laterAlwaysAllSparkCount =  
      later { 
          always { 
            countNSparks(sparkBatchSize * windowSize)
          } during (sparkTimeout -2) 
      } on (windowSize + 1) 
    val laterScalaCount = 
      later { 
        countNScalas(scalaBatchSize * windowSize)
      } on (sparkTimeout + windowSize + 1)
    val laterSparkCountUntilDownToZero = 
      later { 
        { countNSparks(sparkBatchSize * windowSize) } until {
          countNSparks(sparkBatchSize * (windowSize - 1)) and
            next(countNSparks(sparkBatchSize * (windowSize - 2))) and
            next(next(countNSparks(sparkBatchSize * (windowSize - 3)))) 
          } on (sparkTimeout -2) 
      } on (windowSize + 1)
    val formula : Formula[U] = 
      laterAlwaysAllSparkCount and 
      laterScalaCount and 
      laterSparkCountUntilDownToZero

    forAllDStream[Status,(String,Int)](
      gen)(
      TweetOps.countHashtags(batchInterval, windowSize)(_))(
      formula)
  }.set(minTestsOk = 15).verbose 
  
   /**
   *  Safety of TweetOps.countHashtags: we assert that any arbitrary
   *  generated hastag is never not counted
   * */
  def hashtagsAreAlwasysCounted = {
    type U = (RDD[Status], RDD[(String, Int)])
    val windowSize = 3
    val (numBatches, maxHashtagLength) = (windowSize * 6, 8)
    
    val tweets = BatchGen.ofNtoM(5, 10, TwitterGen.tweetWithHashtagsOfMaxLen(maxHashtagLength))      
    val gen = BatchGen.always(tweets, numBatches)
        
    val alwaysCounted: Formula[U] = alwaysR[U] { case (statuses, counts) =>  
      val expectedHashtags = getExpectedHashtagsForStatuses(statuses)
      val expectedHashtagsWithActualCount = 
        expectedHashtags
         .map((_, ()))
         .join(counts)
         .map{case (hashtag, (_, count)) => (hashtag, count)}
         .cache()
      val countedHashtags = expectedHashtagsWithActualCount.map{_._1}
      val countings = expectedHashtagsWithActualCount.map{_._2}
      
      // all hashtags have been counted
      (countedHashtags.subtract(expectedHashtags) isEmpty) and
      // no count is zero
      (countings should foreachRecord { _ > 0 }) 
    } during numBatches
    
    forAllDStream[Status,(String,Int)](
      gen)(
      TweetOps.countHashtags(batchInterval, windowSize)(_))(
      alwaysCounted)

  }.set(minTestsOk = 15).verbose 
      
  /** Safety of TweetOps.getTopHastag: there is never 
   *  more than one top hashtag
   * */
  def alwaysOnlyOneTopHashtag = {
    type U = (RDD[Status], RDD[String])
    val topHashtagBatch = (_ : U)._2

    val (numBatches, maxHashtagLength) = (5, 8)   
    val tweets = 
      BatchGen.ofNtoM(5, 10, 
                      TwitterGen.tweetWithHashtagsOfMaxLen(maxHashtagLength))

    val gen = BatchGen.always(tweets, numBatches)    
    val formula : Formula[U] = always { 
      at(topHashtagBatch){ hashtags =>
        hashtags.count <= 1 
      }
    } during numBatches
    
    forAllDStream[Status,String](
      gen)(
      TweetOps.getTopHashtag(batchInterval, 2)(_))(
      formula)
  }.set(minTestsOk = 10).verbose
  
  /**
  *  Test TweetOps.getTopHastag by generating an scenario where 
  *  the hashtag "#spark" is the most popular, and then the 
  *  hashtag "#scala" is the most popular, and asserting in 
  *  the output that "#spark" is the most popular hastag 
  *  until "#scala" is the most popular
  *  */
  def sparkTopUntilScalaTop = {
    type U = (RDD[Status], RDD[String])
    
    val windowSize = 1
    val topHashtagBatch = (_ : U)._2
    val scalaTimeout = 6
    val sparkPopular = 
      BatchGen.ofN(5, TwitterGen.tweetWithHashtags(List("#spark"))) +
      BatchGen.ofN(2, TwitterGen.tweetWithHashtags(List("#scalacheck"))) 
    val scalaPopular = 
      BatchGen.ofN(7, TwitterGen.tweetWithHashtags(List("#scala"))) +
      BatchGen.ofN(2, TwitterGen.tweetWithHashtags(List("#scalacheck"))) 
    val gen = BatchGen.until(sparkPopular, scalaPopular, scalaTimeout) 
      
    val formula : Formula[U] = 
      { at(topHashtagBatch)(_ should foreachRecord(_ == "#spark" )) } until {
        at(topHashtagBatch)(_ should foreachRecord(_ == "#scala" ))
      } on (scalaTimeout)
    
    forAllDStream[Status,String](
      gen)(
      TweetOps.getTopHashtag(batchInterval, windowSize)(_))(
      formula)
  }.set(minTestsOk = 15).verbose
  
}

