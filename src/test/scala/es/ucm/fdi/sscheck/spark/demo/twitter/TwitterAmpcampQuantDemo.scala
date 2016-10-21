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
import scala.math.max
import es.ucm.fdi.sscheck.gen.UtilsGen
import es.ucm.fdi.sscheck.prop.tl.{Formula,DStreamTLProperty}
import es.ucm.fdi.sscheck.prop.tl.Formula._
import es.ucm.fdi.sscheck.gen.{Batch,PDStream,BatchGen}
import es.ucm.fdi.sscheck.gen.BatchGenConversions._
import es.ucm.fdi.sscheck.gen.PDStreamGenConversions._
import es.ucm.fdi.sscheck.matcher.specs2.RDDMatchers._
import org.scalacheck.Gen.const
import es.ucm.fdi.sscheck.gen.PDStreamGen

/**
 * Properties with generators for the Twitter example from http://ampcamp.berkeley.edu/3/exercises/realtime-processing-with-spark-streaming.html  
 * 
 * NOTE we cannot use import Mockito through its integration in Specs2 with org.specs2.mock.Mockito because Spark throws 
 * object not serializable (class: org.specs2.mock.mockito.TheMockitoMocker$$anon$1, value: org.specs2.mock.mockito.TheMockitoMocker$$anon$1@3290b1a6)
 * 
 * NOTE: Even though there is some repeated snippets, code has not been refactored so each
 * property can be self contained
 */
@RunWith(classOf[JUnitRunner])
class TwitterAmpcampQuantDemo   
  extends Specification 
  with DStreamTLProperty
  with ScalaCheck {
  
  // Spark configuration
  override def sparkMaster : String = "local[*]"
  val batchInterval = Duration(300)  //  FIXME Duration(500) 
  override def batchDuration = batchInterval
  override def defaultParallelism = 4
  override def enableCheckpointing = true
  
  def is = 
    sequential ^ s2"""
      - where $pending getHashtagsOk
      - where $pending countHashtagsAlwasysCounted
      - where $countHashtagsForallNumRepetitionsEventuallyCountNumRepetitions
      - where $pending alwaysEventuallyZeroCount   
      - where $pending onlyOneTopHashtag
      - where $pending sparkTopUntilScalaTop
      """        

  val hashtagRe = """#\S+""".r
  /** Get the expected hashtags for a RDD of Status, as defined by 
   *  the matching with hashtagRe
   */
  def getExpectedHashtagsForStatuses(statuses: RDD[Status]): RDD[String] = 
    statuses.flatMap { status => hashtagRe.findAllIn(status.getText)}
  
  /** By using quantifiers we can use the "reference implementation" technique
   *  (see "ScalaCheck: The Definitive Guide" chap 4.3) to check our implementation
   *  based on flatMap with split and then filter with startsWith, against a regexp
   *  based specification. Also we get a more thorough test, because we use a different 
   *  randomly generated set of hashtags for each batch of each test case
   * */
  def getHashtagsOk = {
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
   *  Safety: never not counted
   * 
   *  Here we convert from wall-clock time into logical time by defining the
   *  window duration and slide as a multiple of the batch interval. Note
   *  no expressivity is loss because DStream.window already imposes the
   *  constraint of using multiples of the batch interval for the window
   *  duration and slide 
   * */
  def countHashtagsAlwasysCounted = {
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
  
  /** This a a simple property with just now and next as temporal operators, 
   *  but that is able to express the basic condition for counting correctly
   *  and on time, through the usage of Prop.forAll. It states that for any
   *  number of repetitions less or equal to the counting window size, and 
   *  for any random stream, if we repeat the stream that number of repetitions,  
   *  then after the number of repetitions -1 we will have a count of at least 
   *  (to account for hashtgas randomly generated twice) the number of repetitions 
   *  for all the hashtags in the first batch.
   *  
   *  This is a such a simple property that maybe we might consider it a sscheck
   *  version of "double coin", or "reverse (reverse list) == list"
   */
  def countHashtagsForallNumRepetitionsEventuallyCountNumRepetitions = {
    type U = (RDD[Status], RDD[(String, Int)])
    val windowSize = 5
    val (numBatches, maxHashtagLength) = (windowSize * 6, 8)

    // numRepetitions should be <= windowSize, as in the worst case each
    // hashtag is generated once per batch before being repeated
      // using Prop.forAllNoShrink because sscheck currently does not support shrinking 
    Prop.forAllNoShrink(Gen.choose(1, windowSize)) { numRepetitions =>
      println(s"Testing for numRepetitions = $numRepetitions")
      
      val tweets = BatchGen.ofNtoM(5, 10, TwitterGen.tweetWithHashtagsOfMaxLen(maxHashtagLength))      
      val gen = for {
        tweets <- BatchGen.always(tweets, numBatches)
        // using tweets as a constant generator, to repeat each generated
        // stream numRepetitions times
        delayedTweets <- PDStreamGen.always(tweets, numRepetitions)
      } yield delayedTweets

      val eventuallyCountNumRepetitions: Formula[U] = nowF[U] { case (statuses, _) =>
        val hashtagsInFirstBatch = getExpectedHashtagsForStatuses(statuses)
        // -2 because we have already consumed 1 batch in the outer nowF, and 
        // we will consume 1 batch in the internal now  
        next(max(numRepetitions-2, 0))(now { case (_, counts) =>
          val countsForHashtagsInFirstBatch = 
            hashtagsInFirstBatch
              .map((_, ()))
              .join(counts)
              .map{case (hashtag, (_, count)) => count}
          countsForHashtagsInFirstBatch should foreachRecord { _ >= numRepetitions }
        }) 
      }
      forAllDStream[Status,(String,Int)](
        gen)(
        TweetOps.countHashtags(batchInterval, windowSize)(_))(
        eventuallyCountNumRepetitions)
    }
  }.set(minTestsOk = 15).verbose 

  
  /** Liveness: always each hashtag eventually gets a count of 0, if we generate empty batches 
   * at the end of the test case so all hashtags get out of the counting window
   */
  def alwaysEventuallyZeroCount = {
    type U = (RDD[Status], RDD[(String, Int)])
    val windowSize = 4   
    val (numBatches, maxHashtagLength) = (windowSize * 4, 8)
    
    val tweets = BatchGen.ofNtoM(5, 10, TwitterGen.tweetWithHashtagsOfMaxLen(maxHashtagLength))
    val emptyTweetBatch = Batch.empty[Status]
    val gen = BatchGen.always(tweets, numBatches) ++ BatchGen.always(tweets, windowSize)
    
    val alwaysEventuallyZeroCount: Formula[U] = alwaysF[U] { case (statuses, _) =>  
        laterR[U] { case (_, counts) => 
          true   
        } on numBatches
    } during numBatches
    
    forAllDStream[Status,(String,Int)](
      gen)(
      TweetOps.countHashtags(batchInterval, windowSize)(_))(
      alwaysEventuallyZeroCount)
  }.set(minTestsOk = 10).verbose
  
  
  
  // safety: always not more than 1 top: no need for quantifiers for this
  // FIXME: can use random hashtags for this like above, no need to use a fixed set, fiex original example
  def onlyOneTopHashtag = {
    type U = (RDD[Status], RDD[String])
    val topHashtagBatch = (_ : U)._2

    val numBatches = 5
    val possibleHashTags = List("#spark", "#scala", "#scalacheck")
    val tweets = BatchGen.ofNtoM(5, 10, 
                                TwitterGen.tweetWithHashtags(possibleHashTags)
                                )
    val gen = BatchGen.always(tweets, numBatches)    
    val formula : Formula[U] = 
      always { 
      at(topHashtagBatch){ hashtags =>
        hashtags.count <= 1 
      }
    } during numBatches
    
    forAllDStream[Status,String](
      gen)(
      TweetOps.getTopHastag(batchInterval, 2)(_))(
      formula)
  }.set(minTestsOk = 10).verbose
  
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
      TweetOps.getTopHastag(batchInterval, windowSize)(_))(
      formula)
  }.set(minTestsOk = 15).verbose
 
}

/*
  def countHashtagsOk = {
    type U = (RDD[Status], RDD[(String, Int)])
    val windowSize = 3
    val (numBatches, maxHashtagLength) = (windowSize * 6, 8)

    val tweets = BatchGen.ofNtoM(5, 10, TwitterGen.tweetWithHashtagsOfMaxLen(maxHashtagLength))                                                 
    val gen = BatchGen.always(tweets, numBatches)
    
    val hashtagRe = """#\S+""".r
    val alwaysEventuallyCounted: Formula[U] = alwaysR[U] { case (statuses, counts) =>  
//      later {
      val hashtag = hashtagRe.findFirstIn(statuses.take(1)(0).getText).get
      println(s"hashtag $hashtag")
      counts should existsRecord (count => count._1 == hashtag && count._2 > 0)
//      } on 1
      
//      true
    } during numBatches
    
    forAllDStream[Status,(String,Int)](
      gen)(
      TweetOps.countHashtags(batchInterval, windowSize)(_))(
      alwaysEventuallyCounted)
 * 
 */