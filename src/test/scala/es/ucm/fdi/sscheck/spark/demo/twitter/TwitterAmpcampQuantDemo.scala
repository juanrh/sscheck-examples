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
import es.ucm.fdi.sscheck.prop.tl.{Formula,DStreamTLProperty, Solved}
import es.ucm.fdi.sscheck.prop.tl.Formula._
import es.ucm.fdi.sscheck.gen.{Batch,PDStream,BatchGen}
import es.ucm.fdi.sscheck.gen.BatchGenConversions._
import es.ucm.fdi.sscheck.gen.PDStreamGenConversions._
import es.ucm.fdi.sscheck.matcher.specs2.RDDMatchers._
import org.scalacheck.Gen.const
import es.ucm.fdi.sscheck.gen.PDStreamGen
import es.ucm.fdi.sscheck.spark.demo.twitter.TwitterGen._

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
  val batchInterval = Duration(500) 
  override def batchDuration = batchInterval
  override def defaultParallelism = 4
  override def enableCheckpointing = true
  
  def is = 
    sequential ^ s2"""
      - where if we repeat a DStream N times then we count each hashtag in the first
      batch N times $forallNumRepetitionsLaterCountNumRepetitions
      - where all hashtags eventually get a count of 0 when they get out of 
      the counting window $alwaysEventuallyZeroCount
      - where always when we generate periodic peaks of a random 
      hashtag then the hashtag becomes the top hashtag $alwaysPeakImpliesEventuallyTop
      """        

  private val hashtagRe = """#\S+""".r
  /** Get the expected hashtags for a RDD of Status, as defined by 
   *  the matching with hashtagRe
   */
  private def getExpectedHashtagsForStatuses(statuses: RDD[Status]): RDD[String] = 
    statuses.flatMap { status => hashtagRe.findAllIn(status.getText)}
  
  /** This a a simple property with just now and next as temporal operators, 
   *  but that is able to express the basic condition for counting correctly
   *  and on time, through the usage of Prop.forAll and letter quantifiers 
   *  It states that for any number of repetitions N less or equal to the 
   *  counting window size, and for any random stream, if we repeat the stream 
   *  N times then after the N-1 we will have a count of at least 
   *  (to account for hashtags randomly generated twice) the number of repetitions 
   *  for all the hashtags in the first batch (i.e. quantifying with forAll on 
   *  the first letter of the generated streams).
   */
  def forallNumRepetitionsLaterCountNumRepetitions = {
    type U = (RDD[Status], RDD[(String, Int)])
    val windowSize = 5
    val (numBatches, maxHashtagLength) = (windowSize * 6, 8)

    // numRepetitions should be <= windowSize, as in the worst case each
    // hashtag is generated once per batch before being repeated
      // using Prop.forAllNoShrink because sscheck currently does not support shrinking 
    println("Running forallNumRepetitionsLaterCountNumRepetitions")
    Prop.forAllNoShrink(Gen.choose(1, windowSize)) { numRepetitions =>
      println(s"Testing for numRepetitions = $numRepetitions")
      
      val tweets = BatchGen.ofNtoM(5, 10, 
                                   tweetWithHashtagsOfMaxLen(maxHashtagLength))      
      val gen = for {
        tweets <- BatchGen.always(tweets, numBatches)
        // using tweets as a constant generator, to repeat each generated
        // stream numRepetitions times
        delayedTweets <- PDStreamGen.always(tweets, numRepetitions)
      } yield delayedTweets

      val laterCountNumRepetitions = nextF[U] { case (statuses, _) =>
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
      forAllDStream(
        gen)(
        TweetOps.countHashtags(batchInterval, windowSize)(_))(
        laterCountNumRepetitions)
    }
  }.set(minTestsOk = 15).verbose 

  /** Liveness of TweetOps.countHashtags: always each hashtag eventually 
   *  gets a count of 0, if we generate empty batches at the end of the 
   *  test case so all hashtags get out of the counting window
   */
  def alwaysEventuallyZeroCount = {
    type U = (RDD[Status], RDD[(String, Int)])
    val windowSize = 4   
    val (numBatches, maxHashtagLength) = (windowSize * 4, 8)
    
    // repeat hashtags a bit so counts are bigger than 1   
    val tweets = for {
      hashtags <- Gen.listOfN(6, hashtag(maxHashtagLength))
      tweets <- BatchGen.ofNtoM(5, 10, 
                  addHashtag(Gen.oneOf(hashtags))(tweet(noHashtags=true)))
    } yield tweets
    val emptyTweetBatch = Batch.empty[Status]
    val gen = BatchGen.always(tweets, numBatches) ++ 
              BatchGen.always(emptyTweetBatch, windowSize*2)
    
    val alwaysEventuallyZeroCount = alwaysF[U] { case (statuses, _) =>
      val hashtags = getExpectedHashtagsForStatuses(statuses)
      laterR[U] { case (_, counts) => 
        val countsForStatuses = 
          hashtags
            .map((_, ()))
            .join(counts)
            .map{case (hashtag, (_, count)) => count}
        countsForStatuses should foreachRecord { _ == 0}
      } on windowSize*3
    } during numBatches
    
    println("Running alwaysEventuallyZeroCount")
    forAllDStream(
      gen)(
      TweetOps.countHashtags(batchInterval, windowSize)(_))(
      alwaysEventuallyZeroCount)
  }.set(minTestsOk = 15).verbose
  
  /**
   * Liveness of TweetOps.getTopHashtag: if we superpose random tweets
   * with an periodic peak for a random hashtag, then each peak implies
   * that the corresponding hashtag will eventually be the top hashtag
   */
  def alwaysPeakImpliesEventuallyTop = {
    type U = (RDD[Status], RDD[String])
    val windowSize = 2
    val sidesLen = windowSize * 2 
	  val numBatches = sidesLen + 1 + sidesLen
	  val maxHashtagLength = 8
	  val peakSize = 20
	  
    val emptyTweetBatch = Batch.empty[Status]
    val tweets =
      BatchGen.always(
          BatchGen.ofNtoM(5, 10, 
              tweetWithHashtagsOfMaxLen(maxHashtagLength)), 
          numBatches)      
    val popularTweetBatch = for {
      hashtag <- hashtag(maxHashtagLength)
      batch <-  BatchGen.ofN(peakSize, tweetWithHashtags(List(hashtag)))
    } yield batch
    val tweetsSpike = BatchGen.always(emptyTweetBatch, sidesLen) ++
                           BatchGen.always(popularTweetBatch, 1) ++
                           BatchGen.always(emptyTweetBatch, sidesLen)
    // repeat 6 times the superposition of random tweets 
    // with a sudden spike for a random hashtag
    val gen = Gen.listOfN(6, tweets + tweetsSpike).map{_.reduce(_++_)}
   
    val alwaysAPeakImpliesEventuallyTop = alwaysF[U] { case (statuses, _) => 
      val hashtags = getExpectedHashtagsForStatuses(statuses)
      val peakHashtags = hashtags.map{(_,1)}.reduceByKey{_+_}
                         .filter{_._2 >= peakSize}.keys.cache()
      val isPeak = Solved[U] { ! peakHashtags.isEmpty }
      val eventuallyTop = laterR[U] { case (_, topHashtag) => 
        (topHashtag.subtract(peakHashtags) isEmpty) and
        (peakHashtags.subtract(topHashtag) isEmpty)
      } on numBatches
      
      isPeak ==> eventuallyTop
    } during numBatches * 3 
                                 
    println("Running alwaysPeakImpliesEventuallyTop")
    forAllDStream(
      gen)(
      TweetOps.getTopHashtag(batchInterval, windowSize)(_))(
      alwaysAPeakImpliesEventuallyTop)
  }.set(minTestsOk = 15).verbose
}

