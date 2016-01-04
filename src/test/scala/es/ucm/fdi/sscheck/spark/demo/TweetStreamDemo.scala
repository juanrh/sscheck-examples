package es.ucm.fdi.sscheck.spark.demo

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.ScalaCheck
import org.specs2.Specification
import org.specs2.matcher.ResultMatchers
import org.specs2.mock.Mockito
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.{Gen, Prop}

import twitter4j.Status;

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.DStream._

import scalaz.syntax.std.boolean._
    
import es.ucm.fdi.sscheck.spark.streaming.SharedStreamingContextBeforeAfterEach
import es.ucm.fdi.sscheck.prop.tl.{Formula,DStreamProp}
import es.ucm.fdi.sscheck.prop.tl.Formula._
import es.ucm.fdi.sscheck.gen.{PDStreamGen,BatchGen}
import es.ucm.fdi.sscheck.gen.BatchGenConversions._
import es.ucm.fdi.sscheck.gen.PDStreamGenConversions._
import es.ucm.fdi.sscheck.matcher.specs2.RDDMatchers._


/**
 * 
- If a tag from twitter becomes trending topic it eventually returns to a "normal" state. It
might be interesting to use it for real with tags for Messi and other football players during
weekends or Champions League matches. I.e. trending topics are not trending forever
- Users with problems (denoted with a special tag like #helpdeskACME) get the problem solved
(denoted with another special tag like #thanksACME or #solvedACME, so we do not rely on the
client thanking the service)
   . always (hastag request implies eventually response)
   . always(eventually(number of answers == number of responses))
 
 * 
 * * */
@RunWith(classOf[JUnitRunner])
class TweetStreamDemo   
  extends Specification 
  with SharedStreamingContextBeforeAfterEach 
  with ResultMatchers
  with Mockito
  with ScalaCheck  {
  
  // Spark configuration
  override def sparkMaster : String = "local[7]"
  override def batchDuration = Duration(500) 
  override def defaultParallelism = 3
  override def enableCheckpointing = true

  
  // FIXME use serializable mocks http://docs.mockito.googlecode.com/hg/org/mockito/Mockito.html#20
  
  def is = 
    sequential ^ s2"""
      - where $p1
      """
  val tweetGen : Gen[Status] = 
    for {
      text <- arbitrary[String] 
    } yield {
      val status = mock[Status].extraInterface[Serializable]
      status.getText returns text thenReturns text
      status
    }
    
  /** Take a Status generator and return a generator of Status mockst
   *  that adds the specified hashtag to both getText and getHashtagEntities 
   * */
  def addHashtag(hashtag : String)(tweetGen : Gen[Status]) : Gen[Status] = {
    require(hashtag.length > 0 &&  hashtag(0) == '#', 
            s"not empty hashtags starting with '#' are required, found $hashtag" )
    // FIXME generate hashtags and mock getHashtagEntities and 	getText
    for {
      status <- tweetGen
      statusText = status.getText
      pos <- Gen.choose(0, statusText.length)
      (start, end) = statusText.splitAt(pos)
      newText = start + hashtag + end
    } yield ???
  }
      
  def p1 = Prop.forAll(tweetGen){ status =>
    println(s"status [${status}] with text ${status.getText}")
    true
  }.set(minTestsOk = 10).verbose
}