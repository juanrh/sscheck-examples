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
 */
@RunWith(classOf[JUnitRunner])
class TwitterAmpcampDemo   
  extends Specification 
  with DStreamTLProperty
  with ResultMatchers
  with ScalaCheck {
  
  // Spark configuration
  override def sparkMaster : String = "local[*]"
  val batchInterval = Duration(200) // Duration(500) 
  override def batchDuration = batchInterval
  override def defaultParallelism = 3
  override def enableCheckpointing = true
  
  def is = 
    sequential ^ s2"""
      - where $countHashtagsOk
      """
    
/*          - where $p1
                - where $getHashtagsOk
 
      * */


  def p1 = {
    val gen = TwitterGen.addHashtag("#pepe")(TwitterGen.tweet())
    Prop.forAll(gen){ status =>
    println(s"status [${status}] with text: ${status.getText}")
    true 
    }
  }.set(minTestsOk = 50).verbose
      
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

    forAllDStream(
      gen)(
      TweetOps.getHashtags)(
      formula)
  }.set(minTestsOk = 2).verbose
      
  
  def countHashtagsOk = {
    type U = (RDD[Status], RDD[(String, Int)])
    val countBatch = (_ : U)._2
    val windowSize = 3
    //val numBatches = windowSize * 4
    val (sparkTimeout, scalaTimeout) = (windowSize * 4, windowSize * 2)
    val (sparkTweet, scalaTweet) = 
      (TwitterGen.tweetWithHashtags(List("#spark")), TwitterGen.tweetWithHashtags(List("#scala"))) 
    val gen = BatchGen.always(BatchGen.ofN(2, sparkTweet), sparkTimeout) ++  
              BatchGen.always(BatchGen.ofN(1, scalaTweet), scalaTimeout)
    
    /**
     * Ideas: siempre 6 de spark hasta sparkTimeout-2, y eventualmente scala vale 3
     * TODO el 6 viene de 2 del tamanio de spark tweet * 3 de windows size
     * */
   /* 
    * Falso pq no ocurre al principio del stream, comentar
    * 
    *  val formula : Formula[U] = always { 
      at(countBatch)(_ should existsRecord(_ == ("#spark", 6)))
      
    } during (scalaTimeout - 2)
    
    TODO Contar pq queda bonito que haga falta el eventually aqui de format natural
    TODO aniadir
     - eventually (#scala,3)
     - despues del always ("#spark", 6) se llega a ("#spark", 0): hacerlo con una 
     propiedad aparte para ir contando poco a poco, esta propiedad NO sustituye el eventually
     por un ("#spark", 6) until ("#spark", 0) pq se tiene ("#spark", 6), ("#spark", 4), ("#spark", 2), ("#spark", 0)
     hasta que se sale de la ventana, lo que si podemos hacer es
     ("#spark", 6) until ( ("#spark", 4) and next("#spark", 2) and next(next(("#spark", 0))) 
     que es precisamente lo que pasa
    * */           
              
    val formula : Formula[U] =
      later { 
        always { 
          at(countBatch)(_ should existsRecord(_ == ("#spark", 2 * windowSize)))
        } during (sparkTimeout -2) //  // 
      } on 4
    forAllDStream(
      gen)(
      TweetOps.countHashtags(_, batchInterval, windowSize))(
      formula)
  }.set(minTestsOk = 5).verbose // FIXME restore .set(minTestsOk = 15).verbose
    
  
}

