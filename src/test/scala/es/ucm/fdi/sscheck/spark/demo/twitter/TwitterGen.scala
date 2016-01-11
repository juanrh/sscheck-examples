package es.ucm.fdi.sscheck.spark.demo.twitter

import org.scalacheck.Gen
 
import twitter4j.Status

import org.mockito.Mockito._

import es.ucm.fdi.sscheck.gen.UtilsGen

/** Generators for tweets
 */
object TwitterGen {
    /** Generator of mocks of Twitter4J Status objects with a getText method
 		 *  that returns texts of up to 140 characters
 		 *  
 		 *  @param noHashtags if true no hashtags are generated in the 
 		 *  tweet text
   	 * */
    def tweet(noHashtags : Boolean = true) : Gen[Status] = {
      val maxWordLength = 8
      val wordGen = for {
        letters <- UtilsGen.containerOfNtoM[List, Char](1, maxWordLength,  Gen.alphaChar) 
        cleanLetters = if (noHashtags && letters(0) == '#') letters.slice(1, maxWordLength)
                       else letters
      } yield cleanLetters.mkString
      for {
        words <- UtilsGen.containerOfNtoM[List, String](1, 20,  wordGen)
      } yield {
        val text = words.mkString(" ").slice(0, 140)
        val status = mock(classOf[Status], withSettings().serializable())
        when(status.getText).thenReturn(text)
         // This is to control what ScalaCheck shows when a counterexample
         // is found. Also this way DStream.print() shows something useful
        when(status.toString).thenReturn(text)
        status
      }
    }
    
    /** Take a Status generator and return a generator of Status mocks
     *  that adds the specified hashtag to getText
   	 * */
    def addHashtag(hashtagGen : Gen[String])
                  (tweetGen : Gen[Status]) : Gen[Status] = 
      for {
        hashtag <- hashtagGen
        status <- tweetGen
        statusText = status.getText
        pos <- Gen.choose(0, statusText.length)
        (start, end) = statusText.splitAt(pos)
        textWithHashtag = s"$start $hashtag $end"
      } yield {
        require(hashtag.length > 0 &&  hashtag(0) == '#', 
                s"not empty hashtags starting with '#' are required, found $hashtag" )
        when(status.getText).thenReturn(textWithHashtag)
        when(status.toString).thenReturn(textWithHashtag)
        status
      }
      
     def tweetWithHashtags(possibleHashTags : Seq[String]) : Gen[Status] = 
       addHashtag(Gen.oneOf(possibleHashTags))(tweet(noHashtags=true))
}