 import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.Status

object TwitterSentimentScore extends App {

  import Utils._

  val sparkConfiguration = new SparkConf().
    setAppName("spark-twitter-stream-example").
    setMaster(sys.env.get("spark.master").getOrElse("local[*]"))

  val sparkContext = new SparkContext(sparkConfiguration)

  val streamingContext = new StreamingContext(sparkContext, Seconds(5))

  val tweets: DStream[Status] =
    TwitterUtils.createStream(streamingContext, None)

  val uselessWords = sparkContext.broadcast(load("/stop-words.dat"))
  val positiveWords = sparkContext.broadcast(load("/pos-words.dat"))
  val negativeWords = sparkContext.broadcast(load("/neg-words.dat"))

  val textAndSentences: DStream[(TweetText, Sentence)] =
    tweets.
      map(_.getText).
      map(tweetText => (tweetText, wordsOf(tweetText)))

  val textAndMeaningfulSentences: DStream[(TweetText, Sentence)] =
    textAndSentences.
      mapValues(toLowercase).
      mapValues(keepActualWords).
      mapValues(words => keepMeaningfulWords(words, uselessWords.value)).
      filter { case (_, sentence) => sentence.length > 0 }

  val textAndNonNeutralScore: DStream[(TweetText, Int)] =
    textAndMeaningfulSentences.
      mapValues(sentence => computeScore(sentence, positiveWords.value, negativeWords.value)).
      filter { case (_, score) => score != 0 }

  textAndNonNeutralScore.map(makeReadable).print

  streamingContext.start()

  streamingContext.awaitTermination()

}
