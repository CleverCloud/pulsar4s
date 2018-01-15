package com.sksamuel.pulsar4s.streams

import java.util.UUID

import com.sksamuel.pulsar4s.{Message, MessageId, PulsarClient, Topic}
import org.reactivestreams.{Publisher, Subscriber, Subscription}
import org.reactivestreams.tck.{PublisherVerification, TestEnvironment}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.testng.TestNGSuiteLike

class PulsarPublisherVerificationTest
  extends PublisherVerification[Message](
    new TestEnvironment(DEFAULT_TIMEOUT_MILLIS),
    PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS
  ) with TestNGSuiteLike with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    client.close()
  }

  import scala.concurrent.ExecutionContext.Implicits.global

  val client = PulsarClient("pulsar://localhost:6650", "sample/standalone/ns1")

  // we use a random topic so that previous runs don't affect us
  val topic = Topic("persistent://sample/standalone/ns1/reactivepub_" + UUID.randomUUID())

  // we need to pump some messages into pulsar
  private val producer = client.producer(topic)
  for (castle <- castles) {
    producer.send(castle)
  }

  override def maxElementsFromPublisher(): Long = castles.length

  override def boundedDepthOfOnNextAndRequestRecursion: Long = 1

  override def createFailedPublisher(): Publisher[Message] = new Publisher[Message] {
    override def subscribe(subscriber: Subscriber[_ >: Message]): Unit = {
      subscriber.onSubscribe(new Subscription {
        override def cancel(): Unit = ()
        override def request(l: Long): Unit = ()
      })
      subscriber.onError(new RuntimeException("Testing failed publishers"))
    }
  }

  override def createPublisher(max: Long): Publisher[Message] = {
    new PulsarPublisher(client, topic, MessageId.earliest, max)
  }
}

case class Empire(name: String, location: String, capital: String)
