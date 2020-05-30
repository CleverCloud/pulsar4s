package com.sksamuel.pulsar4s.akka.streams

import java.util.UUID

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import com.sksamuel.pulsar4s._
import org.apache.pulsar.client.api.Schema
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Await
import scala.concurrent.duration._

class PulsarMultiSinkTest extends AnyFunSuite with Matchers {

  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val schema: Schema[String] = Schema.STRING

  val client = PulsarClient("pulsar://localhost:6650")

  test("pulsar multi sink should write single-topic messages to pulsar cluster") {
    val topic = Topic("persistent://sample/standalone/ns1/sinktest_" + UUID.randomUUID)

    val producerFn = (topic: Topic) => client.producer(ProducerConfig(topic))
    val f = Source.fromIterator(() => List("a", "b", "c", "d").iterator)
      .map(string => topic -> ProducerMessage(string))
      .runWith(multiSink(producerFn))

    Await.ready(f, 15.seconds)

    val config = ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription.generate)
    val consumer = client.consumer(config)
    consumer.seekEarliest()
    Iterator.continually(consumer.receive(30.seconds).get).take(4).toList.flatten.size shouldBe 4
  }

  test("pulsar multi sink should write multi-topic messages to pulsar cluster") {
    val topic1 = Topic("persistent://sample/standalone/ns1/sinktest_" + UUID.randomUUID)
    val topic2 = Topic("persistent://sample/standalone/ns1/sinktest_" + UUID.randomUUID)

    val producerFn = (topic: Topic) => client.producer(ProducerConfig(topic))
    val f = Source.fromIterator(() =>
      (List("a", "b", "c", "d").map(x => topic1 -> x) ++ List("e", "f", "g", "h").map(x => topic2 -> x)).iterator)
      .map { case (t, s) => t -> ProducerMessage(s) }
      .runWith(multiSink(producerFn))

    Await.ready(f, 15.seconds)

    val config1 = ConsumerConfig(topics = Seq(topic1), subscriptionName = Subscription.generate)
    val consumer1 = client.consumer(config1)
    consumer1.seekEarliest()
    Iterator.continually(consumer1.receive(30.seconds).get).take(4).toList.flatten.size shouldBe 4
    val config2 = ConsumerConfig(topics = Seq(topic2), subscriptionName = Subscription.generate)
    val consumer2 = client.consumer(config2)
    consumer2.seekEarliest()
    Iterator.continually(consumer2.receive(30.seconds).get).take(4).toList.flatten.size shouldBe 4
  }

  test("future done should be completed when stream completes") {
    val topic = Topic("persistent://sample/standalone/ns1/sinktest_" + UUID.randomUUID)

    val producerFn = (topic: Topic) => client.producer(ProducerConfig(topic))
    val f = Source.fromIterator(() => List("a").iterator)
      .map(string => topic -> ProducerMessage(string))
      .runWith(multiSink(producerFn))

    Await.result(f, 15.seconds) shouldBe Done
  }
}
