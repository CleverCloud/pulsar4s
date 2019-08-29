package com.sksamuel.pulsar4s

import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.client.api.SubscriptionType
import org.scalatest.FunSuite
import org.scalatest.Matchers

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.Future
import scala.util.Success
import java.time.Instant

class ProducerConsumerTest extends FunSuite with Matchers {

  implicit val schema: Schema[String] = Schema.STRING

  test("producer should return messageId when sending a synchronous messsage") {
    val client = PulsarClient("pulsar://localhost:6650")
    val topic = Topic("persistent://sample/standalone/ns1/test_" + UUID.randomUUID)

    val producer = client.producer(ProducerConfig(topic))
    val messageId = producer.send("wibble").get
    messageId.bytes.length > 0 shouldBe true

    producer.close()
    client.close()
  }

  test("producer and consumer synchronous round trip") {

    val client = PulsarClient("pulsar://localhost:6650")
    val topic = Topic("persistent://sample/standalone/ns1/test_" + UUID.randomUUID)

    val producer = client.producer(ProducerConfig(topic))
    val messageId = producer.send("wibble")
    producer.close()

    val consumer = client.consumer(ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription.generate))
    consumer.seek(messageId.get)
    val msg = consumer.receive
    new String(msg.get.data) shouldBe "wibble"
    consumer.close()

    client.close()
  }

  test("producer and consumer synchronous round trip with fixed delay") {

    val client = PulsarClient("pulsar://localhost:6650")
    val topic = Topic("persistent://sample/standalone/ns1/test_" + UUID.randomUUID)

    val start = System.currentTimeMillis

    val producer = client.producer(ProducerConfig(topic))
    val messageId = producer.send(ProducerMessage[String]("wibble", 5.seconds))
    producer.close()

    val consumer = client.consumer(ConsumerConfig(
      topics = Seq(topic),
      subscriptionName = Subscription.generate,
      subscriptionType = Some(SubscriptionType.Shared)
    ))
    consumer.seek(messageId.get)
    val msg = consumer.receive
    new String(msg.get.data) shouldBe "wibble"
    consumer.close()

    val end = System.currentTimeMillis()

    assert(end - start >= 5000)

    client.close()
  }

  test("producer and consumer synchronous round trip with deliverAt") {

    val client = PulsarClient("pulsar://localhost:6650")
    val topic = Topic("persistent://sample/standalone/ns1/test_" + UUID.randomUUID)

    val start = System.currentTimeMillis

    val producer = client.producer(ProducerConfig(topic))
    val deliverAt = Instant.now.plusMillis(5.seconds.toMillis)
    val messageId = producer.send(ProducerMessage[String]("wibble", deliverAt))
    producer.close()

    val consumer = client.consumer(ConsumerConfig(
      topics = Seq(topic),
      subscriptionName = Subscription.generate,
      subscriptionType = Some(SubscriptionType.Shared)
    ))
    consumer.seek(messageId.get)
    val msg = consumer.receive
    new String(msg.get.data) shouldBe "wibble"
    consumer.close()

    val end = System.currentTimeMillis()

    assert(end - start >= 5000)

    client.close()
  }

  test("consumers on separate subscriptions should have replay") {

    val client = PulsarClient("pulsar://localhost:6650")
    val topic = Topic("persistent://sample/standalone/ns1/test_" + UUID.randomUUID)

    val producer = client.producer(ProducerConfig(topic))
    producer.send("wibble")
    producer.send("wobble")
    producer.send("wubble")
    producer.close()

    val consumer1 = client.consumer(ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription.generate))
    consumer1.seek(MessageId.earliest)
    consumer1.receive.get.data shouldBe "wibble".getBytes
    consumer1.receive.get.data shouldBe "wobble".getBytes
    consumer1.receive.get.data shouldBe "wubble".getBytes

    val consumer2 = client.consumer(ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription.generate))
    consumer2.seek(MessageId.earliest)
    consumer2.receive.get.data shouldBe "wibble".getBytes
    consumer2.receive.get.data shouldBe "wobble".getBytes
    consumer2.receive.get.data shouldBe "wubble".getBytes

    consumer1.close()
    consumer2.close()
    client.close()
  }

  test("support patterns in consumer") {
    val client = PulsarClient("pulsar://localhost:6650")

    val random = UUID.randomUUID
    val topic1 = Topic(s"persistent://public/default/multitest${random}_1")
    val topic2 = Topic(s"persistent://public/default/multitest${random}_2")

    implicit val executor: ExecutionContextExecutor = ExecutionContext.global

    val latch = new CountDownLatch(1)

    val producer1 = client.producer(ProducerConfig(topic1))
    val producer2 = client.producer(ProducerConfig(topic2))

    @volatile var producing = true

    Future {
      while (producing) {
        producer1.send("wibble")
        producer2.send("bibble")
      }
    }

    Future {

      // let the topics be created
      Thread.sleep(2000)

      val consumer = client.consumer(ConsumerConfig(
        topicPattern = Some(s"persistent://public/default/multitest$random.*".r),
        subscriptionName = Subscription.generate)
      )

      try {
        val set = Iterator.continually(consumer.receive(1.second)).flatMap {
          case Success(Some(msg)) => List(msg)
          case _ => Nil
        }.take(25).map(msg => new String(msg.data)).toSet[String]
        if (set == Set("wibble", "bibble"))
          latch.countDown()
      } catch {
        case t: Throwable =>
          t.printStackTrace()
      }
    }

    latch.await(15, TimeUnit.SECONDS) shouldBe true
    producing = false
    producer1.close()
    producer2.close()
  }
}
