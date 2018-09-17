package com.sksamuel.pulsar4s

import java.util.UUID

import org.apache.pulsar.client.api.Schema
import org.scalatest.{FunSuite, Matchers}

class ProducerConsumerTest extends FunSuite with Matchers {

  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val schema: Schema[String] = Schema.STRING

  test("producer should return messageId when sending a synchronous messsage") {
    val client = PulsarClient("pulsar://localhost:6650")
    val topic = Topic("persistent://sample/standalone/ns1/test_" + UUID.randomUUID())

    val producer = client.producer(ProducerConfig(topic))
    val messageId = producer.send("wibble").get
    messageId.bytes.length > 0 shouldBe true

    producer.close()
    client.close()
  }

  test("producer and consumer synchronous round trip") {

    val client = PulsarClient("pulsar://localhost:6650")
    val topic = Topic("persistent://sample/standalone/ns1/test_" + UUID.randomUUID())

    val producer = client.producer(ProducerConfig(topic))
    val messageId = producer.send("wibble")
    producer.close()

    val consumer = client.consumer(ConsumerConfig(Seq(topic), Subscription.generate))
    consumer.seek(messageId.get)
    val msg = consumer.receive
    new String(msg.get.data) shouldBe "wibble"
    consumer.close()

    client.close()
  }

  test("consumers on separate subscriptions should have replay") {

    val client = PulsarClient("pulsar://localhost:6650")
    val topic = Topic("persistent://sample/standalone/ns1/test_" + UUID.randomUUID())

    val producer = client.producer(ProducerConfig(topic))
    producer.send("wibble")
    producer.send("wobble")
    producer.send("wubble")
    producer.close()

    val consumer1 = client.consumer(ConsumerConfig(Seq(topic), Subscription.generate))
    consumer1.seek(MessageId.earliest)
    consumer1.receive.get.data shouldBe "wibble".getBytes
    consumer1.receive.get.data shouldBe "wobble".getBytes
    consumer1.receive.get.data shouldBe "wubble".getBytes

    val consumer2 = client.consumer(ConsumerConfig(Seq(topic), Subscription.generate))
    consumer2.seek(MessageId.earliest)
    consumer2.receive.get.data shouldBe "wibble".getBytes
    consumer2.receive.get.data shouldBe "wobble".getBytes
    consumer2.receive.get.data shouldBe "wubble".getBytes

    consumer1.close()
    consumer2.close()
    client.close()
  }
}
