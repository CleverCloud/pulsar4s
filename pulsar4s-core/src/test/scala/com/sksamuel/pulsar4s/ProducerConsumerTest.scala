package com.sksamuel.pulsar4s

import org.scalatest.{FunSuite, Matchers}

class ProducerConsumerTest extends FunSuite with Matchers {

  import scala.concurrent.ExecutionContext.Implicits.global

  val client = PulsarClient("pulsar://localhost:6650", "sample/standalone/ns1")
  val topic = Topic("persistent://sample/standalone/ns1/producerconsumertest")

  test("producer should return messageId when sending a synchronous messsage") {
    val producer = client.producer(topic)
    val messageId = producer.send("wibble")
    messageId.bytes.length > 0 shouldBe true
  }

  test("producer and consumer synchronous round trip") {

    val producer = client.producer(topic)
    val messageId = producer.send("wibble")

    val consumer = client.consumer(topic, Subscription("sub1"))
    consumer.seek(messageId)

    val msg = consumer.receive
    new String(msg.data) shouldBe "wibble"
  }

  test("consumers on separate subscriptions should have replay") {
    val topic = Topic("persistent://sample/standalone/ns1/t2")
    val producer = client.producer(topic)
    producer.send("wibble")
    producer.send("wobble")
    producer.send("wubble")

    val consumer1 = client.consumer(topic, Subscription("sub1"))
    consumer1.seek(MessageId.earliest)
    consumer1.receive.data shouldBe "wibble".getBytes
    consumer1.receive.data shouldBe "wobble".getBytes
    consumer1.receive.data shouldBe "wubble".getBytes

    val consumer2 = client.consumer(topic, Subscription("sub2"))
    consumer2.seek(MessageId.earliest)
    consumer2.receive.data shouldBe "wibble".getBytes
    consumer2.receive.data shouldBe "wobble".getBytes
    consumer2.receive.data shouldBe "wubble".getBytes
  }
}
