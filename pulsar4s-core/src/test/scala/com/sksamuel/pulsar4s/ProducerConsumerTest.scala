package com.sksamuel.pulsar4s

import org.scalatest.{FunSuite, Matchers}

class ProducerConsumerTest extends FunSuite with Matchers {

  import scala.concurrent.ExecutionContext.Implicits.global

  test("producer should return messageId when sending a synchronous messsage") {
    val client = PulsarClient("pulsar://localhost:6650", "sample/standalone/ns1")
    val topic = Topic("persistent://sample/standalone/ns1/producerconsumertest")

    val producer = client.producer(topic)
    val messageId = producer.send("wibble")
    messageId.bytes.length > 0 shouldBe true

    producer.close()
    client.close()
  }

  test("producer and consumer synchronous round trip") {
    val client = PulsarClient("pulsar://localhost:6650", "sample/standalone/ns1")
    val topic = Topic("persistent://sample/standalone/ns1/producerconsumertest")

    val producer = client.producer(topic)
    val messageId = producer.send("wibble")
    producer.close()

    val consumer = client.consumer(topic, Subscription.generate)
    consumer.seek(messageId)
    val msg = consumer.receive
    new String(msg.data) shouldBe "wibble"
    consumer.close()
  }

  test("consumers on separate subscriptions should have replay") {
    val client = PulsarClient("pulsar://localhost:6650", "sample/standalone/ns1")
    val topic = Topic("persistent://sample/standalone/ns1/t2")

    val producer = client.producer(topic)
    producer.send("wibble")
    producer.send("wobble")
    producer.send("wubble")
    producer.close()

    val consumer1 = client.consumer(topic, Subscription.generate)
    consumer1.seek(MessageId.earliest)
    consumer1.receive.data shouldBe "wibble".getBytes
    consumer1.receive.data shouldBe "wobble".getBytes
    consumer1.receive.data shouldBe "wubble".getBytes

    val consumer2 = client.consumer(topic, Subscription.generate)
    consumer2.seek(MessageId.earliest)
    consumer2.receive.data shouldBe "wibble".getBytes
    consumer2.receive.data shouldBe "wobble".getBytes
    consumer2.receive.data shouldBe "wubble".getBytes

    consumer1.close()
    consumer2.close()
    client.close()
  }
}
