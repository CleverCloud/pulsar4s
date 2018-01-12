package com.sksamuel.pulsar4s.cats

import java.util.UUID

import com.sksamuel.pulsar4s.{PulsarClient, Subscription, Topic}
import org.scalatest.{FunSuite, Matchers}

class CatsAsyncHandlerTest extends FunSuite with Matchers {

  import CatsAsyncHandler._

  test("async producer should use cats") {
    val client = PulsarClient("pulsar://localhost:6650", "sample/standalone/ns1")
    val topic = Topic("persistent://sample/standalone/ns1/async_" + UUID.randomUUID())

    val producer = client.producer(topic)
    val t = producer.sendAsync("wibble")
    t.unsafeRunSync() should not be null

    producer.close()
    client.close()
  }

  test("async consumer should use cats") {
    val client = PulsarClient("pulsar://localhost:6650", "sample/standalone/ns1")
    val topic = Topic("persistent://sample/standalone/ns1/async_" + UUID.randomUUID())

    val consumer = client.consumer(topic, Subscription("mysub_" + UUID.randomUUID()))
    consumer.seekEarliest()
    val t = consumer.receiveAsync
    new String(t.unsafeRunSync().data) shouldBe "wibble"

    consumer.close()
    client.close()
  }
}
