package com.sksamuel.pulsar4s

import java.util.UUID

import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class FutureAsyncHandlerTest extends FunSuite with Matchers with BeforeAndAfterAll {

  import scala.concurrent.ExecutionContext.Implicits.global

  val client = PulsarClient("pulsar://localhost:6650", "sample/standalone/ns1")
  val topic = Topic("persistent://sample/standalone/ns1/futureasync_" + UUID.randomUUID())

  override def afterAll(): Unit = {
    client.close()
  }

  test("async producer should bring future effect into scope by default") {
    val producer = client.producer(topic)
    val f = producer.sendAsync("wibble")
    Await.result(f, Duration.Inf) should not be null
    producer.close()
  }

  test("async consumer should bring future effect into scope by default") {
    val consumer = client.consumer(topic, Subscription("mysub_" + UUID.randomUUID()))
    consumer.seekEarliest()
    val f = consumer.receiveAsync
    new String(Await.result(f, Duration.Inf).data) shouldBe "wibble"
    consumer.close()
  }
}
