package com.sksamuel.pulsar4s.monix

import java.util.UUID

import com.sksamuel.pulsar4s.{PulsarClient, Subscription, Topic}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class MonixAsyncHandlerTest extends FunSuite with Matchers with BeforeAndAfterAll {

  import MonixAsyncHandler._
  import monix.execution.Scheduler.Implicits.global

  val client = PulsarClient("pulsar://localhost:6650", "sample/standalone/ns1")
  val topic = Topic("persistent://sample/standalone/ns1/monix_" + UUID.randomUUID())

  override def afterAll(): Unit = {
    client.close()
  }

  test("async producer should use monix") {
    val producer = client.producer(topic)
    val t = producer.sendAsync("wibble")
    val f = t.runAsync
    Await.result(f, Duration.Inf) should not be null
    producer.close()
  }

  test("async consumer should use monix") {
    val consumer = client.consumer(topic, Subscription("mysub_" + UUID.randomUUID()))
    consumer.seekEarliest()
    val t = consumer.receiveAsync
    val f = t.runAsync
    new String(Await.result(f, Duration.Inf).data) shouldBe "wibble"
    consumer.close()
  }
}
