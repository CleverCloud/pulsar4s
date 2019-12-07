package com.sksamuel.pulsar4s

import java.util.UUID

import org.apache.pulsar.client.api.Schema
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class FutureAsyncHandlerTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val schema: Schema[String] = Schema.STRING

  private val client = PulsarClient("pulsar://localhost:6650")
  private val topic = Topic("persistent://sample/standalone/ns1/futureasync_" + UUID.randomUUID())

  override def afterAll(): Unit = {
    client.close()
  }

  test("async producer should bring future effect into scope by default") {
    val producer = client.producer(ProducerConfig(topic))
    val f = producer.sendAsync("wibble")
    Await.result(f, Duration.Inf) should not be null
    producer.close()
  }

  test("async consumer should bring future effect into scope by default") {
    val consumer = client.consumer(ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription("mysub_" + UUID.randomUUID)))
    consumer.seekEarliest()
    val f = consumer.receiveAsync
    new String(Await.result(f, Duration.Inf).data) shouldBe "wibble"
    consumer.close()
  }
}
