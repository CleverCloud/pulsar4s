package com.sksamuel.pulsar4s.akka.streams

import java.util.UUID

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import com.sksamuel.pulsar4s.ConsumerConfig
import com.sksamuel.pulsar4s.ConsumerMessage
import com.sksamuel.pulsar4s.MessageId
import com.sksamuel.pulsar4s.ProducerConfig
import com.sksamuel.pulsar4s.PulsarClient
import com.sksamuel.pulsar4s.Subscription
import com.sksamuel.pulsar4s.Topic
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.client.api.SubscriptionInitialPosition

import scala.concurrent.Await
import scala.concurrent.ExecutionContextExecutor
import scala.util.Failure
import scala.util.Try
import scala.concurrent.Future
import scala.concurrent.duration._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.pulsar.client.api.PulsarClientException


class PulsarCommittableSourceTest extends AnyFunSuite with Matchers {

  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val schema: Schema[String] = Schema.STRING
  implicit val executor: ExecutionContextExecutor = system.dispatcher

  private val client = PulsarClient("pulsar://localhost:6650")

  private val specialStringSchema = new Schema[String] {
    override def encode(message: String) = ("message:" + message).getBytes("UTF-8")
    override def decode(data: Array[Byte]) = new String(data, "UTF-8").split(":", 2)(1)
    override def getSchemaInfo = Schema.STRING.getSchemaInfo
  }

  test("pulsar committableSource should read messages from a cluster") {

    val topic = Topic("persistent://sample/standalone/ns1/sourcetest_" + UUID.randomUUID)
    val config = ProducerConfig(topic)
    val producer = client.producer(config)
    producer.send("a")
    producer.send("b")
    producer.send("c")
    producer.send("d")
    producer.close()

    val createFn = () => client.consumer(ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription.generate))
    val f = committableSource(createFn, Some(MessageId.earliest))
      .take(4)
      .runWith(Sink.seq[CommittableMessage[String]])
    val msgs = Await.result(f, 15.seconds)
    msgs.map(_.message.value) shouldBe Seq("a", "b", "c", "d")
  }

  test("pulsar committableSource should not fail when schema throws an exception") {

    val topic = Topic("persistent://sample/standalone/ns1/sourcetest_" + UUID.randomUUID)
    val config = ProducerConfig(topic)
    val producer = client.producer(config)
    producer.send("test")
    producer.close()

    val createFn = () => client.consumer(
      ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription.generate
    ))(specialStringSchema)
    val f = committableSource(createFn, Some(MessageId.earliest)).take(1).runWith(Sink.seq[CommittableMessage[String]])
    val msgs = Await.result(f, 15.seconds)
    msgs.headOption.flatMap(_.message.valueTry.toOption) shouldBe None
  }

  test("pulsar committableSource should fail when creation function throws an exception") {

    val topic = Topic("persistent://sample/standalone/ns1/sourcetest_" + UUID.randomUUID)
    val config = ProducerConfig(topic)
    val producer = client.producer(config)
    producer.send("test")
    producer.close()

    val exception = new RuntimeException("failed!")
    def createFn(shouldThrow: Boolean) = () => {
      if (shouldThrow) throw exception
      client.consumer(
        ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription.generate
      ))(specialStringSchema)
    }
    val successFt = committableSource(
      createFn(shouldThrow = false),
      Some(MessageId.earliest)
    ).take(1).runWith(Sink.seq[CommittableMessage[String]])
    val msgs = Await.result(successFt, 15.seconds)
    msgs.headOption.flatMap(_.message.valueTry.toOption) shouldBe None
    val failureFt = committableSource(
      createFn(shouldThrow = true),
      Some(MessageId.earliest)
    ).take(1).runWith(Sink.seq[CommittableMessage[String]])
    Try(Await.result(failureFt, 15.seconds)) shouldBe Failure(exception)
  }

  test("shutdown of failed source does not cause an exception") {

    val topic = Topic("persistent://sample/standalone/ns1/sourcetest_" + UUID.randomUUID)
    val config = ProducerConfig(topic)
    val producer = client.producer(config)
    producer.send("test")
    producer.close()

    val exception = new RuntimeException
    val createFn = () => throw exception
    val (control, doneFt) = committableSource(createFn, Some(MessageId.earliest))
      .toMat(Sink.ignore)(Keep.both).run()
    Try(Await.result(doneFt, 15.seconds)) shouldBe Failure(exception)
    Await.result(control.shutdown(), 1.second) shouldBe Done
  }

  test("pulsar committableSource should acknowledge messages") {

    val topic = Topic("persistent://sample/standalone/ns1/sourcetest_" + UUID.randomUUID)
    val config = ProducerConfig(topic)
    val producer = client.producer(config)
    producer.send("a")
    producer.send("b")
    producer.send("c")
    producer.send("d")
    producer.close()

    val createFn = () => client.consumer(ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription.generate))
    val f = committableSource(createFn, Some(MessageId.earliest))
      .take(4)
      .mapAsync(10)(msg => msg.ack().map(_ => msg.message))
      .runWith(Sink.seq[ConsumerMessage[String]])

    val msgs = Await.result(f, 15.seconds)
    msgs.map(_.value) shouldBe Seq("a", "b", "c", "d")
  }

  test("pulsar committableSource should negatively acknowledge messages") {

    val topic = Topic("persistent://sample/standalone/ns1/sourcetest_" + UUID.randomUUID)
    val config = ProducerConfig(topic)
    val producer = client.producer(config)
    producer.send("a")
    producer.send("b")
    producer.send("c")
    producer.send("d")
    producer.close()

    val createFn = () => client.consumer(ConsumerConfig(
      topics = Seq(topic),
      subscriptionName = Subscription.generate,
      negativeAckRedeliveryDelay = Some(5.seconds)
    ))
    val f = committableSource(createFn, Some(MessageId.earliest))
      .take(4)
      .mapAsync(10) { msg =>
        if (msg.message.value < "c") {
          msg.nack().map(_ => Vector.empty)
        } else {
          msg.ack().map(_ => Vector(msg.message))
        }
      }
      .mapConcat(identity)
      .runWith(Sink.seq[ConsumerMessage[String]])

    val msgs = Await.result(f, 15.seconds)
    msgs.map(_.value) shouldBe Seq("c", "d")
  }

  test("pulsar committableSource should acknowledge messages on multiple topics") {

    val topics = (0 to 2).map(_ => Topic("persistent://sample/standalone/ns1/sourcetest_" + UUID.randomUUID))
    val n = topics.size * 4
    Await.ready(Future.sequence(topics.map { topic =>
      Future {
        val config = ProducerConfig(topic)
        val producer = client.producer(config)
        producer.send("a")
        producer.send("b")
        producer.send("c")
        producer.send("d")
        producer.close()
      }
    }), 10.seconds)

    val createFn = () => client.consumer(ConsumerConfig(
      topics = topics,
      subscriptionName = Subscription.generate,
      subscriptionInitialPosition = Some(SubscriptionInitialPosition.Earliest)
    ))
    val f = committableSource(createFn)
      .take(n)
      .mapAsync(10)(msg => msg.ack().map(_ => msg.message))
      .runWith(Sink.seq[ConsumerMessage[String]])

    val msgs = Await.result(f, 30.seconds)
    msgs should have size n
    msgs.map(_.value).distinct shouldBe Seq("a", "b", "c", "d")
  }

  test("materialized control value can shut down the source") {

    val topic = Topic("persistent://sample/standalone/ns1/sourcetest_" + UUID.randomUUID)
    val config = ProducerConfig(topic)
    val producer = client.producer(config)

    Future {
      while (true) {
        producer.send("a")
        Thread.sleep(500)
      }
    }

    val createFn = () => client.consumer(ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription.generate))
    val (control, f) = committableSource(createFn, Some(MessageId.earliest))
      .mapAsync(10)(msg => msg.ack().map(_ => msg.message))
      .toMat(Sink.seq[ConsumerMessage[String]])(Keep.both)
      .run()

    Future {
      Thread.sleep(1000)
      control.complete()
    }

    // unless the control shuts down the consumer, the source would never end, and this future would not complete
    val msgs = Await.result(f, 2.minutes)
    msgs.size should be > 0

    Await.result(control.shutdown(), 5.seconds)

    producer.close()
  }

  test("materialized control value can drain and shut down the source") {

    val topic = Topic("persistent://sample/standalone/ns1/sourcetest_" + UUID.randomUUID)
    val config = ProducerConfig(topic)
    val producer = client.producer(config)

    Future {
      while (true) {
        producer.send("a")
        Thread.sleep(500)
      }
    }

    val createFn = () => client.consumer(ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription.generate))
    val (control, f) = committableSource(createFn, Some(MessageId.earliest))
      .mapAsync(10)(msg => msg.ack().map(_ => msg.message))
      .toMat(Sink.seq[ConsumerMessage[String]])(Keep.both)
      .run()

    Thread.sleep(1000)
    val drained = control.drainAndShutdown(f)

    // unless the control shuts down the consumer, the source would never end, and this future would not complete
    val msgs = Await.result(drained, 2.minutes)
    msgs.size should be > 0

    producer.close()
  }

  test("consumer is already closed when drainAndShutdown completes successfully") {

    val topic = Topic("persistent://sample/standalone/ns1/sourcetest_" + UUID.randomUUID)
    val config = ProducerConfig(topic)
    val producer = client.producer(config)

    Future {
      while (true) {
        producer.send("a")
        Thread.sleep(500)
      }
    }

    val consumer = client.consumer(ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription.generate))
    val createFn = () => consumer
    val (control, f) = committableSource(createFn, Some(MessageId.earliest))
      .mapAsync(10)(msg => msg.ack().map(_ => msg.message))
      .toMat(Sink.seq[ConsumerMessage[String]])(Keep.both)
      .run()

    Thread.sleep(1000)
    val drained = control.drainAndShutdown(f)

    // unless the control shuts down the consumer, the source would never end, and this future would not complete
    val msgs = Await.result(drained, 2.minutes)
    msgs.size should be > 0

    the[PulsarClientException] thrownBy consumer.receive.get should have message "Consumer already closed"

    producer.close()
  }

  test("consumer closes after delay when stopped") {

    val topic = Topic("persistent://sample/standalone/ns1/sourcetest_" + UUID.randomUUID)
    val config = ProducerConfig(topic)
    val producer = client.producer(config)

    Future {
      while (true) {
        producer.send("a")
        Thread.sleep(500)
      }
    }

    val consumer = client.consumer(ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription.generate))
    val createFn = () => consumer
    val (control, f) = committableSource(createFn, Some(MessageId.earliest), closeDelay = 2.seconds)
      .mapAsync(10)(msg => msg.ack().map(_ => msg.message))
      .toMat(Sink.seq[ConsumerMessage[String]])(Keep.both)
      .run()

    Thread.sleep(1000)

    control.complete()

    val msgs = Await.result(f, 1.second)
    msgs.size should be > 0
    noException should be thrownBy consumer.receive.get
    Thread.sleep(2.seconds.toMillis)
    the[PulsarClientException] thrownBy consumer.receive.get should have message "Consumer already closed"

    producer.close()
  }
}
