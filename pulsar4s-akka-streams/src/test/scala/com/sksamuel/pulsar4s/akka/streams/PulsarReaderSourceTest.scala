package com.sksamuel.pulsar4s.akka.streams

import java.util.UUID
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import com.sksamuel.pulsar4s.{ConsumerMessage, Message, MessageId, ProducerConfig, PulsarClient, ReaderConfig, Topic}
import org.apache.pulsar.client.api.Schema

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.Future
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import com.sksamuel.pulsar4s.PulsarAsyncClient

class PulsarReaderSourceTest extends AnyFunSuite with Matchers {
  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val schema: Schema[String] = Schema.STRING
  implicit val executor: ExecutionContextExecutor = system.dispatcher

  val client: PulsarAsyncClient = PulsarClient("pulsar://localhost:6650")

  test("pulsar reader source should read messages from a cluster") {

    val topic = Topic("persistent://sample/standalone/ns1/sourcetest_" + UUID.randomUUID)
    val config = ProducerConfig(topic)
    val producer = client.producer(config)
    producer.send("a")
    producer.send("b")
    producer.send("c")
    producer.send("d")
    producer.close()

    val createFn = () => client.reader(ReaderConfig(
      topic = topic,
      startMessage = Message(MessageId.earliest)
    ))
    val f = sourceReader(createFn, Some(MessageId.earliest))
      .take(4)
      .runWith(Sink.seq[ConsumerMessage[String]])
    val msgs = Await.result(f, 15.seconds)
    msgs.map(_.value) shouldBe Vector("a", "b", "c", "d")
  }

  test("materialized control value can shut down the reader source") {

    val topic = Topic("persistent://sample/standalone/ns1/sourcetest_" + UUID.randomUUID)
    val config = ProducerConfig(topic)
    val producer = client.producer(config)

    Future {
      while (true) {
        producer.send("a")
        Thread.sleep(500)
      }
    }

    val createFn = () => client.reader(ReaderConfig(
      topic = topic,
      startMessage = Message(MessageId.earliest)
    ))
    val (control, f) = sourceReader(createFn, Some(MessageId.earliest))
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

    val createFn = () => client.reader(ReaderConfig(
      topic = topic,
      startMessage = Message(MessageId.earliest)
    ))
    val (control, f) = sourceReader(createFn, Some(MessageId.earliest))
      .toMat(Sink.seq[ConsumerMessage[String]])(Keep.both)
      .run()

    Thread.sleep(1000)
    val drained = control.drainAndShutdown(f)

    // unless the control shuts down the consumer, the source would never end, and this future would not complete
    val msgs = Await.result(drained, 2.minutes)
    msgs.size should be > 0

    producer.close()
  }
}
