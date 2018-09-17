import java.util.UUID

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import com.sksamuel.pulsar4s.{ConsumerConfig, ProducerConfig, ProducerMessage, PulsarClient, Subscription, Topic}
import org.apache.pulsar.client.api.Schema
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class PulsarSinkTest extends FunSuite with Matchers {

  import com.sksamuel.pulsar4s.akka.streams._

  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val schema: Schema[String] = Schema.STRING

  val client = PulsarClient("pulsar://localhost:6650")

  test("pulsar sink should write messages to pulsar cluster") {
    val topic = Topic("persistent://sample/standalone/ns1/sinktest_" + UUID.randomUUID)

    val producerFn = () => client.producer(ProducerConfig(topic))
    val f = Source.fromIterator(() => List("a", "b", "c", "d").iterator)
      .map(string => ProducerMessage(string))
      .runWith(sink(producerFn))

    Await.ready(f, 15.seconds)

    val config = ConsumerConfig(Seq(topic), Subscription.generate)
    val consumer = client.consumer(config)
    consumer.seekEarliest()
    Iterator.continually(consumer.receive(30.seconds).get).take(4).toList.flatten.size shouldBe 4
  }

  test("future done should be completed when stream completes") {
    val topic = Topic("persistent://sample/standalone/ns1/sinktest_" + UUID.randomUUID)

    val producerFn = () => client.producer(ProducerConfig(topic))
    val f = Source.fromIterator(() => List("a").iterator)
      .map(string => ProducerMessage(string))
      .runWith(sink(producerFn))

    Await.result(f, 15.seconds) shouldBe Done
  }
}
