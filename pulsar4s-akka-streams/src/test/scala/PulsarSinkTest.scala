import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import com.sksamuel.pulsar4s.{ConsumerConfig, ProducerConfig, PulsarClient, Subscription, Topic}
import org.apache.pulsar.client.api.Schema
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration._

class PulsarSinkTest extends FunSuite with Matchers {

  import com.sksamuel.pulsar4s.akka.streams._

  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  test("pulsar sink should write messages to pulsar cluster") {

    implicit val schema: Schema[String] = Schema.STRING

    val client = PulsarClient("pulsar://localhost:6650")
    val topic = Topic("persistent://sample/standalone/ns1/sinktest_" + UUID.randomUUID)

    val producerFn = () => client.producer(ProducerConfig(topic))
    Source.fromIterator(() => List("a", "b", "c", "d").iterator)
      .to(sink(producerFn))
      .run()

    val config = ConsumerConfig(Seq(topic), Subscription.generate)
    val consumer = client.consumer(config)
    consumer.seekEarliest()
    Iterator.continually(consumer.receive(30.seconds).get).take(4).toList.flatten.size shouldBe 4
  }
}
