import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.sksamuel.pulsar4s.{ConsumerConfig, Message, MessageId, ProducerConfig, PulsarClient, Subscription, Topic}
import org.apache.pulsar.client.api.Schema
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration._
import scala.concurrent.Await

class PulsarSourceTest extends FunSuite with Matchers {

  import com.sksamuel.pulsar4s.akka.streams._

  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  test("pulsar source should read messages from a cluster") {

    implicit val schema: Schema[String] = Schema.STRING

    val client = PulsarClient("pulsar://localhost:6650")
    val topic = Topic("persistent://sample/standalone/ns1/sourcetest_" + UUID.randomUUID)
    val config = ProducerConfig(topic)
    val producer = client.producer(config)
    producer.send("a")
    producer.send("b")
    producer.send("c")
    producer.send("d")
    producer.close()

    val createFn = () => client.consumer(ConsumerConfig(Seq(topic), Subscription.generate))
    val f = source(createFn, MessageId.earliest)
      .take(4)
      .runWith(Sink.seq[Message[String]])
    val msgs = Await.result(f, 15.seconds)
    msgs.map(_.value) shouldBe Seq("a", "b", "c", "d")
  }
}
