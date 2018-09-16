import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink}
import com.sksamuel.pulsar4s.{ConsumerConfig, Message, MessageId, ProducerConfig, PulsarClient, Subscription, Topic}
import org.apache.pulsar.client.api.Schema
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

class PulsarSourceTest extends FunSuite with Matchers {

  import com.sksamuel.pulsar4s.akka.streams._

  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val schema: Schema[String] = Schema.STRING
  implicit val executor: ExecutionContextExecutor = ExecutionContext.global

  val client = PulsarClient("pulsar://localhost:6650")

  test("pulsar source should read messages from a cluster") {

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

  test("materialized control value can shutdown the source") {

    val topic = Topic("persistent://sample/standalone/ns1/sourcetest_" + UUID.randomUUID)
    val config = ProducerConfig(topic)
    val producer = client.producer(config)

    Future {
      while (true) {
        producer.send("a")
        Thread.sleep(500)
      }
    }

    val createFn = () => client.consumer(ConsumerConfig(Seq(topic), Subscription.generate))
    val (control, f) = source(createFn, MessageId.earliest)
      .toMat(Sink.seq[Message[String]])(Keep.both)
      .run()

    Future {
      Thread.sleep(1000)
      control.close()
    }

    // unless the control shuts down the consumer, the source would never end, and this future would not complete
    val msgs = Await.result(f, 2.minutes)
    msgs.size should be > 0

    producer.close()
  }
}
