package com.sksamuel.pulsar4s.pekko.streams

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import com.sksamuel.pulsar4s.ProducerMessage

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import com.sksamuel.pulsar4s.{ Consumer, Producer, PulsarAsyncClient }

object Example {

  import com.sksamuel.pulsar4s.{ConsumerConfig, MessageId, ProducerConfig, PulsarClient, Subscription, Topic}
  import org.apache.pulsar.client.api.Schema

  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: Materializer = Materializer.apply(system)
  implicit val schema: Schema[Array[Byte]] = Schema.BYTES

  val client: PulsarAsyncClient = PulsarClient("pulsar://localhost:6650")

  val intopic: Topic = Topic("persistent://sample/standalone/ns1/in")
  val outtopic: Topic = Topic("persistent://sample/standalone/ns1/out")

  val consumerFn: () => Consumer[Array[Byte]] = () => client.consumer(ConsumerConfig(topics = Seq(intopic), subscriptionName = Subscription("mysub")))
  val producerFn: () => Producer[Array[Byte]] = () => client.producer(ProducerConfig(outtopic))

  val control = source(consumerFn, Some(MessageId.earliest))
    .map { consumerMessage => ProducerMessage(consumerMessage.data) }
    .to(sink(producerFn)).run()

  Await.result(control.shutdown(), 10.seconds)

}
