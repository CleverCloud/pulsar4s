package com.sksamuel.pulsar4s.akka.streams

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.sksamuel.pulsar4s.ProducerMessage

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object Example {

  import com.sksamuel.pulsar4s.{ConsumerConfig, MessageId, ProducerConfig, PulsarClient, Subscription, Topic}
  import org.apache.pulsar.client.api.Schema

  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val schema: Schema[Array[Byte]] = Schema.BYTES

  val client = PulsarClient("pulsar://localhost:6650")

  val intopic = Topic("persistent://sample/standalone/ns1/in")
  val outtopic = Topic("persistent://sample/standalone/ns1/out")

  val consumerFn = () => client.consumer(ConsumerConfig(topics = Seq(intopic), subscriptionName = Subscription("mysub")))
  val producerFn = () => client.producer(ProducerConfig(outtopic))

  val control = source(consumerFn, Some(MessageId.earliest))
    .map { consumerMessage => ProducerMessage(consumerMessage.data) }
    .to(sink(producerFn)).run()

  Await.result(control.shutdown(), 10.seconds)

}
