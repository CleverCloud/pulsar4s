package com.sksamuel.pulsar4s

import org.apache.pulsar.client.api
import org.apache.pulsar.client.impl.ProducerStats

import scala.compat.java8.FutureConverters
import scala.concurrent.Future

class DefaultProducer(client: api.PulsarClient, override val topic: Topic) extends Producer {

  private val jproducer = client.createProducer(topic.name)

  override def name: ProducerName = ProducerName(jproducer.getProducerName)

  override def send(msg: Array[Byte]): MessageId = MessageId(jproducer.send(msg))

  override def sendAsync(msg: Array[Byte]): Future[MessageId] = ???

  override def send(msg: SMessage): MessageId = MessageId(jproducer.send(SMessage.toJava(msg)))

  override def sendAsync(msg: SMessage): Future[MessageId] = {
    val f = FutureConverters.toScala(jproducer.sendAsync(SMessage.toJava(msg)))
    f.map(MessageId.apply)
  }

  override def send[T: MessageWriter](t: T): MessageId = ???
  override def sendAsync[T: MessageWriter](t: T): Future[MessageId] = ???
  override def lastSequenceId: Long = ???
  override def stats: ProducerStats = ???
  override def close(): Unit = ???
  override def closeAsync: Future[Unit] = ???
}
