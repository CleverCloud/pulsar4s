package com.sksamuel.pulsar4s

import org.apache.pulsar.client.api.{Producer => JProducer}
import org.apache.pulsar.client.impl.ProducerStats

import scala.compat.java8.FutureConverters
import scala.concurrent.Future

class DefaultProducer(producer: JProducer, override val topic: Topic) extends Producer {

  override def name: ProducerName = ProducerName(producer.getProducerName)

  override def send(msg: Array[Byte]): MessageId = MessageId(producer.send(msg))

  override def sendAsync(msg: Array[Byte]): Future[MessageId] = ???

  override def send(msg: SMessage): MessageId = MessageId(producer.send(SMessage.toJava(msg)))

  override def sendAsync(msg: SMessage): Future[MessageId] = {
    val f = FutureConverters.toScala(producer.sendAsync(SMessage.toJava(msg)))
    f.map(MessageId.apply)
  }

  override def send[T: MessageWriter](t: T): MessageId = ???
  override def sendAsync[T: MessageWriter](t: T): Future[MessageId] = ???

  override def lastSequenceId: Long = producer.getLastSequenceId

  override def stats: ProducerStats = producer.getStats

  override def close(): Unit = producer.close()
  override def closeAsync: Future[Unit] = FutureConverters.toScala(producer.closeAsync()).map(_ => ())
}
