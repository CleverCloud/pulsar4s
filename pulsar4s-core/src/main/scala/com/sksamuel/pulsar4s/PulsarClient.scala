package com.sksamuel.pulsar4s

import java.util.UUID

import com.sksamuel.exts.Logging
import org.apache.pulsar.client.api
import org.apache.pulsar.client.api.{ReaderConfiguration, Schema}

case class Topic(name: String)
case class Subscription(name: String)
object Subscription {
  def generate = Subscription(UUID.randomUUID.toString)
}

trait PulsarClient {
  def close(): Unit

  def producer[T](topic: String, config: ProducerConfig)(implicit schema: Schema[T]): Producer[T]
  def consumer[T](topic: String, config: ConsumerConfig)(implicit schema: Schema[T]): Consumer[T]

  def reader(topic: Topic, subscription: Subscription, seek: MessageId, conf: ReaderConfiguration): Reader
}

object PulsarClient {

  def apply(url: String, namespace: String): PulsarClient = new PulsarClient with Logging {

    val client: api.PulsarClient = org.apache.pulsar.client.api.PulsarClient.create(url)

    override def close(): Unit = client.close()

    override def producer[T](topic: String, config: ProducerConfig)(implicit schema: Schema[T]): Producer[T] = {
      logger.info(s"Creating producer on $topic with config $config")
      val builder = client.newProducer(schema)
      builder.topic(topic)
      config.encryptionKey.foreach(builder.addEncryptionKey)
      config.blockIfQueueFull.foreach(builder.blockIfQueueFull)
      config.compressionType.foreach(builder.compressionType)
      new Producer(builder.create())
    }

    override def consumer[T](topic: String, config: ConsumerConfig)(implicit schema: Schema[T]): Consumer[T] = {
      logger.info(s"Creating consumer on $topic with config $config")
      val builder = client.newConsumer(schema)
      builder.topic(topic)
      config.consumerName.foreach(builder.consumerName)
      config.readCompacted.foreach(builder.readCompacted)
      new Consumer(builder.subscribe())
    }

    override def reader(topic: Topic, subscription: Subscription, seek: MessageId, conf: ReaderConfiguration): Reader = {
      new Reader(client.createReader(subscription.name, seek, conf), topic, subscription)
    }
  }
}