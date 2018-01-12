package com.sksamuel.pulsar4s

import java.util.UUID

import com.sksamuel.exts.Logging
import org.apache.pulsar.client.api
import org.apache.pulsar.client.api.{ConsumerConfiguration, ProducerConfiguration, ReaderConfiguration}

case class Topic(name: String)
case class Subscription(name: String)
object Subscription {
  def generate = Subscription(UUID.randomUUID.toString)
}

trait PulsarClient {
  def close(): Unit
  def producer(topic: Topic): Producer
  def producer(topic: Topic, conf: ProducerConfiguration): Producer
  def consumer(topic: Topic, subscription: Subscription): Consumer
  def consumer(topic: Topic, subscription: Subscription, conf: ConsumerConfiguration): Consumer
  def reader(topic: Topic, subscription: Subscription, seek: MessageId, conf: ReaderConfiguration): Reader
}

object PulsarClient {

  def apply(url: String, namespace: String): PulsarClient = new PulsarClient with Logging {

    val client: api.PulsarClient = org.apache.pulsar.client.api.PulsarClient.create(url)

    override def close(): Unit = client.close()

    override def producer(topic: Topic): Producer = {
      new Producer(client.createProducer(topic.name), topic)
    }

    override def reader(topic: Topic, subscription: Subscription, seek: MessageId, conf: ReaderConfiguration): Reader = {
      new Reader(client.createReader(subscription.name, seek, conf), topic, subscription)
    }

    override def producer(topic: Topic, conf: ProducerConfiguration): Producer = {
      new Producer(client.createProducer(topic.name, conf), topic)
    }

    override def consumer(topic: Topic, subscription: Subscription): Consumer = {
      logger.info(s"Creating consumer on $topic with susbcription $subscription")
      new Consumer(client.subscribe(topic.name, subscription.name), topic, subscription)
    }

    override def consumer(topic: Topic, subscription: Subscription, conf: ConsumerConfiguration): Consumer = {
      logger.info(s"Creating consumer on $topic with susbcription $subscription")
      new Consumer(client.subscribe(topic.name, subscription.name, conf), topic, subscription)
    }
  }
}