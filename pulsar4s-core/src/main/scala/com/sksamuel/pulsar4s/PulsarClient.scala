package com.sksamuel.pulsar4s

import com.sksamuel.exts.Logging
import org.apache.pulsar.client.api
import org.apache.pulsar.client.api.{ConsumerConfiguration, ProducerConfiguration}

import scala.concurrent.ExecutionContext

case class Topic(name: String)
case class Subscription(name: String)

trait PulsarClient {
  def close(): Unit
  def producer(topic: Topic): Producer
  def producer(topic: Topic, conf: ProducerConfiguration): Producer
  def consumer(topic: Topic, subscription: Subscription): Consumer
  def consumer(topic: Topic, subscription: Subscription, conf: ConsumerConfiguration): Consumer
}

object PulsarClient {

  def apply(url: String, namespace: String)
           (implicit ec: ExecutionContext): PulsarClient = new PulsarClient with Logging {

    val client: api.PulsarClient = org.apache.pulsar.client.api.PulsarClient.create(url)

    override def close(): Unit = client.close()

    override def producer(topic: Topic): Producer = {
      new Producer(client.createProducer(topic.name), topic)
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

object Test extends App {

  import ExecutionContext.Implicits.global

  val client = PulsarClient("pulsar://localhost:6650", "sample/standalone/ns1")
  val topic = Topic("persistent://sample/standalone/ns1/my-topic")
  //client.send(topic)
  //client.subscribe(topic, "mysub").consume(new Listener {})
}