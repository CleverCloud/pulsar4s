package com.sksamuel.pulsar4s

import org.apache.pulsar.client.api

import scala.concurrent.ExecutionContext

case class Topic(name: String)
case class Subscription(name: String)

trait PulsarClient {
  def close(): Unit
  def producer(topic: Topic): Producer
  def consumer(topic: Topic, subscription: Subscription): Consumer
}

object PulsarClient {

  def apply(url: String, namespace: String)
           (implicit ec: ExecutionContext): PulsarClient = new PulsarClient {

    val client: api.PulsarClient = org.apache.pulsar.client.api.PulsarClient.create(url)

    override def close(): Unit = client.close()

    override def producer(topic: Topic): Producer = new DefaultProducer(client.createProducer(topic.name), topic)
    override def consumer(topic: Topic, subscription: Subscription): Consumer = new DefaultConsumer(client.subscribe(topic.name, subscription.name), topic, subscription)
  }
}

object Test extends App {

  import ExecutionContext.Implicits.global

  val client = PulsarClient("pulsar://localhost:6650", "sample/standalone/ns1")
  val topic = Topic("persistent://sample/standalone/ns1/my-topic")
  //client.send(topic)
  //client.subscribe(topic, "mysub").consume(new Listener {})
}