package com.sksamuel.pulsar4s

import java.util.UUID

import org.scalatest.{FunSuite, Matchers}

import scala.util.{Failure, Success, Try}

class MessageReaderTest extends FunSuite with Matchers {

  import scala.concurrent.ExecutionContext.Implicits.global

  case class Person(name: String, location: String)

  implicit object PersonReader extends MessageReader[Person] {
    override def read(msg: Message): Try[Person] = {
      val str = new String(msg.data)
      str.split('/') match {
        case Array(name, location) => Success(Person(name, location))
        case _ => Failure(new RuntimeException(s"Unable to parse $str"))
      }
    }
  }

  test("message reader should be used to read a message") {

    val client = PulsarClient("pulsar://localhost:6650", "sample/standalone/ns1")
    val topic = Topic("persistent://sample/standalone/ns1/" + UUID.randomUUID())

    val producer = client.producer(topic)
    producer.send("jon snow/the wall")
    producer.close()

    val consumer = client.consumer(topic, Subscription("wibble"))
    consumer.seek(MessageId.earliest)
    consumer.receiveT shouldBe Person("jon snow", "the wall")

    consumer.close()
    client.close()
  }
}
