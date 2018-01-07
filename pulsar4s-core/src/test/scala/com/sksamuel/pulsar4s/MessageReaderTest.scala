package com.sksamuel.pulsar4s

import java.util.UUID

import org.scalatest.{FunSuite, Matchers}

class MessageReaderTest extends FunSuite with Matchers {

  import scala.concurrent.ExecutionContext.Implicits.global

  case class Person(name: String, location: String)

  implicit object PersonReader extends MessageReader[Person] {
    override def read(msg: Message): Either[Throwable, Person] = {
      val str = new String(msg.data)
      str.split('/') match {
        case Array(name, location) => Right(Person(name, location))
        case _ => Left(new RuntimeException(s"Unable to parse $str"))
      }
    }
  }

  test("message reader should be used to read a message") {
    val client = PulsarClient("pulsar://localhost:6650", "sample/standalone/ns1")
    val topic = Topic("persistent://sample/standalone/ns1/" + UUID.randomUUID())
    val producer = client.producer(topic)
    producer.send("jon snow/the wall")

    val consumer = client.consumer(topic, Subscription("wibble"))
    consumer.seek(MessageId.earliest)
    val t = consumer.receiveT
    t.right.get shouldBe Person("jon snow", "the wall")
  }
}
