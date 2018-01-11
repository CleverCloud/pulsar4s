package com.sksamuel.pulsar4s.jackson

import com.sksamuel.pulsar4s.Message
import org.scalatest.{Matchers, WordSpec}

case class Place(id: Int, name: String)
case class Cafe(name: String, place: Place)

class JacksonTest extends WordSpec with Matchers {

  import Jackson._
  import com.sksamuel.pulsar4s.{MessageReader, MessageWriter}

  "A derived MessageWriter instance" should {
    "create a message from a class" in {
      val msg = implicitly[MessageWriter[Cafe]].write(Cafe("le table", Place(1, "Paris"))).get
      msg.key shouldBe None
      msg.messageId shouldBe None
      new String(msg.data) shouldBe """{"name":"le table","place":{"id":1,"name":"Paris"}}"""
    }
  }

  "A derived MessageReader instance" should {
    "read a class from a message" in {
      implicitly[MessageReader[Cafe]].read(Message("""{"name":"le table","place":{"id":1,"name":"Paris"}}""")).get shouldBe
        Cafe("le table", Place(1, "Paris"))
    }
  }
}
