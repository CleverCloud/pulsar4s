package com.sksamuel.pulsar4s.jacksontest

import org.apache.pulsar.client.api.Schema
import org.scalatest.{Matchers, WordSpec}

case class Place(id: Int, name: String)
case class Cafe(name: String, place: Place)

class Json4sTest extends WordSpec with Matchers {

  import org.json4s._
  implicit val formats: DefaultFormats.type = DefaultFormats

  "A json4s schema instance" should {
    "create bytes from a class" in {
      val cafe = Cafe("le table", Place(1, "Paris"))
      val bytes = implicitly[Schema[Cafe]].encode(cafe)
      bytes shouldBe """{"name":"le table","place":{"id":1,"name":"Paris"}}""".getBytes("UTF-8")
    }
    "read a class from bytes" in {
      val bytes = """{"name":"le table","place":{"id":1,"name":"Paris"}}""".getBytes("UTF-8")
      implicitly[Schema[Cafe]].decode(bytes) shouldBe Cafe("le table", Place(1, "Paris"))
    }
  }
}
