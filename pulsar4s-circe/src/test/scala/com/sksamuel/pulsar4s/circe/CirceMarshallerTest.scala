package com.sksamuel.pulsar4s.circe

import org.apache.pulsar.client.api.Schema
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CirceMarshallerTest extends AnyWordSpec with Matchers {

  import io.circe.generic.auto._

  "A circe schema instance" should {
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
