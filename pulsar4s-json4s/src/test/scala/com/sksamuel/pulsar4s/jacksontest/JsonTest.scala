package com.sksamuel.pulsar4s.jacksontest

import org.apache.pulsar.client.api.Schema
import org.json4s.DefaultFormats
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

case class Place(id: Int, name: String)
case class Cafe(name: String, place: Place)

class Json4sTest extends AnyWordSpec with Matchers {

  implicit val formats: DefaultFormats.type = DefaultFormats

  import org.json4s._
  import com.sksamuel.pulsar4s.json4s._
  import org.json4s.jackson.JsonMethods._

//  "A json4s schema instance" should {
//    "create bytes from a class" in {
//      val cafe = Cafe("le table", Place(1, "Paris"))
//      val bytes = implicitly[Schema[Cafe]].encode(cafe)
//      bytes shouldBe """{"name":"le table","place":{"id":1,"name":"Paris"}}""".getBytes("UTF-8")
//    }
//    "read a class from bytes" in {
//      val bytes = """{"name":"le table","place":{"id":1,"name":"Paris"}}""".getBytes("UTF-8")
//      implicitly[Schema[Cafe]].decode(bytes) shouldBe Cafe("le table", Place(1, "Paris"))
//    }
//  }
}
