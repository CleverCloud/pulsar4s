package com.sksamuel.pulsar4s.avro

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

case class Place(id: Int, name: String)
case class Cafe(name: String, place: Place)

class CodecDerivationTest extends AnyWordSpec with Matchers {

  "A derived Schema instance" should {

    "be implicitly found" in {
      """
        implicitly[org.apache.pulsar.client.api.Schema[Cafe]]
      """ should compile
    }
  }

}
