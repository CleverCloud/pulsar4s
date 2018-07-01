package com.sksamuel.pulsar4s.circe

import org.scalatest.{Matchers, WordSpec}

case class Place(id: Int, name: String)
case class Cafe(name: String, place: Place)

class CodecDerivationTest extends WordSpec with Matchers {

  "A derived Schema instance" should {

    "be implicitly found if circe.generic.auto is in imported" in {
      """
        import io.circe.generic.auto._
        implicitly[org.apache.pulsar.client.api.Schema[Cafe]]
      """ should compile
    }

    "not compile if no decoder is in scope" in {
      """
        implicitly[org.apache.pulsar.client.api.Schema[Cafe]]
      """ shouldNot compile
    }
  }

}
