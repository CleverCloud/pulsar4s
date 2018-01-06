package com.sksamuel.pulsar4s.circe

import org.scalatest.{Matchers, WordSpec}

case class Place(id: Int, name: String)
case class Cafe(name: String, place: Place)

class CodecDerivationTest extends WordSpec with Matchers {

  "A derived MessageReader instance" should {

    "be implicitly found if circe.generic.auto is in imported" in {
      """
        import io.circe.generic.auto._
        import com.sksamuel.pulsar4s.MessageReader
        implicitly[MessageReader[Cafe]]
      """ should compile
    }

    "not compile if no decoder is in scope" in {
      """
        import com.sksamuel.pulsar4s.MessageReader
        implicitly[MessageReader[Cafe]]
      """ shouldNot compile
    }
  }

  "A derived Indexable instance" should {
    "be implicitly found if circe.generic.auto is in imported" in {
      """
        import io.circe.generic.auto._
        import com.sksamuel.pulsar4s.MessageWriter
        implicitly[MessageWriter[Cafe]]
      """ should compile
    }

    "not compile if no decoder is in scope" in {
      """
        import com.sksamuel.pulsar4s.MessageWriter
        implicitly[MessageWriter[Cafe]]
      """ shouldNot compile
    }
  }

}
