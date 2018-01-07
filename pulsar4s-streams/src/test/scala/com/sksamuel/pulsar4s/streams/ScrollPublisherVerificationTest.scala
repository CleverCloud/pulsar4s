package com.sksamuel.pulsar4s.streams

import com.sksamuel.pulsar4s.{Message, PulsarClient, Topic}
import org.reactivestreams.Publisher
import org.reactivestreams.tck.{PublisherVerification, TestEnvironment}
import org.scalatest.testng.TestNGSuiteLike

class PulsarPublisherVerificationTest
  extends PublisherVerification[Message](
    new TestEnvironment(DEFAULT_TIMEOUT_MILLIS),
    PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS
  ) with TestNGSuiteLike {

  import scala.concurrent.ExecutionContext.Implicits.global

  val client = PulsarClient("pulsar://localhost:6650", "sample/standalone/ns1")
  val topic = Topic("persistent://sample/standalone/ns1/reactivepub")

  //  implicit val system = ActorSystem()

  //  http.execute {
  //    bulk(
  //      indexInto("scrollpubver" / "empires") source Empire("Parthian", "Persia", "Ctesiphon"),
  //      indexInto("scrollpubver" / "empires") source Empire("Ptolemaic", "Egypt", "Alexandria"),
  //      indexInto("scrollpubver" / "empires") source Empire("British", "Worldwide", "London"),
  //      indexInto("scrollpubver" / "empires") source Empire("Achaemenid", "Persia", "Babylon"),
  //      indexInto("scrollpubver" / "empires") source Empire("Sasanian", "Persia", "Ctesiphon"),
  //      indexInto("scrollpubver" / "empires") source Empire("Mongol", "East Asia", "Avarga"),
  //      indexInto("scrollpubver" / "empires") source Empire("Roman", "Mediterranean", "Rome"),
  //      indexInto("scrollpubver" / "empires") source Empire("Sumerian", "Mesopotamia", "Uruk"),
  //      indexInto("scrollpubver" / "empires") source Empire("Klingon", "Space", "Kronos"),
  //      indexInto("scrollpubver" / "empires") source Empire("Romulan", "Space", "Romulus"),
  //      indexInto("scrollpubver" / "empires") source Empire("Cardassian", "Space", "Cardassia Prime"),
  //      indexInto("scrollpubver" / "empires") source Empire("Egyptian", "Egypt", "Memphis"),
  //      indexInto("scrollpubver" / "empires") source Empire("Babylonian", "Levant", "Babylon")
  //    ).immediateRefresh()
  //  }

  override def boundedDepthOfOnNextAndRequestRecursion: Long = 2l

  override def createFailedPublisher(): Publisher[Message] = null

  override def createPublisher(elements: Long): Publisher[Message] = {
    new PulsarPublisher(client, topic)
  }
}

case class Empire(name: String, location: String, capital: String)
