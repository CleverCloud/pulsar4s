package com.sksamuel.pulsar4s

import java.nio.charset.StandardCharsets

import io.circe.jawn.decode
import io.circe.{Decoder, Encoder, Json, Printer}

import scala.annotation.implicitNotFound
import scala.util.{Failure, Success, Try}

/**
  * Automatic MessageWriter and MessageReader derivation
  *
  * == Examples ==
  *
  * {{{
  *  import io.circe.generic.auto._
  *  import com.sksamuel.pulsar4s.circe._
  *
  *  case class City(id: Int, name: String)
  *
  *  producer.send(City(1, "London"))
  *
  *  val city: City = consumer.receive
  *
  * }}}
  */
package object circe {
  @implicitNotFound(
    "No Decoder for type ${T} found. Use 'import io.circe.generic.auto._' or provide an implicit Decoder instance ")
  implicit def circeReader[T](implicit decoder: Decoder[T]): MessageReader[T] = new MessageReader[T] {
    override def read(msg: Message): Try[T] = decode[T](new String(msg.data, StandardCharsets.UTF_8)) match {
      case Left(e) => Failure(e)
      case Right(t) => Success(t)
    }
  }

  @implicitNotFound(
    "No Encoder for type ${T} found. Use 'import io.circe.generic.auto._' or provide an implicit Encoder instance ")
  implicit def circeWriter[T](implicit encoder: Encoder[T], printer: Json => String = Printer.noSpaces.pretty): MessageWriter[T] = new MessageWriter[T] {
    override def write(t: T): Try[Message] = Try {
      Message(printer(encoder(t)).getBytes(StandardCharsets.UTF_8))
    }
  }
}
