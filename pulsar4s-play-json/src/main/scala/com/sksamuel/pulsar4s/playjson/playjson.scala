package com.sksamuel.pulsar4s

import java.nio.charset.Charset

import play.api.libs.json.{Json, Reads, Writes}

import scala.annotation.implicitNotFound
import scala.util.Try

package object playjson {

  @implicitNotFound("No Writes for type ${T} found. Bring an implicit Writes[T] instance in scope")
  implicit def playJsonWriter[T](implicit w: Writes[T]): MessageWriter[T] = new MessageWriter[T] {
    override def write(t: T): Try[Message] = {
      Try {
        val bytes = Json.stringify(Json.toJson(t)(w)).getBytes(Charset.forName("UTF-8"))
        Message(None, bytes, Map.empty, None, 0, System.currentTimeMillis())
      }
    }
  }

  @implicitNotFound("No Reads for type ${T} found. Bring an implicit Reads[T] instance in scope")
  implicit def playJsonReader[T](implicit r: Reads[T]): MessageReader[T] = new MessageReader[T] {
    override def read(msg: Message): Try[T] = Try {
      Json.parse(msg.data).as[T]
    }
  }
}
