package com.sksamuel.pulsar4s

import java.nio.charset.StandardCharsets

import io.circe.{Decoder, Encoder, Json, Printer}
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.common.schema.{SchemaInfo, SchemaType}

import scala.annotation.implicitNotFound

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
    "No Encoder for type ${T} found. Use 'import io.circe.generic.auto._' or provide an implicit Encoder instance ")
  implicit def circeSchema[T: Manifest](implicit
                                        encoder: Encoder[T],
                                        decoder: Decoder[T],
                                        printer: Json => String = Printer.noSpaces.pretty): Schema[T] = new Schema[T] {
    override def encode(t: T): Array[Byte] = printer(encoder(t)).getBytes(StandardCharsets.UTF_8)
    override def decode(bytes: Array[Byte]): T = io.circe.jawn.decode[T](new String(bytes, StandardCharsets.UTF_8)).right.get
    override def getSchemaInfo: SchemaInfo =
      new SchemaInfo()
        .setName(manifest[T].runtimeClass.getCanonicalName)
        .setType(SchemaType.JSON)
        .setSchema("""{"type":"any"}""".getBytes("UTF-8"))
  }
}
