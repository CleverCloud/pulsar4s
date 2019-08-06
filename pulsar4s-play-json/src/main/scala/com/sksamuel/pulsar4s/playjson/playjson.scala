package com.sksamuel.pulsar4s

import java.nio.charset.Charset

import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.common.schema.{SchemaInfo, SchemaType}
import play.api.libs.json.{Json, Reads, Writes}

import scala.annotation.implicitNotFound

package object playjson {

  @implicitNotFound("No Writes or Reads for type ${T} found. Bring an implicit Writes[T] and Reads[T] instance in scope")
  implicit def playSchema[T: Manifest](implicit w: Writes[T], r: Reads[T]): Schema[T] = new Schema[T] {
    override def encode(t: T): Array[Byte] = Json.stringify(Json.toJson(t)(w)).getBytes(Charset.forName("UTF-8"))
    override def decode(bytes: Array[Byte]): T = Json.parse(bytes).as[T]
    override def getSchemaInfo: SchemaInfo =
      new SchemaInfo()
        .setName(manifest[T].runtimeClass.getCanonicalName)
        .setType(SchemaType.JSON)
        .setSchema("""{"type":"any"}""".getBytes("UTF-8"))
  }
}
