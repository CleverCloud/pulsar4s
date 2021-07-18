package com.sksamuel.pulsar4s

import java.nio.charset.Charset

import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl
import org.apache.pulsar.common.schema.{SchemaInfo, SchemaType}

import scala.annotation.implicitNotFound

package object sprayjson {

  import spray.json._

  @implicitNotFound("No RootJsonWriter for type ${T} found. Bring an implicit RootJsonWriter[T] instance in scope")
  implicit def spraySchema[T: Manifest](implicit w: RootJsonWriter[T], r: RootJsonReader[T]): Schema[T] = new Schema[T] {
    override def clone(): Schema[T] = this
    override def encode(t: T): Array[Byte] = w.write(t).compactPrint.getBytes(Charset.forName("UTF-8"))
    override def decode(bytes: Array[Byte]): T = r.read(new String(bytes, "UTF-8").parseJson)
    override def getSchemaInfo: SchemaInfo = {
      SchemaInfoImpl.builder()
        .name(manifest[T].runtimeClass.getCanonicalName)
        .`type`(SchemaType.BYTES)
        .schema(Array.empty[Byte])
        .build()
    }
  }
}
