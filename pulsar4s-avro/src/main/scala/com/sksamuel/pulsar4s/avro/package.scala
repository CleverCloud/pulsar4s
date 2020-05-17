package com.sksamuel.pulsar4s

import java.io.ByteArrayOutputStream
import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import com.sksamuel.avro4s.AvroSchema
import com.sksamuel.avro4s.AvroInputStream
import com.sksamuel.avro4s.AvroOutputStream
import com.sksamuel.avro4s.Decoder
import com.sksamuel.avro4s.Encoder
import com.sksamuel.avro4s.SchemaFor
import org.apache.avro
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.common.schema.{SchemaInfo, SchemaType}

import scala.annotation.implicitNotFound

/**
  * Automatic Schema derivation using avro4s
  *
  * == Examples ==
  *
  * {{{
  *  import com.sksamuel.pulsar4s.avro._
  *
  *  case class City(id: Int, name: String)
  *
  *  producer.send(City(1, "London"))
  *
  *  val city: City = consumer.receive
  *
  * }}}
  */
package object avro {

  @implicitNotFound("No Avro Schema for type ${T} found.")
  implicit def avroSchema[T: Manifest: SchemaFor: Encoder: Decoder]: Schema[T] = new Schema[T] {

    val schema: avro.Schema = AvroSchema[T]

    override def clone(): Schema[T] = this

    override def encode(t: T): Array[Byte] = {
      val baos = new ByteArrayOutputStream
      val aos = AvroOutputStream.binary[T].to(baos).build(schema)
      aos.write(t)
      aos.flush()
      aos.close()
      baos.toByteArray()
    }

    override def decode(bytes: Array[Byte]): T = {
      val bais = new ByteArrayInputStream(bytes)
      val ais = AvroInputStream.binary[T].from(bais).build(schema)
      val first = ais.iterator.next()
      ais.close()
      first
    }

    override def getSchemaInfo: SchemaInfo =
      new SchemaInfo()
        .setName(manifest[T].runtimeClass.getCanonicalName)
        .setType(SchemaType.AVRO)
        .setSchema(schema.toString.getBytes(StandardCharsets.UTF_8))
  }
}
