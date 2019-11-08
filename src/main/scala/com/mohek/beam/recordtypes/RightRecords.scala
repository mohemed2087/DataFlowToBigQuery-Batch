package com.mohek.beam.recordtypes


import java.io.ByteArrayOutputStream

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.values.KV
import com.mohek.beam.utils.{Constants, RecordUtils}

case class RightRecords(RecordTypeIndicator: Integer,
                   @JsonProperty VersionNumber: Integer,
                   @JsonProperty CNumber: String,
                   @JsonProperty ID: String,
                   @JsonProperty UID: String,
                   @JsonProperty Weight: Integer
                  )

class ExtractParcelsDoFn(recordType: Integer, keyIndex: Integer, recordTypeIndex: Integer) extends DoFn[String, KV[String, RightRecords]] {
  val columNames = Constants.RIGHTREC_COLUMNS.split(",").map(item => item).toList

  @ProcessElement
  def processElement(c: ProcessContext): Unit = {
    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)
    if ((!c.element().contains(Constants.HEADER_REGEX)) && (c.element().split(",")(recordTypeIndex).toInt == recordType)) {
      val rrecList = c.element().split(",").map(item => item).toList
      val newList = (columNames zip rrecList).map { case (m, a) => Map(m -> a) }.flatten.toMap
      val json = objectMapper.writeValueAsString(newList)
      val parcels: RightRecords = objectMapper.readValue[RightRecords](json)
      val row = KV.of(c.element().split(",")(keyIndex), parcels)
      c.output(row)
    }
  }
}

class GetRightRecs() extends DoFn[KV[String, KV[RightRecords, java.lang.Iterable[LeftRecords]]], ParentRow] {
  import scala.collection.JavaConverters._
  @ProcessElement
  def processElement(c: ProcessContext): Unit = {
    val records = ParentRow(c.element().getValue.getKey, c.element().getValue.getValue.asScala)
    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)
    val out = new ByteArrayOutputStream()
    objectMapper.writeValue(out, records)
    val json = out.toString
    //  println(json)
    c.output(records)

  }

}
