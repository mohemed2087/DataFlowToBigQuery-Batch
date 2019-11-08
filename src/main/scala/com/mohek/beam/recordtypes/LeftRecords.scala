package com.mohek.beam.recordtypes

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import com.mohek.beam.utils.{Constants, RecordUtils}
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.values.KV

import scala.util.Try


case class LeftRecords(@JsonProperty Indicator: Integer,
                       @JsonProperty VersionNumber: Integer,
                       @JsonProperty UID: String,
                       @JsonProperty Code: String,
                       @JsonProperty Data: String
                      )

class ExtractLeftRecsDoFn(recordType: Integer, keyIndex: Integer, recordTypeIndex: Integer) extends DoFn[String, KV[String, LeftRecords]] {
  val columNames = Constants.LEFTREC_COLUMNS.split(",").map(item => item).toList

  @ProcessElement
  def processElement(c: ProcessContext): Unit = {
    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)
    if ((!c.element().contains(Constants.HEADER_REGEX)) && (c.element().split(",")(recordTypeIndex).toInt == recordType)) {
      val lrecList = c.element().split(",").map(item => item).toList
      val newList = (columNames zip lrecList).map { case (m, a) => Map(m -> a) }.flatten.toMap
      val json = objectMapper.writeValueAsString(newList)
      val lrecs: LeftRecords = objectMapper.readValue[LeftRecords](json)
      val row = KV.of(c.element().split(",")(keyIndex), lrecs)
      c.output(row)
    }
  }
}




