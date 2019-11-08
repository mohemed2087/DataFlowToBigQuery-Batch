package com.mohek.beam

import java.io.ByteArrayOutputStream

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import com.mohek.beam.recordtypes.{ExtractParcelsDoFn, ExtractLeftRecsDoFn, GetRightRecs, ParentRow}
import com.mohek.beam.utils.Constants
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.{Default, PipelineOptions, PipelineOptionsFactory}
import org.apache.beam.sdk.extensions.joinlibrary.Join
import org.apache.beam.sdk.transforms.{Create, DoFn, GroupByKey, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

object DataTransformationApp {
  def main(args: Array[String]): Unit = {

    val options = PipelineOptionsFactory.fromArgs(args: _*).withValidation().as(classOf[DataTransformationOptions])
    val pipeline = Pipeline.create(options)


    val data = pipeline.apply("ReadFiles", TextIO.read().from(options.getInputFile))

    val rRec = data.apply("Extract RightRec", ParDo.of(new ExtractParcelsDoFn(Constants.RIGHT_RECORD_TYPE, Constants.RIGHTREC_KEY_INDEX, Constants.RECORD_TYPE_INDEX)))
    val lRec = data.apply("Extract LeftRec", ParDo.of(new ExtractLeftRecsDoFn(Constants.LEFT_RECORD_TYPE, Constants.LEFTREC_KEY_INDEX, Constants.RECORD_TYPE_INDEX))).apply(GroupByKey.create())

    val joined = Join.innerJoin(rRec, lRec)

    val rrecs = joined.apply("", ParDo.of(new GetRightRecs()))

    rrecs.apply("FormatResults", ParDo.of(new FormatParcelResults)).apply("Write Parcels", TextIO.write().withoutSharding().to("/home/user/Desktop/PreAdvice/parcel/output"))

    pipeline.run().waitUntilFinish()
  }
}

class FormatParcelResults extends DoFn[ParentRow, String] {

  @ProcessElement
  def processElement(c: ProcessContext): Unit = {
    val records = c.element()
    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)
    val out = new ByteArrayOutputStream()
    objectMapper.writeValue(out, records)
    val json = out.toString
    c.output(json)
  }
}

