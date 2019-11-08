package com.mohek.samples

import java.lang

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.{Default, PipelineOptions, PipelineOptionsFactory}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms._
import org.apache.beam.sdk.values.{KV, PCollection}

object WordCount {

  def main(args: Array[String]): Unit = {

    val options = PipelineOptionsFactory.fromArgs(args: _*).withValidation().as(classOf[PreAdviceOptions])
    val pipeline = Pipeline.create(options)

    pipeline.apply("ReadFiles", TextIO.read().from(options.getInputFile))
      .apply(new CountWords)
      .apply(MapElements.via(new FormatResult))
      .apply("WriteWords", TextIO.write().to(options.getOutputPath))


    pipeline.run().waitUntilFinish()

  }

}

trait PreAdviceOptions extends PipelineOptions {

  @Default.String("/home/user/Downloads/delivery_point_address.csv")
  def getInputFile: String

  def setInputFile(path: String)

  @Default.String("/home/user/Desktop/beam/output")
  def getOutputPath: String

  def setOutputPath(path: String)

}

class FormatResult extends SimpleFunction[KV[String, java.lang.Long], String] {
  override def apply(input: KV[String, lang.Long]): String = {
    input.getKey + ": " + input.getValue
  }
}

class ExtractWords extends DoFn[String, String] {
  @ProcessElement
  def processElement(c: ProcessContext): Unit = {
    for (word <- c.element().split(" ")) yield {
      if (!word.isEmpty) c.output(word)
    }
  }
}

class CountWords extends PTransform[PCollection[String], PCollection[KV[String, java.lang.Long]]] {
  override def expand(input: PCollection[String]) = {
    input.apply(ParDo.of(new ExtractWords)) //Ignore IntelliJ error: "Cannot resolve apply". The code will compile.
      .apply(Count.perElement())
  }
}
