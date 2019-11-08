package com.mohek.beam

import org.apache.beam.sdk.options.{Default, PipelineOptions}

trait DataTransformationOptions extends PipelineOptions {
  @Default.String("/home/user/Desktop/PreAdvice/POL_PreAdvice_321_1_0000003.csv")
  def getInputFile: String

  def setInputFile(path: String)

  @Default.String("/home/user/Desktop/PreAdvice/output/output")
  def getOutputPath: String

  def setOutputPath(path: String)
}
