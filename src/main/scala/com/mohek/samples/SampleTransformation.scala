package com.mohek.samples

import java.util

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.extensions.joinlibrary.Join
import org.apache.beam.sdk.options.{Default, PipelineOptions, PipelineOptionsFactory}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.join.CoGbkResult
import org.apache.beam.sdk.transforms.{Create, DoFn, ParDo}
import org.apache.beam.sdk.values.{KV, TupleTag}

object SampleTransformation {


  def main(args: Array[String]): Unit = {

    val options = PipelineOptionsFactory.fromArgs(args: _*).withValidation().as(classOf[SamplePipelineOptions])
    val pipeline = Pipeline.create(options)

    val pt1 = pipeline.apply(Create.of(KV.of("1", "2,3,III0134iiiii40585540,34")))
   // val pt2 = pipeline.apply(Create.of(KV.of("2", "3,3,1,123,4566,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,")))
    val pt2 = pipeline.apply(Create.of(KV.of("2",new util.ArrayList{ "7,5,6,66,afd,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,"})))

    val nullValue : util.ArrayList[String] =  new util.ArrayList{"fs"}

    Join.leftOuterJoin(pt1,pt2,nullValue).apply(ParDo.of(new sampleDisplay1))
//    import org.apache.beam.sdk.values.TupleTag
//    val t1 = new TupleTag[String]
//    val t2 = new TupleTag[Iterable[String]]
//
//    import org.apache.beam.sdk.transforms.join.CoGbkResult
//    import org.apache.beam.sdk.transforms.join.CoGroupByKey
//    import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple
//    import org.apache.beam.sdk.values.KV
//    import org.apache.beam.sdk.values.PCollection
//
//    val coGbkResultCollection = KeyedPCollectionTuple.of(t1, pt1).and(t2, pt2).apply(CoGroupByKey.create[String])
//    val result = coGbkResultCollection.apply(
//      ParDo.of(new Joining(t1,t2)))
//      .setCoder(KvCoder.of((pt1.getCoder.asInstanceOf[KvCoder[String,String]]).getKeyCoder, KvCoder.of((pt1.getCoder.asInstanceOf[KvCoder[String,String]]).getValueCoder, (pt2.getCoder.asInstanceOf[KvCoder[String,Iterable[String]]]).getValueCoder)))
//     // .setCoder(NullableCoder.of(KvCoder.of(()))
//        //    val testRes = Join.leftOuterJoin(test1, test3,null)
//        //   testRes.apply("", ParDo.of(new sampleDisplay1))

        pipeline.run().waitUntilFinish()

  }

}

trait SamplePipelineOptions extends PipelineOptions {

  @Default.String("/home/user/Desktop/PreAdvice/POL_PreAdvice_321_1_0000003.csv")
  def getInputFile: String

  def setInputFile(path: String)

  @Default.String("/home/user/Desktop/beam/output")
  def getOutputPath: String

  def setOutputPath(path: String)

}

class sampleDisplay1 extends DoFn[KV[String,KV[String, util.ArrayList[String]]], String] {

  @ProcessElement
  def processElement(c: ProcessContext): Unit = {
    println(c.element().getKey+"   "+c.element().getValue.getKey+"   "+c.element().getValue.getValue)
    c.output(c.element().toString)
  }
}

class Joining(t1: TupleTag[String],t2: TupleTag[Iterable[String]]) extends DoFn[KV[String, CoGbkResult], KV[String, KV[String, Iterable[String]]]] {
  @ProcessElement
  def processElement(c: ProcessContext) {
    val e = c.element()
    val pt1Vals = e.getValue.getAll(t1)
    val pt2Vals = e.getValue.getAll(t2)
    import org.apache.beam.sdk.values.KV
//    for (leftValue <- pt1Vals) {
//      import scala.collection.JavaConversions._
//      for (rightValue <- pt2Vals) {
//        println(KV.of(e.getKey, KV.of(leftValue, rightValue)))
//        c.output(KV.of(e.getKey, KV.of(leftValue, rightValue)))
//      }
//    }
    import scala.collection.JavaConversions._
    for (leftValue <- pt1Vals) {
      if (pt2Vals.iterator.hasNext) {
        import scala.collection.JavaConversions._
        for (rightValue <- pt2Vals) {
          println(KV.of(e.getKey, KV.of(leftValue, rightValue)))
          c.output(KV.of(e.getKey, KV.of(leftValue, rightValue)))
        }
      }

      else {
        println(KV.of(e.getKey, KV.of(leftValue, Iterable(""))))
        c.output(KV.of(e.getKey, KV.of(leftValue, Iterable(""))))}
    }
  }
}



