package com.mohek.beam.recordtypes

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.beam.sdk.values.PCollection

case class ParentRow(
                      @JsonProperty rrecs: RightRecords,
                      @JsonProperty lrecs: Iterable[LeftRecords]
                    )
