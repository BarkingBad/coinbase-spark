package pl.edu.agh.model

import io.circe.generic.JsonCodec

@JsonCodec
case class OpenAQMessage(
    date: Option[DateStruct],
    parameter: Option[String],
    value: Option[Double],
    unit: Option[String],
    averagingPeriod: Option[PeriodStruct],
    location: Option[String],
    city: Option[String],
    country: Option[String],
    coordinates: Option[Coordinates]
)
