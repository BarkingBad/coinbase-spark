package pl.edu.agh.model

import io.circe.generic.JsonCodec

@JsonCodec
case class Coordinates(latitude: Double, longitude: Double)
