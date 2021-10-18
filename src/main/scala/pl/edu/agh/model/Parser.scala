package pl.edu.agh.model

import io.circe.parser.parse

object Parser {

  def apply(json: String): Either[io.circe.Error, OpenAQMessage] = {
    parse(json).flatMap(_.hcursor.as[OpenAQMessage])
  }
}
