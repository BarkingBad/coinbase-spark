package pl.edu.agh.model

import org.scalatest.flatspec.AnyFlatSpec

class ModelSpec extends AnyFlatSpec {
  private val message = """{
      |  "Type" : "Notification",
      |  "MessageId" : "47bd1384-fb58-5def-a7b2-4ba2f0be2f32",
      |  "TopicArn" : "arn:aws:sns:us-east-1:470049585876:OPENAQ_NEW_MEASUREMENT",
      |  "Message" : "{\"date\":{\"utc\":\"2020-11-11T13:00:00.000Z\",\"local\":\"2020-11-12T00:00:00+11:00\"},\"parameter\":\"o3\",\"value\":0.005,\"unit\":\"ppm\",\"averagingPeriod\":{\"value\":1,\"unit\":\"hours\"},\"location\":\"Florey\",\"city\":\"Canberra\",\"country\":\"AU\",\"coordinates\":{\"latitude\":-35.220606,\"longitude\":149.043539},\"attribution\":[{\"name\":\"Health Protection Service, ACT Government\",\"url\":\"https://www.data.act.gov.au/Environment/Air-Quality-Monitoring-Data/94a5-zqnn\"}],\"sourceName\":\"Australia - ACT\",\"sourceType\":\"government\",\"mobile\":false}",
      |  "Timestamp" : "2020-11-11T15:56:22.070Z",
      |  "SignatureVersion" : "1",
      |  "Signature" : "ECdYM5B775xz01MeP0Wy//GzVvtG5ppJ0BgCf5GPZHWY4zXh5qFT60PpsTrwXbSvOLtx/KQyQL/uAt4utgbOcX8dr2Jus1q+fAYkKcx6EaDgfAPsbkaxwPsZGMlCyQ2EHrC+2nbutg3/ftdGXbeyb5QswX+k/D2H35KwS9V9KBJSnKepBzFCap1u+PnKN7AwQ2yTlr54aV3FqLOuwKySbdhon1/C5KsF4p+k+IScqdHntgVCpLjMgXNjy91klRVhYYDmlaj0TMpUyg2h9CVZQgvs8hGrqQ+t5PR8rDGRZW9YBx3qVrHCodlY+von7Q927dhm9YFVGf1zPyLQNijiyA==",
      |  "SigningCertURL" : "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-a86cb10b4e1f29c941702d737128f7b6.pem",
      |  "UnsubscribeURL" : "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:470049585876:OPENAQ_NEW_MEASUREMENT:2c51f8fb-d235-4a24-b161-2f8837251e64",
      |  "MessageAttributes" : {
      |    "country" : {"Type":"String","Value":"AU"},
      |    "unit" : {"Type":"String","Value":"ppm"},
      |    "date_utc" : {"Type":"String","Value":"2020-11-11T13:00:00.000Z"},
      |    "date_local" : {"Type":"String","Value":"2020-11-12T00:00:00+11:00"},
      |    "city" : {"Type":"String","Value":"Canberra"},
      |    "sourceType" : {"Type":"String","Value":"government"},
      |    "parameter" : {"Type":"String","Value":"o3"},
      |    "latitude" : {"Type":"Number","Value":"-35.220606"},
      |    "location" : {"Type":"String","Value":"Florey"},
      |    "sourceName" : {"Type":"String","Value":"Australia - ACT"},
      |    "value" : {"Type":"Number","Value":"0.005"},
      |    "longitude" : {"Type":"Number","Value":"149.043539"}
      |  }
      |}
      |""".stripMargin
  it should "parse sample json" in {
    val res = Parser(message)
    System.err.println(res)
    assert(res.isRight)
  }
}
