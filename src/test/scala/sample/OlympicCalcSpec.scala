package sample

import org.scalatest._

class OlympicCalcSpec extends FunSpec with ShouldMatchers with SparkContextHelper with OlympicCalc {
  describe("OlymicCalc"){
    it("should find the year in which USA scored most medals"){
      withLocalSparkContext("OlymicCalc"){ sc =>
        findMostMedalsYear4USA(sc.textFile("src/test/resources/olympic.csv")) shouldBe "2008"
      }
    }
  }
}
