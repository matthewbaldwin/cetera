package com.socrata.cetera.types

import org.scalatest.{ShouldMatchers, WordSpec}

class QueryTypeSpec extends WordSpec with ShouldMatchers {
  "minShouldMatch" should {
    "return None from a NoQuery" in {
      val query = NoQuery
      val msm = MinShouldMatch.fromParam(query, "3")
      msm should be(None)
    }

    "return None from an AdvancedQuery" in {
      val query = AdvancedQuery("anything goes here")
      val msm = MinShouldMatch.fromParam(query, "75%")
      msm should be(None)
    }

    "return Some from a SimpleQuery" in {
      val query = SimpleQuery("data about dancing shoes")
      val msm = MinShouldMatch.fromParam(query, "3<90%")
      msm should be(Some("3<90%"))
    }

    "trim whitespace from param" in {
      val query = SimpleQuery("pasta and other cards")
      val msm = MinShouldMatch.fromParam(query, " 2<-25%  9<-3   ")
      msm should be(Some("2<-25%  9<-3"))
    }
  }
}
