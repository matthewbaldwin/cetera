package com.socrata.cetera.search

import com.rojoma.json.v3.io.JsonReader
import org.scalatest.{ShouldMatchers, WordSpec}
import scala.language.existentials

import com.socrata.cetera.handlers.SearchParamSet
import com.socrata.cetera.types.{AdvancedQuery, Domain, SimpleQuery}

// NOTE: The toString method of the SortBuilders does not produce
// JSON-parseable output. So, we test the output of the toString method as a
// proxy for equality.

class SortsSpec extends WordSpec with ShouldMatchers {
  "sortScoreDesc" should {
    "sort by score descending" in {
      val expectedJson = JsonReader.fromString(s"""{"_score": {"order": "desc"}}""")
      val actualJson = JsonReader.fromString(Sorts.sortScoreDesc.toString)

      actualJson should be (expectedJson)
    }
  }

  "sortFieldAsc" should {
    "sort by a given field ascending" in {
      val field = "field.in.our.documents"
      val expectedJson = JsonReader.fromString(s"""{"${field}": {"order": "asc"}}""")
      val actualJson = JsonReader.fromString(Sorts.sortFieldAsc(field).toString)

      actualJson should be(expectedJson)
    }
  }

  "sortFieldDesc" should {
    "sort by a given field descending" in {
      val field = "another_field.another_field_total"
      val expectedJson = JsonReader.fromString(s"""{"${field}": {"order" : "desc"}}""")
      val actualJson = JsonReader.fromString(Sorts.sortFieldDesc(field).toString)

      actualJson should be(expectedJson)
    }
  }

  "buildAverageScoreSort" should {
    "build a sort by average field score descending" in {
      val fieldName = "this_looks_like.things"
      val rawFieldName = "this_looks_like.things.raw"
      val classifications = Set("one kind of thing", "another kind of thing")

      val expectedJson = JsonReader.fromString(s"""
      {
          "${fieldName}": {
              "order": "desc",
              "mode": "avg",
              "nested_filter": {
                  "terms": {
                      "${rawFieldName}": [
                          "${classifications.head}",
                          "${classifications.last}"
                      ],
                      "boost": 1.0
                  }
              }
          }
      }
      """)

      val actualJson = JsonReader.fromString(
        Sorts.buildAverageScoreSort(fieldName, rawFieldName, classifications).toString
      )

      actualJson should be (expectedJson)
    }
  }

  "chooseSort" should {
    val cats = Set("comestibles", "potables")
    val tags = Set("tasty", "sweet", "taters", "precious")

    "order by query score descending when given an advanced query" in {
      val expected = Sorts.sortScoreDesc

      val searchParams = SearchParamSet(searchQuery = AdvancedQuery("sandwich AND (soup OR salad)"))
      val actual = Sorts.chooseSort(None, searchParams)

      // sortScores is a val so it's the same object
      actual should be(expected)
    }

    "order by score desc when given a simple query" in {
      val expected = Sorts.sortScoreDesc

      val searchContext = Domain(
        domainId = 1,
        domainCname = "peterschneider.net",
        aliases = None,
        siteTitle = Some("Temporary URI"),
        organization = Some("SDP"),
        isCustomerDomain = false,
        moderationEnabled = false,
        routingApprovalEnabled = true,
        lockedDown = false,
        apiLockedDown = false
      )

      val searchParams = SearchParamSet(searchQuery = SimpleQuery("soup salad sandwich"))
      val actual = Sorts.chooseSort(Some(searchContext), searchParams)

      // sortScores is a val so it's the same object
      actual should be(expected)
    }

    "order by average category score descending when no query but ODN categories present" in {
      val expectedJson = JsonReader.fromString(s"""
      {
          "animl_annotations.categories.score": {
              "order" : "desc",
              "mode" : "avg",
              "nested_filter" : {
                  "terms" : {
                      "animl_annotations.categories.name.raw" : [ "${cats.head}", "${cats.last}" ],
                      "boost": 1.0
                  }
              }
          }
      }
      """)

      val searchParams = SearchParamSet(tags = Some(tags), categories = Some(cats))
      val actualJson = JsonReader.fromString(Sorts.chooseSort(None, searchParams).toString)

      actualJson should be(expectedJson)
    }

    "order by average tag score desc when no query or categories but ODN tags present" in {
      val tagsStr = "[ \"" + tags.mkString("\", \"") + "\" ]"

      val expectedJson = JsonReader.fromString(s"""
      {
          "animl_annotations.tags.score": {
              "order" : "desc",
              "mode" : "avg",
              "nested_filter" : {
                  "terms" : {
                      "animl_annotations.tags.name.raw" : ${tagsStr},
                      "boost": 1.0
                  }
              }
          }
      }
      """)


      val searchParams = SearchParamSet(tags = Some(tags))
      val actualJson = JsonReader.fromString(Sorts.chooseSort(None, searchParams).toString)

      actualJson should be(expectedJson)
    }

    "order by score descending for default null query" in {
      val expected = Sorts.sortScoreDesc
      val searchParams = SearchParamSet()
      val actual = Sorts.chooseSort(None, searchParams)

      actual should be (expected)
    }
  }
}
