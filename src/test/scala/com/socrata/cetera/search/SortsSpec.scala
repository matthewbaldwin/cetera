package com.socrata.cetera.search

import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, WordSpec}

import com.socrata.cetera.types.{AdvancedQuery, NoQuery, SimpleQuery}

// NOTE: The toString method of the SortBuilders does not produce
// JSON-parseable output. So, we test the output of the toString method as a
// proxy for equality.

class SortsSpec extends WordSpec with ShouldMatchers with BeforeAndAfterAll {
  "sortScoreDesc" should {
    "sort by score descending" in {
      val expectedAsString = s"""
        |"_score"{ }""".stripMargin

      val actual = Sorts.sortScoreDesc

      actual.toString should be (expectedAsString)
    }
  }

  "sortFieldAsc" should {
    "sort by a given field ascending" in {
      val field = "field.in.our.documents"
      val expectedAsString = s"""
        |"${field}"{
        |  "order" : "asc"
        |}""".stripMargin

      val actual = Sorts.sortFieldAsc(field)

      actual.toString should be(expectedAsString)
    }
  }

  "sortFieldDesc" should {
    "sort by a given field descending" in {
      val field = "another_field.another_field_total"
      val expectedAsString = s"""
        |"${field}"{
        |  "order" : "desc"
        |}""".stripMargin

      val actual = Sorts.sortFieldDesc(field)

      actual.toString should be(expectedAsString)
    }
  }

  "buildAverageScoreSort" should {
    "build a sort by average field score descending" in {
      val fieldName = "this_looks_like.things"
      val rawFieldName = "this_looks_like.things.raw"
      val classifications = Set("one kind of thing", "another kind of thing")

      // Using toString as proxy since toString does not give JSON parsable string
      val expectedAsString = s"""
         |"${fieldName}"{
         |  "order" : "desc",
         |  "mode" : "avg",
         |  "nested_filter" : {
         |    "terms" : {
         |      "${rawFieldName}" : [ "${classifications.head}", "${classifications.last}" ]
         |    }
         |  }
         |}""".stripMargin

      val actual = Sorts.buildAverageScoreSort(fieldName, rawFieldName, classifications)

      actual.toString should be (expectedAsString)
    }
  }

  "chooseSort" should {
    val cats = Set[String]("comestibles", "potables")
    val tags = Set[String]("tasty", "sweet", "taters", "precious")

    "order by query score descending when given an advanced query" in {
      val expected = Sorts.sortScoreDesc

      val actual = Sorts.chooseSort(
        searchQuery = AdvancedQuery("sandwich AND (soup OR salad)"),
        searchContext = None,
        categories = None,
        tags = Some(tags)
      )

      // sortScores is a val so it's the same object
      actual should be(expected)
    }

    "order by query score desc when given a simple query" in {
      val expected = Sorts.sortScoreDesc

      val searchContext = Domain(
        isCustomerDomain = false,
        organization = Some("SDP"),
        domainCname = "peterschneider.net",
        siteTitle = Some("Temporary URI"),
        moderationEnabled = false,
        routingApprovalEnabled = true
      )

      val actual = Sorts.chooseSort(
        searchQuery = SimpleQuery("soup salad sandwich"),
        searchContext = Some(searchContext),
        categories = Some(cats),
        tags = None
      )

      // sortScores is a val so it's the same object
      actual should be(expected)
    }

    "order by average category score descending when no query but ODN categories present" in {
      val expectedAsString = s"""
        |"animl_annotations.categories.score"{
        |  "order" : "desc",
        |  "mode" : "avg",
        |  "nested_filter" : {
        |    "terms" : {
        |      "animl_annotations.categories.name.raw" : [ "${cats.head}", "${cats.last}" ]
        |    }
        |  }
        |}""".stripMargin

      val actual = Sorts.chooseSort(
        searchQuery = NoQuery,
        searchContext = None,
        categories = Some(cats),
        tags = Some(tags)
      )

      actual.toString should be(expectedAsString)
    }

    "order by average tag score desc when no query or categories but ODN tags present" in {
      val tagsJson = "[ \"" + tags.mkString("\", \"") + "\" ]"

      val expectedAsString = s"""
        |"animl_annotations.tags.score"{
        |  "order" : "desc",
        |  "mode" : "avg",
        |  "nested_filter" : {
        |    "terms" : {
        |      "animl_annotations.tags.name.raw" : ${tagsJson}
        |    }
        |  }
        |}""".stripMargin

      val actual = Sorts.chooseSort(
        searchQuery = NoQuery,
        searchContext = None,
        categories = None,
        tags = Some(tags)
      )

      actual.toString should be(expectedAsString)
    }

    "order by page views descending for default null query" in {
      val expected = Sorts.sortFieldDesc("page_views.page_views_total")

      val actual = Sorts.chooseSort(
        searchQuery = NoQuery,
        searchContext = None,
        categories = None,
        tags = None
      )

      actual.toString should be (expected.toString)
    }
  }
}