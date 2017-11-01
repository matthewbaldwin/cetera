package com.socrata.cetera.search

import com.rojoma.json.v3.io.JsonReader
import org.scalatest.{ShouldMatchers, WordSpec}
import scala.language.existentials

import org.elasticsearch.search.sort.{SortBuilder, SortOrder}

import com.socrata.cetera.handlers.SearchParamSet
import com.socrata.cetera.search.Sorts._
import com.socrata.cetera.types._

// NOTE: The toString method of the SortBuilders does not produce
// JSON-parseable output. So, we test the output of the toString method as a
// proxy for equality.

class SortsSpec extends WordSpec with ShouldMatchers {
  def testSortBuilder(
      sortKey: String,
      sortOrder: Option[String],
      expectedBuilder: SortBuilder[_],
      domainId: Option[Int] = None): Unit =
    s"build the correct SortBuilder for sortKey = '$sortKey' and sortOrder = $sortOrder" in {
      Sorts.buildSort(Some(sortKey), sortOrder, domainId) should be(expectedBuilder)
    }

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

  "mapSortParam" should {
    testSortBuilder("createdAt", Some("ASC"), sortFieldAsc(CreatedAtFieldType.fieldName))
    testSortBuilder("createdAt", Some("DESC"), sortFieldDesc(CreatedAtFieldType.fieldName))
    testSortBuilder("createdAt", None, sortFieldDesc(CreatedAtFieldType.fieldName))
    testSortBuilder("created_at", Some("ASC"), sortFieldAsc(CreatedAtFieldType.fieldName))
    testSortBuilder("created_at", Some("DESC"), sortFieldDesc(CreatedAtFieldType.fieldName))
    testSortBuilder("created_at", None, sortFieldDesc(CreatedAtFieldType.fieldName))
    testSortBuilder("updatedAt", Some("ASC"), sortFieldAsc(UpdatedAtFieldType.fieldName))
    testSortBuilder("updatedAt", Some("DESC"), sortFieldDesc(UpdatedAtFieldType.fieldName))
    testSortBuilder("updatedAt", None, sortFieldDesc(UpdatedAtFieldType.fieldName))
    testSortBuilder("updated_at", Some("ASC"), sortFieldAsc(UpdatedAtFieldType.fieldName))
    testSortBuilder("updated_at", Some("DESC"), sortFieldDesc(UpdatedAtFieldType.fieldName))
    testSortBuilder("updated_at", None, sortFieldDesc(UpdatedAtFieldType.fieldName))
    testSortBuilder("submitted_at", Some("ASC"), buildNestedApprovalsSort(SubmittedAtFieldType.fieldName, SortOrder.ASC))
    testSortBuilder("submitted_at", Some("DESC"), buildNestedApprovalsSort(SubmittedAtFieldType.fieldName, SortOrder.DESC))
    testSortBuilder("submitted_at", None, buildNestedApprovalsSort(SubmittedAtFieldType.fieldName, SortOrder.DESC))
    testSortBuilder("reviewed_at", Some("ASC"), buildNestedApprovalsSort(ReviewedAtFieldType.fieldName, SortOrder.ASC))
    testSortBuilder("reviewed_at", Some("DESC"), buildNestedApprovalsSort(ReviewedAtFieldType.fieldName, SortOrder.DESC))
    testSortBuilder("reviewed_at", None, buildNestedApprovalsSort(ReviewedAtFieldType.fieldName, SortOrder.DESC))
    testSortBuilder("page_views_last_week", Some("ASC"), sortFieldAsc(PageViewsLastWeekFieldType.fieldName))
    testSortBuilder("page_views_last_week", Some("DESC"), sortFieldDesc(PageViewsLastWeekFieldType.fieldName))
    testSortBuilder("page_views_last_week", None, sortFieldDesc(PageViewsLastWeekFieldType.fieldName))
    testSortBuilder("page_views_last_month", Some("ASC"), sortFieldAsc(PageViewsLastMonthFieldType.fieldName))
    testSortBuilder("page_views_last_month", Some("DESC"), sortFieldDesc(PageViewsLastMonthFieldType.fieldName))
    testSortBuilder("page_views_last_month", None, sortFieldDesc(PageViewsLastMonthFieldType.fieldName))
    testSortBuilder("page_views_total", Some("ASC"), sortFieldAsc(PageViewsTotalFieldType.fieldName))
    testSortBuilder("page_views_total", Some("DESC"), sortFieldDesc(PageViewsTotalFieldType.fieldName))
    testSortBuilder("page_views_total", None, sortFieldDesc(PageViewsTotalFieldType.fieldName))
    testSortBuilder("name", None, sortFieldAsc(TitleFieldType.lowercaseAlphanumFieldName))
    testSortBuilder("name", Some("ASC"), sortFieldAsc(TitleFieldType.lowercaseAlphanumFieldName))
    testSortBuilder("name", Some("DESC"), sortFieldDesc(TitleFieldType.lowercaseAlphanumFieldName))
    testSortBuilder("owner", None, sortFieldAsc(OwnerDisplayNameFieldType.fieldName))
    testSortBuilder("owner", Some("ASC"), sortFieldAsc(OwnerDisplayNameFieldType.fieldName))
    testSortBuilder("owner", Some("DESC"), sortFieldDesc(OwnerDisplayNameFieldType.fieldName))
    testSortBuilder("submitter", None, buildNestedApprovalsSort(SubmitterNameFieldType.fieldName, SortOrder.ASC))
    testSortBuilder("submitter", Some("ASC"), buildNestedApprovalsSort(SubmitterNameFieldType.fieldName, SortOrder.ASC))
    testSortBuilder("submitter", Some("DESC"), buildNestedApprovalsSort(SubmitterNameFieldType.fieldName, SortOrder.DESC))
    testSortBuilder("reviewer", None, buildNestedApprovalsSort(ReviewerNameFieldType.fieldName, SortOrder.ASC))
    testSortBuilder("reviewer", Some("ASC"), buildNestedApprovalsSort(ReviewerNameFieldType.fieldName, SortOrder.ASC))
    testSortBuilder("reviewer", Some("DESC"), buildNestedApprovalsSort(ReviewerNameFieldType.fieldName, SortOrder.DESC))
    testSortBuilder("domain_category", None, sortFieldAsc(DomainCategoryFieldType.lowercaseAlphanumFieldName))
    testSortBuilder("domain_category", Some("ASC"), sortFieldAsc(DomainCategoryFieldType.lowercaseAlphanumFieldName))
    testSortBuilder("domain_category", Some("DESC"), sortFieldDesc(DomainCategoryFieldType.lowercaseAlphanumFieldName))
    testSortBuilder("datatype", None, sortFieldAsc(DatatypeFieldType.fieldName))
    testSortBuilder("datatype", Some("ASC"), sortFieldAsc(DatatypeFieldType.fieldName))
    testSortBuilder("datatype", Some("DESC"), sortFieldDesc(DatatypeFieldType.fieldName))
    testSortBuilder("relevance", None, sortScoreDesc)
    testSortBuilder("dataset_id", None, sortDatasetId)
    testSortBuilder("screen_name", None, sortFieldAsc(UserScreenName.lowercaseAlphanumFieldName), Some(1))
    testSortBuilder("screen_name", Some("ASC"), sortFieldAsc(UserScreenName.lowercaseAlphanumFieldName), Some(1))
    testSortBuilder("screen_name", Some("DESC"), sortFieldDesc(UserScreenName.lowercaseAlphanumFieldName), Some(1))
    testSortBuilder("email", None, sortFieldAsc(UserEmail.lowercaseAlphanumFieldName), Some(1))
    testSortBuilder("email", Some("ASC"), sortFieldAsc(UserEmail.lowercaseAlphanumFieldName), Some(1))
    testSortBuilder("email", Some("DESC"), sortFieldDesc(UserEmail.lowercaseAlphanumFieldName), Some(1))
    testSortBuilder("role_name", None, buildNestedUserSort(UserRoleName.lowercaseAlphanumFieldName, 1, SortOrder.ASC), Some(1))
    testSortBuilder("role_name", Some("ASC"), buildNestedUserSort(UserRoleName.lowercaseAlphanumFieldName, 1, SortOrder.ASC), Some(1))
    testSortBuilder("role_name", Some("DESC"), buildNestedUserSort(UserRoleName.lowercaseAlphanumFieldName, 1, SortOrder.DESC), Some(1))
    testSortBuilder("last_authenticated_at", Some("ASC"), buildNestedUserSort(UserLastAuthenticatedAt.fieldName, 1, SortOrder.ASC), Some(1))
    testSortBuilder("last_authenticated_at", Some("DESC"), buildNestedUserSort(UserLastAuthenticatedAt.fieldName, 1, SortOrder.DESC), Some(1))
    testSortBuilder("last_authenticated_at", None, buildNestedUserSort(UserLastAuthenticatedAt.fieldName, 1, SortOrder.DESC), Some(1))
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
