package com.socrata.cetera.search

import scala.language.existentials

import org.elasticsearch.search.sort.{SortBuilder, SortOrder}
import org.scalatest.{ShouldMatchers, WordSpec}

import com.socrata.cetera.handlers.SearchParamSet
import com.socrata.cetera.search.Sorts._
import com.socrata.cetera.types._

class SortsSpec extends WordSpec with ShouldMatchers {
  def testSortBuilder(
      sortKey: String,
      sortOrder: Option[String],
      expectedBuilder: SortBuilder[_],
      domainId: Option[Int] = None): Unit =
    s"build the correct SortBuilder for sortKey = '$sortKey' and sortOrder = $sortOrder" in {
      Sorts.buildSort(Some(sortKey), sortOrder, domainId) should be(expectedBuilder)
    }

  "buildSort" should {
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
      actual should be(expected)
    }

    "order by score desc when given a simple query" in {
      val expected = Sorts.sortScoreDesc
      val searchContext = Domain(1, "peterschneider.net", None, None, None, false, false, true, false, false)
      val searchParams = SearchParamSet(searchQuery = SimpleQuery("soup salad sandwich"))
      val actual = Sorts.chooseSort(Some(searchContext), searchParams)
      actual should be(expected)
    }

    "order by average category score descending when no query but ODN categories present" in {
      val expected = buildAverageScoreSort(CategoriesFieldType.Score.fieldName, CategoriesFieldType.Name.rawFieldName, cats)
      val searchParams = SearchParamSet(tags = Some(tags), categories = Some(cats))
      val actual = Sorts.chooseSort(None, searchParams)
      actual should be(expected)
    }

    "order by average tag score desc when no query or categories but ODN tags present" in {
      val expected = buildAverageScoreSort(TagsFieldType.Score.fieldName, TagsFieldType.Name.rawFieldName, tags)
      val searchParams = SearchParamSet(tags = Some(tags))
      val actual = Sorts.chooseSort(None, searchParams)
      actual should be(expected)
    }

    "order by score descending for default null query" in {
      val expected = Sorts.sortScoreDesc
      val searchParams = SearchParamSet()
      val actual = Sorts.chooseSort(None, searchParams)
      actual should be (expected)
    }
  }
}
