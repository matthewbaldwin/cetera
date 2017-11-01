package com.socrata.cetera.search

import org.elasticsearch.index.query.QueryBuilders.{termQuery, termsQuery}
import org.elasticsearch.search.sort._

import com.socrata.cetera.handlers.SearchParamSet
import com.socrata.cetera.search.Sorts.{Ascending, Descending}
import com.socrata.cetera.types._

trait Sort {
  def field: String
  def defaultOrder: String
}

case class SimpleSort(
    override val field: String,
    override val defaultOrder: String
) extends Sort {

  def buildSort(sortOrder: Option[String]): SortBuilder[_] =
    sortOrder.getOrElse(defaultOrder) match {
      case Ascending => Sorts.sortFieldAsc(field)
      case _ => Sorts.sortFieldDesc(field)
    }
}

object SimpleSort {
  def apply(sortKey: String): Option[SimpleSort] = // scalastyle:ignore cyclomatic.complexity
    sortKey match {
      case "createdAt" => Some(SimpleSort(CreatedAtFieldType.fieldName, Descending))
      case "created_at" => Some(SimpleSort(CreatedAtFieldType.fieldName, Descending))
      case "updatedAt" => Some(SimpleSort(UpdatedAtFieldType.fieldName, Descending))
      case "updated_at" => Some(SimpleSort(UpdatedAtFieldType.fieldName, Descending))
      case "page_views_last_week" => Some(SimpleSort(PageViewsLastWeekFieldType.fieldName, Descending))
      case "page_views_last_month" => Some(SimpleSort(PageViewsLastMonthFieldType.fieldName, Descending))
      case "page_views_total" => Some(SimpleSort(PageViewsTotalFieldType.fieldName, Descending))
      case "name" => Some(SimpleSort(TitleFieldType.lowercaseAlphanumFieldName, Ascending))
      case "owner" => Some(SimpleSort(OwnerDisplayNameFieldType.fieldName, Ascending))
      case "domain_category" => Some(SimpleSort(DomainCategoryFieldType.lowercaseAlphanumFieldName, Ascending))
      case "datatype" => Some(SimpleSort(DatatypeFieldType.fieldName, Ascending))
      case "screen_name" => Some(SimpleSort(UserScreenName.lowercaseAlphanumFieldName, Ascending))
      case "email" => Some(SimpleSort(UserEmail.lowercaseAlphanumFieldName, Ascending))
      case _ => None
    }
}

case class NestedSort(
    override val field: String,
    override val defaultOrder: String
) extends Sort {

  def buildSort(sortOrder: Option[String], domainId: Option[Int]): SortBuilder[_] =
    domainId match {
      case None =>
        sortOrder.getOrElse(defaultOrder) match {
          case Ascending => Sorts.buildNestedApprovalsSort(field, SortOrder.ASC)
          case _ => Sorts.buildNestedApprovalsSort(field, SortOrder.DESC)
        }
      case Some(id) =>
        sortOrder.getOrElse(defaultOrder) match {
          case Ascending => Sorts.buildNestedUserSort(field, id, SortOrder.ASC)
          case _ => Sorts.buildNestedUserSort(field, id, SortOrder.DESC)
        }
    }
}

object NestedSort {
  def apply(sortKey: String): Option[NestedSort] =
    sortKey match {
      case "submitted_at" => Some(NestedSort(SubmittedAtFieldType.fieldName, Descending))
      case "reviewed_at" => Some(NestedSort(ReviewedAtFieldType.fieldName, Descending))
      case "submitter" => Some(NestedSort(SubmitterNameFieldType.fieldName, Ascending))
      case "reviewer" => Some(NestedSort(ReviewerNameFieldType.fieldName, Ascending))
      case "role_name" => Some(NestedSort(UserRoleName.lowercaseAlphanumFieldName, Ascending))
      case "last_authenticated_at" => Some(NestedSort(UserLastAuthenticatedAt.fieldName, Descending))
      case _ => None
    }
}


// TODO: Ultimately, these should accept Sortables rather than Strings
object Sorts {

  val SortModeForSoloRes = SortMode.MIN // NOTE: the sortMode doesn't matter if only one item is returned
  val Ascending = "ASC"
  val Descending = "DESC"

  def sortScoreDesc: ScoreSortBuilder =
    SortBuilders.scoreSort().order(SortOrder.DESC)

  def sortFieldAsc(field: String): FieldSortBuilder =
    SortBuilders.fieldSort(field).order(SortOrder.ASC)

  def sortFieldDesc(field: String): FieldSortBuilder =
    SortBuilders.fieldSort(field).order(SortOrder.DESC)

  def sortDatasetId: FieldSortBuilder =
    SortBuilders.fieldSort(SocrataIdDatasetIdFieldType.fieldName)

  def buildAverageScoreSort(fieldName: String, rawFieldName: String, classifications: Set[String]): FieldSortBuilder =
    SortBuilders
      .fieldSort(fieldName)
      .order(SortOrder.DESC)
      .sortMode(SortMode.AVG)
      .setNestedFilter(termsQuery(rawFieldName, classifications.toSeq: _*))

  def buildNestedUserSort(fieldName: String, domainId: Int, sortOrder: SortOrder): FieldSortBuilder =
    SortBuilders
      .fieldSort(fieldName)
      .order(sortOrder)
      .sortMode(SortModeForSoloRes) // NOTE: only one nested role comes back from filter
      .setNestedPath("roles")
      .setNestedFilter(termQuery(UserDomainId.fieldName, domainId))

  def buildNestedApprovalsSort(fieldName: String, sortOrder: SortOrder): FieldSortBuilder =
    SortBuilders
      .fieldSort(fieldName)
      .order(sortOrder)
      .sortMode(SortModeForSoloRes) // NOTE: only should have one workflow and thus one approval summary
      .setNestedPath("approvals")

  private def simpleOrNestedSort(sortKey: String): Either[SimpleSort, NestedSort] =
    (SimpleSort(sortKey), NestedSort(sortKey)) match {
      case (Some(simpleSort), _) => Left(simpleSort)
      case (_, Some(nestedSort)) => Right(nestedSort)
      case _ => throw new IllegalArgumentException(s"'$sortKey' is not a valid sort key")
    }

  def buildSort(
      sortKey: Option[String],
      sortOrder: Option[String],
      domainId: Option[Int] = None): SortBuilder[_] =
    sortKey match {
      case Some("relevance") => sortScoreDesc
      case Some("dataset_id") => sortDatasetId
      case Some(key) =>
        simpleOrNestedSort(key) match {
          case Left(simpleSort) => simpleSort.buildSort(sortOrder)
          case Right(nestedSort) => nestedSort.buildSort(sortOrder, domainId)
        }
      case None => sortScoreDesc
    }

  /** Get the appropriate SortBuilder given a searchContext and a searchParams
    *
    * First pass logic is very simple. query >> categories >> tags >> default
    *
    * @param searchContext an optional search context domain
    * @param searchParams the SearchParamSet
    * @return a SortBuilder
    */
  def chooseSort(searchContext: Option[Domain], searchParams: SearchParamSet): SortBuilder[_] =
    (searchParams.searchQuery, searchContext, searchParams.categories, searchParams.tags) match {
      // ODN Categories
      case (NoQuery, None, Some(cats), _) =>
        buildAverageScoreSort(
          CategoriesFieldType.Score.fieldName,
          CategoriesFieldType.Name.rawFieldName,
          cats
        )

      // ODN Tags
      case (NoQuery, None, None, Some(ts)) =>
        buildAverageScoreSort(
          TagsFieldType.Score.fieldName,
          TagsFieldType.Name.rawFieldName,
          ts
        )

      // Everything else
      case _ => Sorts.sortScoreDesc
    }
}
