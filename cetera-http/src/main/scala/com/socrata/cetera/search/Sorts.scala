package com.socrata.cetera.search

import org.elasticsearch.index.query.QueryBuilders.termsQuery
import org.elasticsearch.search.sort._

import com.socrata.cetera.handlers.SearchParamSet
import com.socrata.cetera.types._

// TODO: Ultimately, these should accept Sortables rather than Strings
object Sorts {
  def sortScoreDesc: ScoreSortBuilder =
    SortBuilders.scoreSort().order(SortOrder.DESC)

  def sortFieldAsc(field: String): FieldSortBuilder =
    SortBuilders.fieldSort(field).order(SortOrder.ASC)

  def sortFieldDesc(field: String): FieldSortBuilder =
    SortBuilders.fieldSort(field).order(SortOrder.DESC)

  def sortDatasetId: FieldSortBuilder =
    SortBuilders.fieldSort(SocrataIdDatasetIdFieldType.fieldName)

  def buildAverageScoreSort(
      fieldName: String,
      rawFieldName: String,
      classifications: Set[String])
    : FieldSortBuilder = {

    SortBuilders
      .fieldSort(fieldName)
      .order(SortOrder.DESC)
      .sortMode(SortMode.AVG)
      .setNestedFilter(termsQuery(rawFieldName, classifications.toSeq: _*))
  }

  /** Get the appropriate SortBuilder given a sort parameter string
    *
    * @param sortParam the sort parameter string
    * @return a SortBuilder
    */
  def mapSortParam(sortParam: String): Option[SortBuilder[_]] = // scalastyle:ignore cyclomatic.complexity
    sortParam match {
      // Timestamps
      case "createdAt ASC" => Some(sortFieldAsc(CreatedAtFieldType.fieldName))
      case "createdAt DESC" => Some(sortFieldDesc(CreatedAtFieldType.fieldName))
      case "createdAt" => Some(sortFieldDesc(CreatedAtFieldType.fieldName))
      case "updatedAt ASC" => Some(sortFieldAsc(UpdatedAtFieldType.fieldName))
      case "updatedAt DESC" => Some(sortFieldDesc(UpdatedAtFieldType.fieldName))
      case "updatedAt" => Some(sortFieldDesc(UpdatedAtFieldType.fieldName))

      // Page view
      case "page_views_last_week ASC" => Some(sortFieldAsc(PageViewsLastWeekFieldType.fieldName))
      case "page_views_last_week DESC" => Some(sortFieldDesc(PageViewsLastWeekFieldType.fieldName))
      case "page_views_last_week" => Some(sortFieldDesc(PageViewsLastWeekFieldType.fieldName))
      case "page_views_last_month ASC" => Some(sortFieldAsc(PageViewsLastMonthFieldType.fieldName))
      case "page_views_last_month DESC" => Some(sortFieldDesc(PageViewsLastMonthFieldType.fieldName))
      case "page_views_last_month" => Some(sortFieldDesc(PageViewsLastMonthFieldType.fieldName))
      case "page_views_total ASC" => Some(sortFieldAsc(PageViewsTotalFieldType.fieldName))
      case "page_views_total DESC" => Some(sortFieldDesc(PageViewsTotalFieldType.fieldName))
      case "page_views_total" => Some(sortFieldDesc(PageViewsTotalFieldType.fieldName))

      // Alphabetical
      case "name" => Some(sortFieldAsc(TitleFieldType.lowercaseAlphanumFieldName))
      case "name ASC" => Some(sortFieldAsc(TitleFieldType.lowercaseAlphanumFieldName))
      case "name DESC" => Some(sortFieldDesc(TitleFieldType.lowercaseAlphanumFieldName))

      // Relevance
      case "relevance" => Some(sortScoreDesc)

      // Dataset ID
      case "dataset_id" => Some(sortDatasetId)

      // Otherwise...
      case _ => None
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
