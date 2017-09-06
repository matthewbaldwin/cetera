package com.socrata.cetera.search

import org.elasticsearch.index.query.QueryBuilders.{termQuery, termsQuery}
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

  def buildNestedUserSort(
      fieldName: String,
      domainId: Int,
      sortOrder: SortOrder)
  : FieldSortBuilder = {

    // NOTE: the sortMode shouldn't matter as only one nested role should come back from the filter
    val sortMode = SortMode.MIN

    SortBuilders
      .fieldSort(fieldName)
      .order(sortOrder)
      .sortMode(sortMode)
      .setNestedPath("roles")
      .setNestedFilter(termQuery(UserDomainId.fieldName, domainId))
  }

  /** Get the appropriate SortBuilder given a sort parameter string
    *
    * @param sortParam the sort parameter string
    * @return a SortBuilder
    */
  def mapSortParam(sortParam: String): Option[SortBuilder[_]] = // scalastyle:ignore cyclomatic.complexity method.length
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

      // Name
      case "name" => Some(sortFieldAsc(TitleFieldType.lowercaseAlphanumFieldName))
      case "name ASC" => Some(sortFieldAsc(TitleFieldType.lowercaseAlphanumFieldName))
      case "name DESC" => Some(sortFieldDesc(TitleFieldType.lowercaseAlphanumFieldName))

      // Owner
      case "owner" => Some(sortFieldAsc(OwnerDisplayNameFieldType.fieldName))
      case "owner ASC" => Some(sortFieldAsc(OwnerDisplayNameFieldType.fieldName))
      case "owner DESC" => Some(sortFieldDesc(OwnerDisplayNameFieldType.fieldName))

      // Category
      case "domain_category" => Some(sortFieldAsc(DomainCategoryFieldType.lowercaseAlphanumFieldName))
      case "domain_category ASC" => Some(sortFieldAsc(DomainCategoryFieldType.lowercaseAlphanumFieldName))
      case "domain_category DESC" => Some(sortFieldDesc(DomainCategoryFieldType.lowercaseAlphanumFieldName))

      // Datatype
      case "datatype" => Some(sortFieldAsc(DatatypeFieldType.fieldName))
      case "datatype ASC" => Some(sortFieldAsc(DatatypeFieldType.fieldName))
      case "datatype DESC" => Some(sortFieldDesc(DatatypeFieldType.fieldName))

      // Relevance
      case "relevance" => Some(sortScoreDesc)

      // Dataset ID
      case "dataset_id" => Some(sortDatasetId)

      // ScreenName (Users)
      case "screen_name" => Some(sortFieldAsc(UserScreenName.fieldName))
      case "screen_name ASC" => Some(sortFieldAsc(UserScreenName.fieldName))
      case "screen_name DESC" => Some(sortFieldDesc(UserScreenName.fieldName))

      // Email (Users)
      case "email" => Some(sortFieldAsc(UserEmail.fieldName))
      case "email ASC" => Some(sortFieldAsc(UserEmail.fieldName))
      case "email DESC" => Some(sortFieldDesc(UserEmail.fieldName))

      // RoleName (Users)
      case "role_name" => Some(sortFieldAsc(UserRoleName.fieldName))
      case "role_name ASC" => Some(sortFieldAsc(UserRoleName.fieldName))
      case "role_name DESC" => Some(sortFieldDesc(UserRoleName.fieldName))

      // LastAuthenticatedAt (Users)
      case "last_authenticated_at ASC" => Some(sortFieldAsc(UserLastAuthenticatedAt.fieldName))
      case "last_authenticated_at DESC" => Some(sortFieldDesc(UserLastAuthenticatedAt.fieldName))
      case "last_authenticated_at" => Some(sortFieldDesc(UserLastAuthenticatedAt.fieldName))

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

  def chooseUserSort(sortString: Option[String], domainId: Int): SortBuilder[_] =
    sortString match {
      case Some("email") | Some("email ASC") => sortFieldAsc(UserEmail.lowercaseAlphanumFieldName)
      case Some("email DESC") => sortFieldDesc(UserEmail.lowercaseAlphanumFieldName)
      case Some("screen_name") | Some("screen_name ASC") => sortFieldAsc(UserScreenName.lowercaseAlphanumFieldName)
      case Some("screen_name DESC") => sortFieldDesc(UserScreenName.lowercaseAlphanumFieldName)
      case Some("last_authenticated_at") | Some("last_authenticated_at ASC") =>
        buildNestedUserSort(UserLastAuthenticatedAt.fieldName, domainId, SortOrder.ASC)
      case Some("last_authenticated_at DESC") =>
        buildNestedUserSort(UserLastAuthenticatedAt.fieldName, domainId, SortOrder.DESC)
      case Some("role_name") | Some("role_name ASC") =>
        buildNestedUserSort(UserRoleName.lowercaseAlphanumFieldName, domainId, SortOrder.ASC)
      case Some("role_name DESC") =>
        buildNestedUserSort(UserRoleName.lowercaseAlphanumFieldName, domainId, SortOrder.DESC)
      case _ => Sorts.sortScoreDesc
    }
}
