package com.socrata.cetera.search

import org.apache.lucene.search.join.ScoreMode
import org.elasticsearch.common.lucene.search.function.CombineFunction
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery.{ScoreMode => FnScoreMode}
import org.elasticsearch.index.query._
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.index.query.functionscore._

import com.socrata.cetera.auth.User
import com.socrata.cetera.{esDomainType, esDocumentType}
import com.socrata.cetera.errors.UnauthorizedError
import com.socrata.cetera.handlers.{ScoringParamSet, SearchParamSet}
import com.socrata.cetera.search.MultiMatchers.buildQuery
import com.socrata.cetera.types._

// TODO: This class is no longer required because ES automatically infers the field namespace correctly
// in the case of child aggregations. We should revert this to an object.
// scalastyle:ignore number.of.methods
case class DocumentQuery(forDomainSearch: Boolean = false) {
  def datatypeQuery(datatypes: Set[String]): TermsQueryBuilder =
    termsQuery(DatatypeFieldType.fieldName, datatypes.toSeq: _*)

  def userQuery(user: String): TermQueryBuilder =
    termQuery(OwnerIdFieldType.fieldName, user)

  def sharedToQuery(user: String): TermQueryBuilder =
    termQuery(SharedToFieldType.rawFieldName, user)

  def attributionQuery(attribution: String): TermQueryBuilder =
    termQuery(AttributionFieldType.rawFieldName, attribution)

  def provenanceQuery(provenance: String): TermQueryBuilder =
    termQuery(ProvenanceFieldType.rawFieldName, provenance)

  def licenseQuery(license: String): TermQueryBuilder =
    termQuery(LicenseFieldType.rawFieldName, license)

  def parentDatasetQuery(parentDatasetId: String): TermQueryBuilder =
    termQuery(ParentDatasetIdFieldType.fieldName, parentDatasetId)

  def hiddenFromCatalogQuery(hidden: Boolean = true): TermQueryBuilder =
    termQuery(HideFromCatalogFieldType.fieldName, hidden)

  def idQuery(ids: Set[String]): IdsQueryBuilder =
    idsQuery().types(esDocumentType).addIds(ids.toSeq: _*)

  def domainIdQuery(domainIds: Set[Int]): TermsQueryBuilder =
    termsQuery(SocrataIdDomainIdFieldType.fieldName, domainIds.toSeq: _*)

  def raStatusAccordingToParentDomainQuery(status: ApprovalStatus, hasStatus: Boolean = true): TermQueryBuilder =
    termQuery(status.raAccordingToParentField, hasStatus)

  def raStatusAccordingToContextQuery(status: ApprovalStatus, contextId: Int): TermQueryBuilder =
    termQuery(status.raQueueField, contextId)

  def modStatusQuery(status: ApprovalStatus): TermQueryBuilder =
    termQuery(ModerationStatusFieldType.fieldName, status.status)

  def defaultViewQuery(default: Boolean = true): TermQueryBuilder =
    termQuery(IsDefaultViewFieldType.fieldName, default)

  def publicQuery(public: Boolean = true): TermQueryBuilder =
    termQuery(IsPublicFieldType.fieldName, public)

  def publishedQuery(published: Boolean = true): TermQueryBuilder =
    termQuery(IsPublishedFieldType.fieldName, published)

  def customerCategoriesQuery(categories: Set[String]): TermsQueryBuilder =
    termsQuery(DomainCategoryFieldType.rawFieldName, categories.toSeq: _*)

  def customerTagsQuery(tags: Set[String]): TermsQueryBuilder =
    termsQuery(DomainTagsFieldType.rawFieldName, tags.toSeq: _*)

  def columnNamesQuery(columnNames: Set[String]): TermsQueryBuilder =
    termsQuery(ColumnNameFieldType.rawFieldName, columnNames.toSeq: _*)
  // ------------------------------------------------------------------------------------------

  // this query limits documents to those owned/shared to the user
  // or visible to user based on their domain and role
  def privateMetadataUserRestrictionsQuery(user: User): BoolQueryBuilder = {
    val userId = user.id
    val domainId = user.authenticatingDomain.map(_.domainId).getOrElse(0)
    val ownIt = userQuery(userId)
    val shareIt = sharedToQuery(userId)
    if (user.canViewPrivateMetadata(domainId)) {
      val beEnabledToSeeIt = domainIdQuery(Set(domainId))
      boolQuery().should(ownIt).should(shareIt).should(beEnabledToSeeIt)
    } else {
      boolQuery().should(ownIt).should(shareIt)
    }
  }

  // this query limits documents to those with metadata keys/values that
  // match the given metadata. If public is true, this matches against the public metadata fields,
  // otherwise, this matches against the private metadata fields.
  def metadataQuery(metadata: Set[(String, String)], public: Boolean): Option[BoolQueryBuilder] =
    if (metadata.nonEmpty) {
      val metadataGroupedByKey = metadata
        .groupBy { case (k, v) => k }
        .map { case (key, set) => key -> set.map(_._2) }
      val (parentField, keyField, valueField) = if (public) {
        DomainMetadataFieldType.fieldSet
      } else {
        DomainPrivateMetadataFieldType.fieldSet
      }
      val unionWithinKeys = metadataGroupedByKey.map { case (k, vs) =>
        vs.foldLeft(boolQuery()) { (b, v) =>
          b.should(
            nestedQuery(
              parentField,
              boolQuery()
                .must(termsQuery(keyField, k))
                .must(termsQuery(valueField, v)),
              ScoreMode.Avg
            )
          )
        }
      }

      if (metadataGroupedByKey.size == 1) {
        unionWithinKeys.headOption // no need to create an intersection for 1 key
      } else {
        val intersectAcrossKeys = unionWithinKeys.foldLeft(boolQuery()) { (b, q) => b.must(q) }
        Some(intersectAcrossKeys)
      }
    } else {
      None
    }

  // this query limits documents to those that both:
  //  - have private metadata keys/values matching the given metadata
  //  - have private metadata visible to the given user
  def privateMetadataQuery(metadata: Set[(String, String)], user: Option[User]): Option[BoolQueryBuilder] =
    if (metadata.nonEmpty) {
      user match {
        // if we haven't a user, don't build a query to search private metadata
        case None => None
        // if the user is a superadmin, use a metadata query pointed at the private fields
        case Some(u) if u.isSuperAdmin => metadataQuery(metadata, public = false)
        case Some(u) =>
          u.authenticatingDomain match {
            // if the user hasn't an authenticating domain, don't build a query to search private metadata
            case None => None
            // if the user has an authenticating domain, use a conditional query
            case Some(d) =>
              val beAbleToViewPrivateMetadata = privateMetadataUserRestrictionsQuery(u)
              val dmt = metadataQuery(metadata, public = false)
              dmt.map(matchMetadataTerms =>
                boolQuery().must(beAbleToViewPrivateMetadata).must(matchMetadataTerms))
          }
      }
    } else {
      None
    }

  // this query limits documents to those that either
  // - have public key/value pairs that match the given metadata
  // - have private key/value pairs that match the given metadata and have private metadata visible to the given user
  def combinedMetadataQuery(
      metadata: Set[(String, String)],
      user: Option[User])
    : Option[BoolQueryBuilder] = {
    val pubQuery = metadataQuery(metadata, public = true)
    val privateQuery = privateMetadataQuery(metadata, user)
    (pubQuery, privateQuery) match {
      case (None, None) => None
      case (Some(matchPublicMetadata), None) => Some(matchPublicMetadata)
      case (None, Some(matchPrivateMetadata)) => Some(matchPrivateMetadata)
      case (Some(matchPublicMetadata), Some(matchPrivateMetadata)) =>
        Some(boolQuery().should(matchPublicMetadata).should(matchPrivateMetadata))
    }
  }


  // if the context is moderated this will limit results to views from moderated domains + default
  // views from unmoderated sites
  def vmSearchContextQuery(domainSet: DomainSet): Option[BoolQueryBuilder] =
    domainSet.searchContext.collect { case c: Domain if c.moderationEnabled =>
      val beDefault = defaultViewQuery(default = true)
      val beFromModeratedDomain = domainIdQuery(domainSet.moderationEnabledIds)
      boolQuery().should(beDefault).should(beFromModeratedDomain)
    }

  // if the context has RA enabled, this will limit results to only those having been through or
  // are presently in the context's RA queue
  def raSearchContextQuery(domainSet: DomainSet): Option[BoolQueryBuilder] =
    domainSet.searchContext.collect { case c: Domain if c.routingApprovalEnabled =>
      val beApprovedByContext = raStatusAccordingToContextQuery(ApprovalStatus.approved, c.domainId)
      val beRejectedByContext = raStatusAccordingToContextQuery(ApprovalStatus.rejected, c.domainId)
      val bePendingWithinContextsQueue = raStatusAccordingToContextQuery(ApprovalStatus.pending, c.domainId)
      boolQuery().should(beApprovedByContext).should(beRejectedByContext).should(bePendingWithinContextsQueue)
    }

  // this query limits results based on search context
  //  - if view moderation is enabled, all derived views from unmoderated federated domains are removed
  //  - if R&A is enabled, all views whose self or parent has not been through the context's RA queue are removed
  def searchContextQuery(domainSet: DomainSet): Option[BoolQueryBuilder] = {
    val vmQuery = vmSearchContextQuery(domainSet)
    val raQuery = raSearchContextQuery(domainSet)
    List(vmQuery, raQuery).flatten match {
      case Nil => None
      case queries: Seq[QueryBuilder] => Some(queries.foldLeft(boolQuery()) { (b, f) => b.must(f) })
    }
  }

  // this query limits results down to views that are both
  //  - from the set of requested domains
  //  - should be included according to the search context and our funky federation rules
  def domainSetQuery(domainSet: DomainSet): BoolQueryBuilder = {
    val domainsQuery = domainIdQuery(domainSet.domains.map(_.domainId))
    val contextQuery = searchContextQuery(domainSet)
    val queries = List(domainsQuery) ++ contextQuery
    queries.foldLeft(boolQuery()) { (b, f) => b.must(f) }
  }

  // this query limits results to those with the given status
  def moderationStatusQuery(status: ApprovalStatus, domainSet: DomainSet): BoolQueryBuilder =
    status match {
      case ApprovalStatus.approved =>
        // to be approved a view must either be
        //   * a default/approved view from a moderated domain
        //   * any view from an unmoderated domain (as they are approved by default)
        // NOTE: funkiness with federation is handled by the searchContext filter.
        val beDefault = defaultViewQuery()
        val beApproved = modStatusQuery(status)
        val beFromUnmoderatedDomain = domainIdQuery(domainSet.moderationDisabledIds)
        boolQuery().should(beDefault).should(beApproved).should(beFromUnmoderatedDomain)
      case _ =>
        // to be rejected/pending, a view must be a derived view from a moderated domain with the given status
        val beFromModeratedDomain = domainIdQuery(domainSet.moderationEnabledIds)
        val beDerived = defaultViewQuery(false)
        val haveGivenStatus = modStatusQuery(status)
        boolQuery().must(beFromModeratedDomain).must(beDerived).must(haveGivenStatus)
    }

  def datalensStatusQuery(status: ApprovalStatus): BoolQueryBuilder = {
    val beADatalens = datatypeQuery(DatalensDatatype.allVarieties)
    status match {
      case ApprovalStatus.approved =>
        // limit results to those that are not unapproved datalens
        val beUnapproved = boolQuery().mustNot(modStatusQuery(status))
        boolQuery().mustNot(boolQuery().must(beADatalens).must(beUnapproved))
      case _ =>
        // limit results to those with the given status
        val haveGivenStatus = modStatusQuery(status)
        boolQuery().must(beADatalens).must(haveGivenStatus)
    }
  }

  // this limits results to those with the given R&A status on their parent domain
  def raStatusQuery(
      status: ApprovalStatus,
      domainSet: DomainSet)
    : BoolQueryBuilder = {
    status match {
      case ApprovalStatus.approved =>
        val beFromRADisabledDomain = domainIdQuery(domainSet.raDisabledIds)
        val haveGivenStatus = raStatusAccordingToParentDomainQuery(ApprovalStatus.approved)
        boolQuery()
          .should(beFromRADisabledDomain)
          .should(haveGivenStatus)
      case _ =>
        val beFromRAEnabledDomain = domainIdQuery(domainSet.raEnabledIds)
        val haveGivenStatus = raStatusAccordingToParentDomainQuery(status)
        boolQuery()
          .must(beFromRAEnabledDomain)
          .must(haveGivenStatus)
    }
  }

  // this limits results to those with the given R&A status on a given context
  def raStatusOnContextQuery(status: ApprovalStatus, domainSet: DomainSet): Option[TermQueryBuilder] =
    domainSet.searchContext.collect {
      case c: Domain if c.routingApprovalEnabled => raStatusAccordingToContextQuery(status, c.domainId)
    }

  // this limits results to those with a given approval status
  // - approved views must pass all 4 processes:
  //   1. viewModeration (if enabled on the domain)
  //   2. datalens moderation (if a datalens)
  //   3. R&A on the parent domain (if enabled on the parent domain)
  //   4. R&A on the context (if enabled on the context and you choose to include contextApproval)
  // - rejected/pending views need be rejected/pending by 1 or more processes
  def approvalStatusQuery(
      status: ApprovalStatus,
      domainSet: DomainSet,
      includeContextApproval: Boolean = true)
    : BoolQueryBuilder = {
    val haveGivenVMStatus = moderationStatusQuery(status, domainSet)
    val haveGivenDLStatus = datalensStatusQuery(status)
    val haveGivenRAStatus = raStatusQuery(status, domainSet)
    val haveGivenRAStatusOnContext = raStatusOnContextQuery(status, domainSet)

    status match {
      case ApprovalStatus.approved =>
        // if we are looking for approvals, a view must be approved according to all 3 or 4 processes
        val query = boolQuery().must(haveGivenVMStatus).must(haveGivenDLStatus).must(haveGivenRAStatus)
        if (includeContextApproval) haveGivenRAStatusOnContext.foreach(query.must)
        query
      case _ =>
        // otherwise, a view can fail due to any of the 3 or 4 processes
        val query = boolQuery().should(haveGivenVMStatus).should(haveGivenDLStatus).should(haveGivenRAStatus)
        if (includeContextApproval) haveGivenRAStatusOnContext.foreach(query.should)
        query
    }
  }

  // this limits results to the given socrata categories (this assumes no search context is present)
  def socrataCategoriesQuery(categories: Set[String]): BoolQueryBuilder =
    boolQuery().should(
      nestedQuery(
        CategoriesFieldType.fieldName,
        termsQuery(CategoriesFieldType.Name.rawFieldName, categories.toSeq: _*),
        ScoreMode.Avg
      )
    )

  // this filter limits results down to the given socrata tags (this assumes no search context is present)
  def socrataTagsQuery(tags: Set[String]): BoolQueryBuilder = {
    boolQuery().should(
      nestedQuery(
        TagsFieldType.fieldName,
        termsQuery(TagsFieldType.Name.rawFieldName, tags.toSeq: _*),
        ScoreMode.Avg
      )
    )
  }

 // this filter limits results down to the (socrata or customer) categories (depending on search context)
  def categoryQuery(categories: Set[String], context: Option[Domain]): Option[QueryBuilder] =
    if (forDomainSearch) {
      if (context.isDefined) Some(customerCategoriesQuery(categories)) else Some(socrataCategoriesQuery(categories))
    } else {
      None // when searching across documents, we prefer a match query over filters
    }

  // this filter limits results down to the (socrata or customer) tags (depending on search context)
  def tagQuery(tags: Set[String], context: Option[Domain]): Option[QueryBuilder] =
    if (forDomainSearch) {
      if (context.isDefined) Some(customerTagsQuery(tags)) else Some(socrataTagsQuery(tags))
    } else {
      None // when searching across documents, we prefer a match query over filters
    }

  // this limits results down to those requested by various search params
  def searchParamsQuery(
      searchParams: SearchParamSet,
      user: Option[User],
      domainSet: DomainSet)
    : Option[BoolQueryBuilder] = {
    val haveContext = domainSet.searchContext.isDefined
    val typeQuery = searchParams.datatypes.map(datatypeQuery(_))
    val ownerQuery = searchParams.user.map(userQuery(_))
    val sharingQuery = (user, searchParams.sharedTo) match {
      case (_, None) => None
      case (Some(u), Some(uid)) if (u.id == uid || u.isSuperAdmin) => Some(sharedToQuery(uid))
      case (_, _) => throw UnauthorizedError(user, "search another user's shared files")
    }
    val attrQuery = searchParams.attribution.map(attributionQuery(_))
    val provQuery = searchParams.provenance.map(provenanceQuery(_))
    val parentIdQuery = searchParams.parentDatasetId.map(parentDatasetQuery(_))
    val idsQuery = searchParams.ids.map(idQuery(_))
    val metaQuery = searchParams.domainMetadata.flatMap(combinedMetadataQuery(_, user))
    val derivationQuery = searchParams.derived.map(d => defaultViewQuery(!d))
    val licensesQuery = searchParams.license.map(licenseQuery(_))
    val colNamesQuery = searchParams.columnNames.map(columnNamesQuery)

    // we want to honor categories and tags in domain search only, and this can only be done via filters.
    // for document search however, we score these in queries
    val catQuery = searchParams.categories.flatMap(c => categoryQuery(c, domainSet.searchContext))
    val tagsQuery = searchParams.tags.flatMap(c => tagQuery(c, domainSet.searchContext))

    // the params below are those that would also influence visibility. these can only serve to further
    // limit the set of views returned from what the visibility queries allow.
    val privacyQuery = searchParams.public.map(publicQuery(_))
    val publicationQuery = searchParams.published.map(publishedQuery(_))
    val hiddenQuery = searchParams.explicitlyHidden.map(hiddenFromCatalogQuery(_))
    val approvalQuery = searchParams.approvalStatus.map(approvalStatusQuery(_, domainSet))

    List(typeQuery, ownerQuery, sharingQuery, attrQuery, provQuery, parentIdQuery, idsQuery, metaQuery,
      derivationQuery, privacyQuery, publicationQuery, hiddenQuery, approvalQuery, licensesQuery,
      colNamesQuery, catQuery, tagsQuery).flatten match {
      case Nil => None
      case queries: Seq[QueryBuilder] => Some(queries.foldLeft(boolQuery()) { (b, f) => b.must(f) })
    }
  }

  // this limits results to those that would show at /browse , i.e. public/published/approved/unhidden
  // optionally acknowledging the context, which sometimes should matter and sometimes should not
  def anonymousQuery(domainSet: DomainSet, includeContextApproval: Boolean = true): BoolQueryBuilder = {
    val bePublic = publicQuery(public = true)
    val bePublished = publishedQuery(published = true)
    val beApproved = approvalStatusQuery(ApprovalStatus.approved, domainSet, includeContextApproval)
    val beUnhidden = hiddenFromCatalogQuery(hidden = false)

    boolQuery()
      .must(bePublic)
      .must(bePublished)
      .must(beApproved)
      .must(beUnhidden)
  }

  // this will limit results down to those owned or shared to a user
  def ownedOrSharedQuery(user: User): BoolQueryBuilder = {
    val userId = user.id
    val ownerQuery = userQuery(userId)
    val sharedQuery = sharedToQuery(userId)
    boolQuery().should(ownerQuery).should(sharedQuery)
  }

  // this will limit results to only those the user is allowed to see
  def authQuery(user: Option[User], domainSet: DomainSet): Option[BoolQueryBuilder] =
    user match {
      // if user is super admin, no vis query needed
      case Some(u) if (u.isSuperAdmin) => None
      // if the user can view everything, they can only view everything on *their* domain
      // plus, of course, things they own/share and public/published/approved views from other domains
      case Some(u) if (u.authenticatingDomain.exists(d => u.canViewAllViews(d.domainId))) => {
        val personQuery = ownedOrSharedQuery(u)
        // re: includeContextApproval = false, in order for admins/etc to see views they've rejected from other domains,
        // we must allow them access to views that are approved on the parent domain, but not on the context.
        // yes, this is a snow-flaky case and it involves our auth. I am sorry  :(
        val anonQuery = anonymousQuery(domainSet, includeContextApproval = false)
        val fromUsersDomain = u.authenticatingDomain.map(d => domainIdQuery(Set(d.domainId)))
        val query = boolQuery().should(personQuery).should(anonQuery)
        fromUsersDomain.foreach(query.should(_))
        Some(query)
      }
      // if the user isn't a superadmin nor can they view everything, they may only see
      // things they own/share and public/published/approved views
      case Some(u) => {
        val personQuery = ownedOrSharedQuery(u)
        val anonQuery = anonymousQuery(domainSet)
        Some(boolQuery().should(personQuery).should(anonQuery))
      }
      // if the user is anonymous, they can only view anonymously-viewable views
      case None => Some(anonymousQuery(domainSet))
    }

  // TODO: rename this; most queries are composites and thus this isn't very descriptive
  def compositeQuery(
      domainSet: DomainSet,
      searchParams: SearchParamSet,
      user: Option[User])
    : BoolQueryBuilder = {
    val domainQueries = Some(domainSetQuery(domainSet))
    val authQueries = authQuery(user, domainSet)
    val searchQueries = searchParamsQuery(searchParams, user, domainSet)

    val allQueries = List(domainQueries, authQueries, searchQueries).flatten
    allQueries.foldLeft(boolQuery()) { (b, f) => b.must(f) }
  }

  private def applyClassificationQuery(
      query: QueryBuilder,
      searchParams: SearchParamSet,
      withinSearchContext: Boolean)
    : QueryBuilder = {

    // If there is no search context, use the ODN categories and tags
    // otherwise use the custom domain categories and tags
    val categoriesAndTags: Seq[QueryBuilder] =
      if (withinSearchContext) {
        List.concat(
          domainCategoriesQuery(searchParams.categories),
          domainTagsQuery(searchParams.tags))
      } else {
        List.concat(
          categoriesQuery(searchParams.categories),
          tagsQuery(searchParams.tags))
      }

    if (categoriesAndTags.nonEmpty) {
      categoriesAndTags.foldLeft(boolQuery().must(query)) { (b, q) => b.must(q) }
    } else {
      query
    }
  }

  def chooseMatchQuery(
      searchQuery: QueryType,
      scoringParams: ScoringParamSet,
      user: Option[User])
    : BoolQueryBuilder =
    searchQuery match {
      case NoQuery => noQuery
      case AdvancedQuery(queryString) => advancedQuery(queryString, scoringParams.fieldBoosts)
      case SimpleQuery(queryString) => simpleQuery(queryString, scoringParams, user)
    }

  def noQuery: BoolQueryBuilder =
    boolQuery().must(matchAllQuery())

  // By default, the must match clause enforces a term match such that one or more of the query terms
  // must be present in at least one of the fields specified. The optional minimum_should_match
  // constraint applies to this clause. The should clause is intended to give a subset of retrieved
  // results boosts based on:
  //
  //   1. better phrase matching in the case of multiterm queries
  //   2. matches in particular fields (specified in fieldBoosts)
  //
  // The scores are then averaged together by ES with a defacto score of 0 for a should
  // clause if it does not in fact match any documents. See the ElasticSearch
  // documentation here:
  //
  //   https://www.elastic.co/guide/en/elasticsearch/guide/current/proximity-relevance.html
  def simpleQuery(
      queryString: String,
      scoringParams: ScoringParamSet,
      user: Option[User])
    : BoolQueryBuilder = {
    val matchTerms = buildQuery(queryString, MultiMatchQueryBuilder.Type.CROSS_FIELDS, scoringParams, user)
    val matchPhrase = buildQuery(queryString, MultiMatchQueryBuilder.Type.PHRASE, scoringParams, user)
    // terms must match and phrases should match
    boolQuery().must(matchTerms).should(matchPhrase)
  }

  // The advanced query allows a user to directly pass in lucene queries
  // NOTE: Advanced queries respect fieldBoosts but not datatypeBoosts
  // Q: Is this expected and desired?
  def advancedQuery(
      queryString: String,
      fieldBoosts: Map[CeteraFieldType with Boostable, Float])
    : BoolQueryBuilder = {

    val documentQuery = queryStringQuery(queryString)
      .field(FullTextSearchAnalyzedFieldType.fieldName)
      .field(FullTextSearchRawFieldType.fieldName)
      .autoGeneratePhraseQueries(true)

    val domainQuery = queryStringQuery(queryString)
      .field(FullTextSearchAnalyzedFieldType.fieldName)
      .field(FullTextSearchRawFieldType.fieldName)
      .field(DomainCnameFieldType.fieldName)
      .autoGeneratePhraseQueries(true)

    fieldBoosts.foreach { case (field, weight) =>
      documentQuery.field(field.fieldName, weight)
      domainQuery.field(field.fieldName, weight)
    }

    boolQuery()
      .should(documentQuery)
      .should(hasParentQuery(esDomainType, domainQuery, false))
  }

  def autocompleteQuery(queryString: String): MatchQueryBuilder =
    matchQuery(TitleFieldType.autocompleteFieldName, queryString)

  // TODO: rename; most queries are composites and thus this isn't very descriptive and on top of
  // that, this is no longer a filteredQuery (which no longer exists in Elasticsearch 5.x)
  def compositeFilteredQuery(
      domainSet: DomainSet,
      searchParams: SearchParamSet,
      query: QueryBuilder,
      user: Option[User])
    : BoolQueryBuilder = {

    val categoriesAndTagsQuery = applyClassificationQuery(query, searchParams, domainSet.searchContext.isDefined)

    // This query incorporates all of the remaining constraints. These constraints determine
    // whether a document is considered part of the selection set, but they do not affect the
    // relevance score of the document.
    boolQuery()
      .filter(compositeQuery(domainSet, searchParams, user))
      .must(categoriesAndTagsQuery)
  }

  def categoriesQuery(categories: Option[Set[String]]): Option[NestedQueryBuilder] =
    categories.map { cs =>
      nestedQuery(
        CategoriesFieldType.fieldName,
        cs.foldLeft(boolQuery().minimumShouldMatch(1)) { (b, q) =>
          b.should(matchPhraseQuery(CategoriesFieldType.Name.fieldName, q))
        },
        ScoreMode.Avg
      )
    }

  def tagsQuery(tags: Option[Set[String]]): Option[NestedQueryBuilder] =
    tags.map { tags =>
      nestedQuery(
        TagsFieldType.fieldName,
        tags.foldLeft(boolQuery().minimumShouldMatch(1)) { (b, q) =>
          b.should(matchPhraseQuery(TagsFieldType.Name.fieldName, q))
        },
        ScoreMode.Avg
      )
    }

  def domainCategoriesQuery(categories: Option[Set[String]]): Option[BoolQueryBuilder] =
    categories.map { cs =>
      cs.foldLeft(boolQuery().minimumShouldMatch(1)) { (b, q) =>
        b.should(matchPhraseQuery(DomainCategoryFieldType.fieldName, q))
      }
    }

  def domainTagsQuery(tags: Option[Set[String]]): Option[BoolQueryBuilder] =
    tags.map { ts =>
      ts.foldLeft(boolQuery().minimumShouldMatch(1)) { (b, q) =>
        b.should(matchPhraseQuery(DomainTagsFieldType.fieldName, q))
      }
    }

  def scoringQuery(
      query: QueryBuilder,
      domainSet: DomainSet,
      scriptScoreFunctions: Set[ScriptScoreFunction],
      scoringParams: ScoringParamSet)
    : FunctionScoreQueryBuilder = {
    val scoreFunctions = Boosts.scriptScoreFunctions(scriptScoreFunctions)
    val dataTypeBoosts = Boosts.datatypeBoostFunctions(scoringParams.datatypeBoosts)
    val domainBoosts = Boosts.domainBoostFunctions(domainSet.domainIdBoosts)
    val officialBoost = Boosts.officialBoostFunction(scoringParams.officialBoost)
    val ageDecay = scoringParams.ageDecay.map(Boosts.ageDecayFunction)

    val allScoringFunctions =
      scoreFunctions ++ dataTypeBoosts ++ domainBoosts ++ officialBoost ++ ageDecay

    val fnScoreQuery = functionScoreQuery(query, allScoringFunctions.toArray)
    fnScoreQuery.scoreMode(FnScoreMode.MULTIPLY).boostMode(CombineFunction.REPLACE)
  }
}
