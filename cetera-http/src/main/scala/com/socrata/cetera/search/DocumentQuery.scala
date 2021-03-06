package com.socrata.cetera.search

import org.apache.lucene.search.join.ScoreMode
import org.elasticsearch.common.lucene.search.function.CombineFunction
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery.{ScoreMode => FnScoreMode}
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.index.query._
import org.elasticsearch.index.query.functionscore._

import com.socrata.cetera.auth.AuthedUser
import com.socrata.cetera.errors.UnauthorizedError
import com.socrata.cetera.handlers.{ScoringParamSet, SearchParamSet}
import com.socrata.cetera.search.MultiMatchers.buildQuery
import com.socrata.cetera.types._
import com.socrata.cetera.{esDocumentType, esDomainType}

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

  def isStoryQuery: TermQueryBuilder =
    termQuery(DatatypeFieldType.fieldName, StoryDatatype.singular)

  def publicQuery(public: Boolean = true): TermQueryBuilder =
    termQuery(IsPublicFieldType.fieldName, public)

  def publishedQuery(published: Boolean = true): TermQueryBuilder =
    termQuery(IsPublishedFieldType.fieldName, published)

  def customerCategoriesQuery(categories: Set[String]): TermsQueryBuilder =
    termsQuery(DomainCategoryFieldType.rawFieldName, categories.toSeq: _*)

  def customerTagsQuery(tags: Set[String]): TermsQueryBuilder =
    termsQuery(DomainTagsFieldType.rawFieldName, tags.toSeq: _*)

  def columnNamesQuery(columnNames: Set[String]): BoolQueryBuilder =
    columnNames.foldLeft(boolQuery().minimumShouldMatch(1)) { (b, q) =>
      b.should(matchQuery(ColumnNameFieldType.lowercaseFieldName, q))
    }

  def nestedApprovalQuery(approvalQuery: TermQueryBuilder): NestedQueryBuilder =
    nestedQuery("approvals", approvalQuery, ScoreMode.Min) // NOTE: should only ever be one, so Mode matters not

  def sumbitterIdQuery(id: String): NestedQueryBuilder =
    nestedApprovalQuery(termQuery(SubmitterIdFieldType.fieldName, id))

  def reviewerIdQuery(id: String): NestedQueryBuilder =
    nestedApprovalQuery(termQuery(ReviewerIdFieldType.fieldName, id))

  def reviewedAutomaticallyQuery(auto: Boolean = true): NestedQueryBuilder =
    nestedApprovalQuery(termQuery(ReviewerAutomaticallyFieldType.fieldName, auto))

  def fontanaStateQuery(status: ApprovalStatus): NestedQueryBuilder =
    nestedApprovalQuery(termQuery(FontanaStateFieldType.fieldName, status.status))

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

  // this query limits documents to those owned/shared to the user
  // or visible to user based on their domain and rights
  // NOTE: Cetera relies on the `edit_*` user rights as a proxy for the `update_view` right, which is the user/view
  // combo right that core uses to determine private metadata visibility.
  def privateMetadataUserRestrictionsQuery(user: AuthedUser): BoolQueryBuilder = {
    val userId = user.id
    val authingDomainId = user.authenticatingDomain.id
    val ownIt = userQuery(userId)
    val sharedIt = sharedToQuery(userId)
    val beOnUsersDomain = domainIdQuery(Set(authingDomainId))
    val beAStory = isStoryQuery
    if (user.canViewAllPrivateMetadata(authingDomainId)) {
      // if the user can view all private metadata, it's all the private metadata on *their* domain
      boolQuery().should(ownIt).should(sharedIt).should(beOnUsersDomain)
    } else if (user.canViewAllOfSomePrivateMetadata(authingDomainId, isStory = false)) {
      // the user can only view the private metadata of non-stories on their domain
      val beANonStoryOnUsersDomain = boolQuery().must(beOnUsersDomain).mustNot(beAStory)
      boolQuery().should(ownIt).should(sharedIt).should(beANonStoryOnUsersDomain)
    } else if (user.canViewAllOfSomePrivateMetadata(authingDomainId, isStory = true)) {
      // the user can only view the private metadata of stories on their domain
      val beAStoryOnUsersDomain = boolQuery().must(beOnUsersDomain).must(beAStory)
      boolQuery().should(ownIt).should(sharedIt).should(beAStoryOnUsersDomain)
    } else {
      boolQuery().should(ownIt).should(sharedIt)
    }
  }

  // this query limits documents to those that both:
  //  - have private metadata keys/values matching the given metadata
  //  - have private metadata visible to the given user
  def privateMetadataQuery(metadata: Set[(String, String)], user: Option[AuthedUser]): Option[BoolQueryBuilder] =
    if (metadata.nonEmpty) {
      user match {
        // if we haven't a user, don't build a query to search private metadata
        case None => None
        // if the user is a superadmin, use a metadata query pointed at the private fields
        case Some(u) if u.isSuperAdmin => metadataQuery(metadata, public = false)
        // otherwise, restrict the metadata query to those docs where the user can view their private metadata
        case Some(u) =>
          val beAbleToViewPrivateMetadata = privateMetadataUserRestrictionsQuery(u)
          metadataQuery(metadata, public = false).map(matchMetadataTerms =>
            boolQuery().must(beAbleToViewPrivateMetadata).must(matchMetadataTerms))
      }
    } else {
      None
    }

  // this query limits documents to those that either
  // - have public key/value pairs that match the given metadata
  // - have private key/value pairs that match the given metadata and have private metadata visible to the given user
  def combinedMetadataQuery(
      metadata: Set[(String, String)],
      user: Option[AuthedUser])
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

  // this query limits results to only stories or non-stories on a domain
  def notQuiteEverythingOnADomainQuery(domainId: Int, includeStories: Boolean): BoolQueryBuilder = {
    val beAStory = isStoryQuery
    val beOnDomain = domainIdQuery(Set(domainId))
    if (includeStories) {
      boolQuery().must(beOnDomain).must(beAStory)
    } else {
      boolQuery().must(beOnDomain).mustNot(beAStory)
    }
  }

  // if this domainSet represents a federated case, this query limits results to
  // - docs on the context
  // - anonymously viewable docs on the federated domains
  def federationQuery(domainSet: DomainSet): Option[BoolQueryBuilder] =
    domainSet.searchContext.flatMap { context =>
      val federatedDomains = domainSet.domains - context
      if (federatedDomains.size == 0) {
        None
      } else {
        val beFromContext = domainIdQuery(Set(context.id))
        val beFromFederatedDomains = domainIdQuery(federatedDomains.map(_.id))
        val beAnonymouslyViewable = anonymousQuery(DomainSet(federatedDomains))
        val beAnonymouslyViewableOnFederatedDomains =
          boolQuery().must(beFromFederatedDomains).must(beAnonymouslyViewable)
        Some(boolQuery().should(beFromContext).should(beAnonymouslyViewableOnFederatedDomains))
      }
    }

  // if the context is moderated this will limit results to views from moderated/fontana domains + default
  // views from unmoderated sites
  def vmSearchContextQuery(domainSet: DomainSet): Option[BoolQueryBuilder] =
    domainSet.searchContext.collect { case c: Domain if c.moderationEnabled =>
      val beDefault = defaultViewQuery(default = true)
      val beFromModeratedDomain = domainIdQuery(domainSet.moderationEnabledIds)
      val beFromFontanaDomain = domainIdQuery(domainSet.hasFontanaApprovals)
      boolQuery().should(beDefault).should(beFromModeratedDomain).should(beFromFontanaDomain)
    }

  // if the context has RA enabled, this will limit results to only those having been through or
  // are presently in the context's RA queue or are from a fontana domain
  def raSearchContextQuery(domainSet: DomainSet): Option[BoolQueryBuilder] =
    domainSet.searchContext.collect { case c: Domain if c.routingApprovalEnabled =>
      val beApprovedByContext = raStatusAccordingToContextQuery(ApprovalStatus.approved, c.id)
      val beRejectedByContext = raStatusAccordingToContextQuery(ApprovalStatus.rejected, c.id)
      val bePendingWithinContextsQueue = raStatusAccordingToContextQuery(ApprovalStatus.pending, c.id)
      val beFromFontanaDomain = domainIdQuery(domainSet.hasFontanaApprovals)
      boolQuery()
        .should(beApprovedByContext)
        .should(beRejectedByContext)
        .should(bePendingWithinContextsQueue)
        .should(beFromFontanaDomain)
    }

  // this query limits results based on search context
  //  - if the context has incoming federated data, it must be anonymously viewable
  //  - if view moderation is enabled, all derived views from unmoderated/non-fontana federated domains are removed
  //  - if R&A is enabled, all views from non-fontana domains whose self or parent
  //    has not been through the context's RA queue are removed
  def searchContextQuery(domainSet: DomainSet): Option[BoolQueryBuilder] = {
    val fedQuery = federationQuery(domainSet)
    val vmQuery = vmSearchContextQuery(domainSet)
    val raQuery = raSearchContextQuery(domainSet)
    List(fedQuery, vmQuery, raQuery).flatten match {
      case Nil => None
      case queries: Seq[QueryBuilder] => Some(queries.foldLeft(boolQuery()) { (b, f) => b.must(f) })
    }
  }

  // this query limits results down to views that are both
  //  - from the set of requested domains
  //  - should be included according to the search context and our funky federation rules
  def domainSetQuery(domainSet: DomainSet): BoolQueryBuilder = {
    val domainsQuery = domainIdQuery(domainSet.domains.map(_.id))
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
      case c: Domain if c.routingApprovalEnabled => raStatusAccordingToContextQuery(status, c.id)
    }

  // this limits results to those with a given approval status
  // - approved views must pass all 3 processes on non-Fontana domains:
  //   1. viewModeration (if enabled on the domain)
  //   2. R&A on the parent domain (if enabled on the parent domain)
  //   3. R&A on the context (if enabled on the context and you choose to include contextApproval)
  // - approved views must be approved by the one workflow for Fontana domains
  // - rejected/pending views need be rejected/pending by 1 or more processes for non-Fontana domains
  // - rejected/pending views need be rejected/pending by the one workflow for Fontana domains
  def approvalStatusQuery(
      status: ApprovalStatus,
      domainSet: DomainSet,
      includeContextApproval: Boolean = true)
    : BoolQueryBuilder = {
    val haveGivenVMStatus = moderationStatusQuery(status, domainSet)
    val haveGivenRAStatus = raStatusQuery(status, domainSet)
    val haveGivenRAStatusOnContext = raStatusOnContextQuery(status, domainSet)
    val beFromNonFontanaApprovalsDomain = domainIdQuery(domainSet.lacksFontanaApprovals)
    val haveFontanaApprovalStatus = fontanaStateQuery(status)
    val beFromFontanaApprovalsDomain = domainIdQuery(domainSet.hasFontanaApprovals)

    val haveOldStatus = status match {
      case ApprovalStatus.approved =>
        // if we are looking for approvals, a view must be approved according to all 3 processes
        val query = boolQuery().must(haveGivenVMStatus).must(haveGivenRAStatus)
        if (includeContextApproval) haveGivenRAStatusOnContext.foreach(query.must)
        query
      case _ =>
        // otherwise, a view can fail due to any of the 3 processes
        val query = boolQuery().should(haveGivenVMStatus).should(haveGivenRAStatus)
        if (includeContextApproval) haveGivenRAStatusOnContext.foreach(query.should)
        query
    }
    val haveOldStatusOnNonFontanaDomains = boolQuery().must(beFromNonFontanaApprovalsDomain).must(haveOldStatus)
    val haveNewStatusOnFontanaDomains = boolQuery().must(beFromFontanaApprovalsDomain).must(haveFontanaApprovalStatus)

    boolQuery().should(haveOldStatusOnNonFontanaDomains).should(haveNewStatusOnFontanaDomains)
  }

  // this limits results to only
  //   - those in the public catalog when the 'visibility' param is set to 'open' OR
  //   - those not in the public catalog when the 'visibility' param is set to 'internal'
  def visibilityStatusQuery(visibility: String, domainSet: DomainSet): BoolQueryBuilder = {
    val anonQuery = anonymousQuery(domainSet)
    if (visibility == VisibilityStatus.internal) {
      boolQuery().mustNot(anonQuery)
    } else {
      anonQuery
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
      user: Option[AuthedUser],
      domainSet: DomainSet)
    : Option[BoolQueryBuilder] = {
    val haveContext = domainSet.searchContext.isDefined
    val typeQuery = searchParams.datatypes.map(datatypeQuery(_))
    val ownerQuery = searchParams.user.map(userQuery(_))
    val sharingQuery = (user, searchParams.sharedTo) match {
      case (_, None) => None
      case (Some(u), Some(uid)) if (u.id == uid || u.isSuperAdmin) => Some(sharedToQuery(uid))
      case (_, _) => throw UnauthorizedError(user.map(_.id), "search another user's shared files")
    }
    val attrQuery = searchParams.attribution.map(attributionQuery(_))
    val provQuery = searchParams.provenance.map(provenanceQuery(_))
    val parentIdQuery = searchParams.parentDatasetId.map(parentDatasetQuery(_))
    val idsQuery = searchParams.ids.map(idQuery(_))
    val metaQuery = searchParams.domainMetadata.flatMap(combinedMetadataQuery(_, user))
    val derivationQuery = searchParams.derived.map(d => defaultViewQuery(!d))
    val licensesQuery = searchParams.license.map(licenseQuery(_))
    val colNamesQuery = searchParams.columnNames.map(columnNamesQuery)
    val submitterQuery = searchParams.submitterId.map(sumbitterIdQuery(_))
    val reviewerQuery = searchParams.reviewerId.map(reviewerIdQuery(_))
    val reviewedAutoQuery = searchParams.reviewedAutomatically.map(reviewedAutomaticallyQuery(_))

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
    val visibilityQuery = searchParams.visibility.map(visibilityStatusQuery(_, domainSet))

    List(
      typeQuery, ownerQuery, sharingQuery, attrQuery, provQuery, parentIdQuery, idsQuery, metaQuery,
      derivationQuery, licensesQuery, colNamesQuery, submitterQuery, reviewerQuery, reviewedAutoQuery,
      catQuery, tagsQuery, privacyQuery, publicationQuery, hiddenQuery, approvalQuery, visibilityQuery
    ).flatten match {
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

  // this will limit results to only those the user is allowed to see
  def authQuery(user: Option[AuthedUser], domainSet: DomainSet): Option[BoolQueryBuilder] =
    user match {
      case Some(u) =>
        if (u.isSuperAdmin) {
          // if user is super admin, no vis query needed
          None
        } else {
          val authingDomainId = u.authenticatingDomain.id
          // the user may see everything they own or share
          val ownIt = userQuery(u.id)
          val sharedIt = sharedToQuery(u.id)
          // and the user may see all anonymously viewable docs
          //   re: includeContextApproval = false, in order for admins/etc to see views they've rejected from other
          //   domains, we must allow them access to views that are approved on the federated domain, but not on the
          //   context. yes, this is a snow-flaky case and it involves our auth. I am sorry  :(
          val bePublicallyAvailable =
            anonymousQuery(domainSet, includeContextApproval = !u.canViewAllViews(authingDomainId))
          // the user may be able to see more: all stories, all non-stories or both.
          // but if so, they can only view those views on *their* domain
          val otherViewsAvailable =
            if (u.canViewAllViews(authingDomainId)) {
              Some(domainIdQuery(Set(authingDomainId))) // allow everything on the domain
            } else if (u.canViewAllOfSomeViews(authingDomainId, isStory = false)) {
              Some(notQuiteEverythingOnADomainQuery(authingDomainId, includeStories = false)) // allow all non-stories
            } else if (u.canViewAllOfSomeViews(authingDomainId, isStory = true)) {
              Some(notQuiteEverythingOnADomainQuery(authingDomainId, includeStories = true)) // allow all stories
            } else {
              None
            }
          val query = boolQuery().should(ownIt).should(sharedIt).should(bePublicallyAvailable)
          otherViewsAvailable.foreach(query.should(_))
          Some(query)
        }
      // if the user is anonymous, they can only view anonymously-viewable views
      case None => Some(anonymousQuery(domainSet))
    }

  // TODO: rename this; most queries are composites and thus this isn't very descriptive
  def compositeQuery(
      domainSet: DomainSet,
      searchParams: SearchParamSet,
      user: Option[AuthedUser])
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
      user: Option[AuthedUser])
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
      user: Option[AuthedUser])
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
      .field(CnamesFieldType.fieldName)
      .autoGeneratePhraseQueries(true)

    fieldBoosts.foreach { case (field, weight) =>
      documentQuery.field(field.fieldName, weight)
      domainQuery.field(field.fieldName, weight)
    }

    boolQuery()
      .should(documentQuery)
      .should(hasParentQuery(esDomainType, domainQuery, false))
  }

  def autocompleteQuery(queryString: String, scoringParams: ScoringParamSet): MatchQueryBuilder = {
    val msm = scoringParams.minShouldMatch.getOrElse("100%")
    matchQuery(TitleFieldType.autocompleteFieldName, queryString).minimumShouldMatch(msm)
  }

  // TODO: rename; most queries are composites and thus this isn't very descriptive and on top of
  // that, this is no longer a filteredQuery (which no longer exists in Elasticsearch 5.x)
  def compositeFilteredQuery(
      domainSet: DomainSet,
      searchParams: SearchParamSet,
      query: QueryBuilder,
      user: Option[AuthedUser])
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
          b.should(matchPhraseQuery(CategoriesFieldType.Name.lowercaseFieldName, q))
        },
        ScoreMode.Avg
      )
    }

  def tagsQuery(tags: Option[Set[String]]): Option[NestedQueryBuilder] =
    tags.map { tags =>
      nestedQuery(
        TagsFieldType.fieldName,
        tags.foldLeft(boolQuery().minimumShouldMatch(1)) { (b, q) =>
          b.should(matchPhraseQuery(TagsFieldType.Name.lowercaseFieldName, q))
        },
        ScoreMode.Avg
      )
    }

  def domainCategoriesQuery(categories: Option[Set[String]]): Option[BoolQueryBuilder] =
    categories.map { cs =>
      cs.foldLeft(boolQuery().minimumShouldMatch(1)) { (b, q) =>
        b.should(matchPhraseQuery(DomainCategoryFieldType.lowercaseFieldName, q))
      }
    }

  def domainTagsQuery(tags: Option[Set[String]]): Option[BoolQueryBuilder] =
    tags.map { ts =>
      ts.foldLeft(boolQuery().minimumShouldMatch(1)) { (b, q) =>
        b.should(matchPhraseQuery(DomainTagsFieldType.lowercaseFieldName, q))
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
