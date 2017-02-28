package com.socrata.cetera.search

import org.elasticsearch.index.query.FilterBuilder
import org.elasticsearch.index.query.FilterBuilders._

import com.socrata.cetera.auth.User
import com.socrata.cetera.errors.UnauthorizedError
import com.socrata.cetera.esDocumentType
import com.socrata.cetera.handlers.SearchParamSet
import com.socrata.cetera.types._

// the `forDomainSearch` param, if true, prepends "document." to every fieldName used in search.
// TODO: revisit choice to make this a case class. Is there a cleaner way to ensure that all
// filters have the appropriate prefix when called from the domainClient?
// scalastyle:ignore number.of.methods
case class DocumentFilters(forDomainSearch: Boolean = false) {

  val fieldNamePrefix = if (forDomainSearch) esDocumentType + "." else ""

  // ------------------------------------------------------------------------------------------
  // These are the building blocks of all other filters defined in this class.
  // In order to properly support search over domains (vs. documents), please ensure
  // that each of these filters include the `fieldNamePrefix`
  // ------------------------------------------------------------------------------------------
  def datatypeFilter(datatypes: Set[String]): FilterBuilder =
    termsFilter(fieldNamePrefix + DatatypeFieldType.fieldName, datatypes.toSeq: _*)

  def userFilter(user: String): FilterBuilder =
    termFilter(fieldNamePrefix + OwnerIdFieldType.fieldName, user)

  def sharedToFilter(user: String): FilterBuilder =
    termFilter(fieldNamePrefix + SharedToFieldType.rawFieldName, user)

  def attributionFilter(attribution: String): FilterBuilder =
    termFilter(fieldNamePrefix + AttributionFieldType.rawFieldName, attribution)

  def provenanceFilter(provenance: String): FilterBuilder =
    termFilter(fieldNamePrefix + ProvenanceFieldType.rawFieldName, provenance)

  def licenseFilter(license: String): FilterBuilder =
    termFilter(fieldNamePrefix + LicenseFieldType.rawFieldName, license)

  def parentDatasetFilter(parentDatasetId: String): FilterBuilder =
    termFilter(fieldNamePrefix + ParentDatasetIdFieldType.fieldName, parentDatasetId)

  def hiddenFromCatalogFilter(hidden: Boolean = true): FilterBuilder =
    termFilter(fieldNamePrefix + HideFromCatalogFieldType.fieldName, hidden)

  def idFilter(ids: Set[String]): FilterBuilder =
    termsFilter(fieldNamePrefix + IdFieldType.fieldName, ids.toSeq: _*)

  def domainIdFilter(domainIds: Set[Int]): FilterBuilder =
    termsFilter(fieldNamePrefix + SocrataIdDomainIdFieldType.fieldName, domainIds.toSeq: _*)

  def raStatusAccordingToParentDomainFilter(status: ApprovalStatus, hasStatus: Boolean = true): FilterBuilder =
    termFilter(fieldNamePrefix + status.raAccordingToParentField, hasStatus)

  def raStatusAccordingToContextFilter(status: ApprovalStatus, contextId: Int): FilterBuilder =
    termFilter(fieldNamePrefix + status.raQueueField, contextId)

  def modStatusFilter(status: ApprovalStatus): FilterBuilder =
    termFilter(fieldNamePrefix + ModerationStatusFieldType.fieldName, status.status)

  def defaultViewFilter(default: Boolean = true): FilterBuilder =
    termFilter(fieldNamePrefix + IsDefaultViewFieldType.fieldName, default)

  def publicFilter(public: Boolean = true): FilterBuilder =
    termFilter(fieldNamePrefix + IsPublicFieldType.fieldName, public)

  def publishedFilter(published: Boolean = true): FilterBuilder =
    termFilter(fieldNamePrefix + IsPublishedFieldType.fieldName, published)

  def customerCategoriesFilter(tags: Set[String]): FilterBuilder =
    termsFilter(fieldNamePrefix + DomainCategoryFieldType.rawFieldName, tags.toSeq: _*)

  def customerTagsFilter(tags: Set[String]): FilterBuilder =
    termsFilter(fieldNamePrefix + DomainTagsFieldType.rawFieldName, tags.toSeq: _*)
  // ------------------------------------------------------------------------------------------

  // this filter limits documents to those owned/shared to the user
  // or visible to user based on their domain and role
  def privateMetadataUserRestrictionsFilter(user: User): FilterBuilder = {
    val userId = user.id
    val domainId = user.authenticatingDomain.map(_.domainId).getOrElse(0)
    val ownIt = userFilter(userId)
    val shareIt = sharedToFilter(userId)
    if (user.canViewPrivateMetadata(domainId)) {
      val beEnabledToSeeIt = domainIdFilter(Set(domainId))
      boolFilter().should(ownIt).should(shareIt).should(beEnabledToSeeIt)
    } else {
      boolFilter().should(ownIt).should(shareIt)
    }
  }

  // this filter limits documents to those with metadata keys/values that
  // match the given metadata. If public is true, this matches against the public metadata fields,
  // otherwise, this matches against the private metadata fields.
  // TODO:  should we score metadata by making this a query?
  // and if you do, remember to make the key and value both phrase matches
  def metadataFilter(metadata: Set[(String, String)], public: Boolean): Option[FilterBuilder] =
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
        vs.foldLeft(boolFilter()) { (b, v) =>
          b.should(
            nestedFilter(
              fieldNamePrefix + parentField,
              boolFilter()
                .must(termsFilter(keyField, k))
                .must(termsFilter(valueField, v))
            )
          )
        }
      }

      if (metadataGroupedByKey.size == 1) {
        unionWithinKeys.headOption // no need to create an intersection for 1 key
      } else {
        val intersectAcrossKeys = unionWithinKeys.foldLeft(boolFilter()) { (b, q) => b.must(q) }
        Some(intersectAcrossKeys)
      }
    } else {
      None // no filter to build from the empty set
    }

  // this filter limits documents to those that both:
  //  - have private metadata keys/values matching the given metadata
  //  - have private metadata visible to the given user
  def privateMetadataFilter(metadata: Set[(String, String)], user: Option[User]): Option[FilterBuilder] =
    if (metadata.nonEmpty) {
      user match {
        // if we haven't a user, don't build a filter to search private metadata
        case None => None
        // if the user is a superadmin, use a metadata filter pointed at the private fields
        case Some(u) if u.isSuperAdmin => metadataFilter(metadata, public = false)
        case Some(u) =>
          u.authenticatingDomain match {
            // if the user hasn't an authenticating domain, don't build a filter to search private metadata
            case None => None
            // if the user has an authenticating domain, use a conditional filter
            case Some(d) =>
              val beAbleToViewPrivateMetadata = privateMetadataUserRestrictionsFilter(u)
              val dmt = metadataFilter(metadata, public = false)
              dmt.map(matchMetadataTerms =>
                boolFilter().must(beAbleToViewPrivateMetadata).must(matchMetadataTerms))
          }
      }
    } else {
      None // no filter to build from the empty set
    }

  // this filter limits documents to those that either
  // - have public key/value pairs that match the given metadata
  // - have private key/value pairs that match the given metadata and have private metadata visible to the given user
  def combinedMetadataFilter(
      metadata: Set[(String, String)],
      user: Option[User]): Option[FilterBuilder] = {
    val pubFilter = metadataFilter(metadata, public = true)
    val privateFilter = privateMetadataFilter(metadata, user)
    (pubFilter, privateFilter) match {
      case (None, None) => None
      case (Some(matchPublicMetadata), None) => Some(matchPublicMetadata)
      case (None, Some(matchPrivateMetadata)) => Some(matchPrivateMetadata)
      case (Some(matchPublicMetadata), Some(matchPrivateMetadata)) =>
        Some(boolFilter().should(matchPublicMetadata).should(matchPrivateMetadata))
    }
  }


  // this filter, if the context is moderated, will limit results to
  // views from moderated domains + default views from unmoderated sites
  def vmSearchContextFilter(domainSet: DomainSet): Option[FilterBuilder] =
    domainSet.searchContext.collect { case c: Domain if c.moderationEnabled =>
      val beDefault = defaultViewFilter(default = true)
      val beFromModeratedDomain = domainIdFilter(domainSet.moderationEnabledIds)
      boolFilter().should(beDefault).should(beFromModeratedDomain)
    }

  // this filter, if the context has RA enabled, will limit results to only those
  // having been through or are presently in the context's RA queue
  def raSearchContextFilter(domainSet: DomainSet): Option[FilterBuilder] =
    domainSet.searchContext.collect { case c: Domain if c.routingApprovalEnabled =>
      val beApprovedByContext = raStatusAccordingToContextFilter(ApprovalStatus.approved, c.domainId)
      val beRejectedByContext = raStatusAccordingToContextFilter(ApprovalStatus.rejected, c.domainId)
      val bePendingWithinContextsQueue = raStatusAccordingToContextFilter(ApprovalStatus.pending, c.domainId)
      boolFilter().should(beApprovedByContext).should(beRejectedByContext).should(bePendingWithinContextsQueue)
    }

  // this filter limits results based on the processes in place on the search context
  //  - if view moderation is enabled, all derived views from unmoderated federated domains are removed
  //  - if R&A is enabled, all views whose self or parent has not been through the context's RA queue are removed
  def searchContextFilter(domainSet: DomainSet): Option[FilterBuilder] = {
    val vmFilter = vmSearchContextFilter(domainSet)
    val raFilter = raSearchContextFilter(domainSet)
    List(vmFilter, raFilter).flatten match {
      case Nil => None
      case filters: Seq[FilterBuilder] => Some(filters.foldLeft(boolFilter()) { (b, f) => b.must(f) })
    }
  }

  // this filter limits results down to views that are both
  //  - from the set of requested domains
  //  - should be included according to the search context and our funky federation rules
  def domainSetFilter(domainSet: DomainSet): FilterBuilder = {
    val domainsFilter = domainIdFilter(domainSet.domains.map(_.domainId))
    val contextFilter = searchContextFilter(domainSet)
    val filters = List(domainsFilter) ++ contextFilter
    filters.foldLeft(boolFilter()) { (b, f) => b.must(f) }
  }

  // this filter limits results to those with the given status
  def moderationStatusFilter(status: ApprovalStatus, domainSet: DomainSet)
  : FilterBuilder = {
    status match {
      case ApprovalStatus.approved =>
        // to be approved a view must either be
        //   * a default/approved view from a moderated domain
        //   * any view from an unmoderated domain (as they are approved by default)
        // NOTE: funkiness with federation is handled by the searchContext filter.
        val beDefault = defaultViewFilter()
        val beApproved = modStatusFilter(ApprovalStatus.approved)
        val beFromUnmoderatedDomain = domainIdFilter(domainSet.moderationDisabledIds)
        boolFilter().should(beDefault).should(beApproved).should(beFromUnmoderatedDomain)
      case _ =>
        // to be rejected/pending, a view must be a derived view from a moderated domain with the given status
        val beFromModeratedDomain = domainIdFilter(domainSet.moderationEnabledIds)
        val beDerived = defaultViewFilter(false)
        val haveGivenStatus = modStatusFilter(status)
        boolFilter().must(beFromModeratedDomain).must(beDerived).must(haveGivenStatus)
    }
  }

  // this filter limits results to datalens with the given status
  def datalensStatusFilter(status: ApprovalStatus): FilterBuilder = {
    val beADatalens = datatypeFilter(DatalensDatatype.allVarieties)
    status match {
      case ApprovalStatus.approved =>
        // limit results to those that are not unapproved datalens
        val beUnapproved = notFilter(modStatusFilter(ApprovalStatus.approved))
        notFilter(boolFilter().must(beADatalens).must(beUnapproved))
      case _ =>
        // limit results to those with the given status
        val haveGivenStatus = modStatusFilter(status)
        boolFilter().must(beADatalens).must(haveGivenStatus)
    }
  }

  // this filter limits results to those with the given R&A status on their parent domain
  def raStatusFilter(status: ApprovalStatus, domainSet: DomainSet): FilterBuilder = {
    status match {
      case ApprovalStatus.approved =>
        val beFromRADisabledDomain = domainIdFilter(domainSet.raDisabledIds)
        val haveGivenStatus = raStatusAccordingToParentDomainFilter(ApprovalStatus.approved)
        boolFilter()
          .should(beFromRADisabledDomain)
          .should(haveGivenStatus)
      case _ =>
        val beFromRAEnabledDomain = domainIdFilter(domainSet.raEnabledIds)
        val haveGivenStatus = raStatusAccordingToParentDomainFilter(status)
        boolFilter()
          .must(beFromRAEnabledDomain)
          .must(haveGivenStatus)
    }
  }

  // this filter limits results to those with the given R&A status on a given context
  def raStatusOnContextFilter(status: ApprovalStatus, domainSet: DomainSet): Option[FilterBuilder] =
    domainSet.searchContext.collect {
      case c: Domain if c.routingApprovalEnabled => raStatusAccordingToContextFilter(status, c.domainId)
    }

  // this filter limits results to those with a given approval status
  // - approved views must pass all 4 processes:
  //   1. viewModeration (if enabled on the domain)
  //   2. datalens moderation (if a datalens)
  //   3. R&A on the parent domain (if enabled on the parent domain)
  //   4. R&A on the context (if enabled on the context and you choose to include contextApproval)
  // - rejected/pending views need be rejected/pending by 1 or more processes
  def approvalStatusFilter(
      status: ApprovalStatus,
      domainSet: DomainSet,
      includeContextApproval: Boolean = true)
  : FilterBuilder = {
    val haveGivenVMStatus = moderationStatusFilter(status, domainSet)
    val haveGivenDLStatus = datalensStatusFilter(status)
    val haveGivenRAStatus = raStatusFilter(status, domainSet)
    val haveGivenRAStatusOnContext = raStatusOnContextFilter(status, domainSet)

    status match {
      case ApprovalStatus.approved =>
        // if we are looking for approvals, a view must be approved according to all 3 or 4 processes
        val filter = boolFilter().must(haveGivenVMStatus).must(haveGivenDLStatus).must(haveGivenRAStatus)
        if (includeContextApproval) haveGivenRAStatusOnContext.foreach(filter.must)
        filter
      case _ =>
        // otherwise, a view can fail due to any of the 3 or 4 processes
        val filter = boolFilter().should(haveGivenVMStatus).should(haveGivenDLStatus).should(haveGivenRAStatus)
        if (includeContextApproval) haveGivenRAStatusOnContext.foreach(filter.should)
        filter
    }
  }

  // this filter limits results down to the given socrata categories (this assumes no search context is present)
  def socrataCategoriesFilter(categories: Set[String]): FilterBuilder = {
    boolFilter().should(
      nestedFilter(
        fieldNamePrefix + CategoriesFieldType.fieldName,
        termsFilter(CategoriesFieldType.Name.rawFieldName, categories.toSeq: _*)
      )
    )
  }

  // this filter limits results down to the given socrata tags (this assumes no search context is present)
  def socrataTagsFilter(tags: Set[String]): FilterBuilder = {
    boolFilter().should(
      nestedFilter(
        fieldNamePrefix + TagsFieldType.fieldName,
        termsFilter(TagsFieldType.Name.rawFieldName, tags.toSeq: _*)
      )
    )
  }

  // this filter limits results down to the (socrata or customer) categories (depending on search context)
  def categoryFilter(categories: Set[String], context: Option[Domain]): Option[FilterBuilder] = {
    if (forDomainSearch) {
      if (context.isDefined) Some(customerCategoriesFilter(categories)) else Some(socrataCategoriesFilter(categories))
    } else {
      None // when searching across documents, we prefer a match query over filters
    }
  }

  // this filter limits results down to the (socrata or customer) tags (depending on search context)
  def tagFilter(tags: Set[String], context: Option[Domain]): Option[FilterBuilder] = {
    if (forDomainSearch) {
      if (context.isDefined) Some(customerTagsFilter(tags)) else Some(socrataTagsFilter(tags))
    } else {
      None // when searching across documents, we prefer a match query over filters
    }
  }

  // this filter limits results down to those requested by various search params
  def searchParamsFilter(searchParams: SearchParamSet, user: Option[User], domainSet: DomainSet)
  : Option[FilterBuilder] = {
    val haveContext = domainSet.searchContext.isDefined
    val typeFilter = searchParams.datatypes.map(datatypeFilter(_))
    val ownerFilter = searchParams.user.map(userFilter(_))
    val sharingFilter = (user, searchParams.sharedTo) match {
      case (_, None) => None
      case (Some(u), Some(uid)) if (u.id == uid) => Some(sharedToFilter(uid))
      case (_, _) => throw UnauthorizedError(user, "search another user's shared files")
    }
    val attrFilter = searchParams.attribution.map(attributionFilter(_))
    val provFilter = searchParams.provenance.map(provenanceFilter(_))
    val parentIdFilter = searchParams.parentDatasetId.map(parentDatasetFilter(_))
    val idsFilter = searchParams.ids.map(idFilter(_))
    val metaFilter = searchParams.domainMetadata.flatMap(combinedMetadataFilter(_, user))
    val derivationFilter = searchParams.derived.map(d => defaultViewFilter(!d))
    val licensesFilter = searchParams.license.map(licenseFilter(_))
    // we want to honor categories and tags in domain search only, and this can only be done via filters.
    // for document search however, we score these in queries
    val catFilter = searchParams.categories.flatMap(c => categoryFilter(c, domainSet.searchContext))
    val tagsFilter = searchParams.tags.flatMap(c => tagFilter(c, domainSet.searchContext))

    // the params below are those that would also influence visibility. these can only serve to further
    // limit the set of views returned from what the visibilityFilters allow.
    val privacyFilter = searchParams.public.map(publicFilter(_))
    val publicationFilter = searchParams.published.map(publishedFilter(_))
    val hiddenFilter = searchParams.explicitlyHidden.map(hiddenFromCatalogFilter(_))
    val approvalFilter = searchParams.approvalStatus.map(approvalStatusFilter(_, domainSet))

    List(typeFilter, ownerFilter, sharingFilter, attrFilter, provFilter, parentIdFilter, idsFilter, metaFilter,
      derivationFilter, privacyFilter, publicationFilter, hiddenFilter, approvalFilter, licensesFilter,
      catFilter, tagsFilter).flatten match {
      case Nil => None
      case filters: Seq[FilterBuilder] => Some(filters.foldLeft(boolFilter()) { (b, f) => b.must(f) })
    }
  }

  // this filter limits results to those that would show at /browse , i.e. public/published/approved/unhidden
  // optionally acknowledging the context, which sometimes should matter and sometimes should not
  def anonymousFilter(domainSet: DomainSet, includeContextApproval: Boolean = true)
  : FilterBuilder = {
    val bePublic = publicFilter(public = true)
    val bePublished = publishedFilter(published = true)
    val beApproved = approvalStatusFilter(ApprovalStatus.approved, domainSet, includeContextApproval)
    val beUnhidden = hiddenFromCatalogFilter(hidden = false)

    boolFilter()
      .must(bePublic)
      .must(bePublished)
      .must(beApproved)
      .must(beUnhidden)
  }

  // this filter will limit results down to those owned or shared to a user
  def ownedOrSharedFilter(user: User): FilterBuilder = {
    val userId = user.id
    val ownerFilter = userFilter(userId)
    val sharedFilter = sharedToFilter(userId)
    boolFilter().should(ownerFilter).should(sharedFilter)
  }

  // this filter will limit results to only those the user is allowed to see
  def authFilter(user: Option[User], domainSet: DomainSet, requireAuth: Boolean)
  : Option[FilterBuilder] = {
    (requireAuth, user) match {
      // if user is super admin, no vis filter needed
      case (true, Some(u)) if (u.isSuperAdmin) => None
      // if the user can view everything, they can only view everything on *their* domain
      // plus, of course, things they own/share and public/published/approved views from other domains
      case (true, Some(u)) if (u.authenticatingDomain.exists(d => u.canViewAllViews(d.domainId))) => {
        val personFilter = ownedOrSharedFilter(u)
        // re: includeContextApproval = false, in order for admins/etc to see views they've rejected from other domains,
        // we must allow them access to views that are approved on the parent domain, but not on the context.
        // yes, this is a snow-flaky case and it involves our auth. I am sorry  :(
        val anonFilter = anonymousFilter(domainSet, includeContextApproval = false)
        val fromUsersDomain = u.authenticatingDomain.map(d => domainIdFilter(Set(d.domainId)))
        val filter = boolFilter().should(personFilter).should(anonFilter)
        fromUsersDomain.foreach(filter.should(_))
        Some(filter)
      }
      // if the user isn't a superadmin nor can they view everything, they may only see
      // things they own/share and public/published/approved views
      case (true, Some(u)) => {
        val personFilter = ownedOrSharedFilter(u)
        val anonFilter = anonymousFilter(domainSet)
        Some(boolFilter().should(personFilter).should(anonFilter))
      }
      // if the user is hitting the internal endpoint without auth, we throw
      case (true, None) => throw UnauthorizedError(user, "search the internal catalog")
      // if auth isn't required, the user can only see public/published/approved views
      case (false, _) => Some(anonymousFilter(domainSet))
    }
  }

  def compositeFilter(
      domainSet: DomainSet,
      searchParams: SearchParamSet,
      user: Option[User],
      requireAuth: Boolean)
  : FilterBuilder = {
    val domainFilters = Some(domainSetFilter(domainSet))
    val authFilters = authFilter(user, domainSet, requireAuth)
    val searchFilters = searchParamsFilter(searchParams, user, domainSet)

    val allFilters = List(domainFilters, authFilters, searchFilters).flatten
    allFilters.foldLeft(boolFilter()) { (b, f) => b.must(f) }
  }
}
