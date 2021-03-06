package com.socrata.cetera.response

import com.rojoma.json.v3.util.JsonUtil
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.search.SearchHit
import org.slf4j.LoggerFactory

import com.socrata.cetera.auth.AuthedUser
import com.socrata.cetera.errors.JsonDecodeException
import com.socrata.cetera.handlers.FormatParamSet
import com.socrata.cetera.types._

// scalastyle:ignore number.of.methods
object Format {
  lazy val logger = LoggerFactory.getLogger(Format.getClass)
  val UrlSegmentLengthLimit = 50

  def hyphenize(text: String): String = Option(text) match {
    case Some(s) if s.nonEmpty => s.replaceAll("[^\\p{L}\\p{N}_]+", "-").take(UrlSegmentLengthLimit)
    case _ => "-"
  }

  def links(
      cname: String,
      locale: Option[String],
      datatype: Option[Datatype],
      viewtype: String,
      datasetId: String,
      datasetCategory: Option[String],
      datasetName: String,
      previewImageId: Option[String])
    : Map[String, String] = {

    val cnameWithLocale = locale.foldLeft(cname){ (path, locale) => s"$path/$locale" }

    val perma = (datatype, viewtype) match {
      case (Some(StoryDatatype), _)          => s"stories/s"
      case (Some(DatalensDatatype), _)       => s"view"
      case (_, DatalensDatatype.singular)    => s"view"
      case _                                 => s"d"
    }

    val pretty = datatype match {
      // TODO: maybe someday stories will allow pretty seo links
      // stories don't have a viewtype today, but who knows...
      case Some(StoryDatatype) => perma
      case _ =>
        val category = datasetCategory.filter(s => s.nonEmpty).getOrElse(DatasetDatatype.singular)
        s"${hyphenize(category)}/${hyphenize(datasetName)}"
    }

    val previewImageUrl = previewImageId.map(id => s"https://$cname/views/$datasetId/files/$id")
    if (previewImageId.isEmpty) logger.debug(s"Missing previewImageId field for document $datasetId")

    Map(
      "permalink" -> s"https://$cnameWithLocale/$perma/$datasetId",
      "link" -> s"https://$cnameWithLocale/$pretty/$datasetId"
    ) ++ previewImageUrl.map(url => ("previewImageUrl", url))
  }

  def domainPrivateMetadata(doc: Document, user: Option[AuthedUser]): Option[Seq[CustomerMetadataFlattened]] =
    user match {
      case Some(u) if (u.owns(doc) || u.shares(doc) ||
        u.canViewAllOfSomePrivateMetadata(doc.socrataId.domainId.toInt, doc.isStory)) =>
        Some(doc.privateCustomerMetadataFlattened)
      case _ => None
    }

  def approvals(doc: Document, user: Option[AuthedUser]): Option[Seq[FlattenedApproval]] =
    user match {
      case Some(u) if (u.owns(doc) || u.shares(doc) ||
        u.canViewAllOfSomeApprovalsNotes(doc.socrataId.domainId.toInt, doc.isStory)) =>
        doc.approvals.map(_.map(_.removeEmails))
      case _ =>
        doc.approvals.map(_.map(_.removeNotesAndEmails()))
    }

  private def literallyApproved(doc: Document): Boolean =
    doc.moderationStatus.contains(ApprovalStatus.approved.status)

  private def approvalsContainId(doc: Document, id: Int): Boolean =
    doc.approvingDomainIds.exists(_.contains(id))

  def moderationStatus(doc: Document, viewsDomain: Domain): Option[String] =
    if (viewsDomain.hasFontanaApprovals) {
      // we are pretending that fontana domains can never have VM
      None
    } else {
      viewsDomain.moderationEnabled match {
        case true => if (doc.isDefaultView) Some(ApprovalStatus.approved.status) else doc.moderationStatus
        case false => None
      }
    }

  def moderationApprovedByContext(doc: Document, viewsDomain: Domain, domainSet: DomainSet): Option[Boolean] =
    if (viewsDomain.hasFontanaApprovals) {
      // the context has no say in the approval of a view if it comes from a fontana domain
      None
    } else {
      (domainSet.contextIsModerated, viewsDomain.moderationEnabled) match {
        // if a view comes from a moderated domain, it must be a default view or approved
        case (true, true) => Some(doc.isDefaultView || literallyApproved(doc))
        // if a view comes from an unmoderated domain, it must be a default view
        case (true, false) => Some(doc.isDefaultView)
        // if the context isn't moderated, a views moderationApproval is decided by its parent domain, not the context
        case (false, _) => None
      }
    }

  def routingStatus(doc: Document, viewsDomain: Domain): Option[String] =
    if (viewsDomain.hasFontanaApprovals) {
      // we are pretending that fontana domains can never have R&A
      None
    } else {
      viewsDomain.routingApprovalEnabled match {
        case true =>
          if (doc.isApprovedByParentDomain) {
            Some(ApprovalStatus.approved.status)
          } else if (doc.isPendingOnParentDomain) {
            Some(ApprovalStatus.pending.status)
          } else if (doc.isRejectedByParentDomain) {
            Some(ApprovalStatus.rejected.status)
          } else {
            None
          }
        case false => None
      }
    }

  def routingApprovedByContext(doc: Document, viewsDomain: Domain, domainSet: DomainSet): Option[Boolean] =
    if (viewsDomain.hasFontanaApprovals) {
      // the context has no say in the approval of a view if it comes from a fontana domain
      None
    } else {
      val contextDomainId = domainSet.searchContext.map(d => d.id).getOrElse(0)
      domainSet.contextHasRoutingApproval match {
        case true => Some(approvalsContainId(doc, contextDomainId))
        case false => None
      }
    }

  def contextApprovals(doc: Document, viewsDomain: Domain, domainSet: DomainSet): (Option[Boolean],Option[Boolean]) = {
    val viewDomainId = viewsDomain.id
    val contextDomainId = domainSet.searchContext.map(d => d.id).getOrElse(0)
    val moderationApprovalOnContext = moderationApprovedByContext(doc, viewsDomain, domainSet)
    val routingApprovalOnContext = routingApprovedByContext(doc, viewsDomain, domainSet)
    if (viewDomainId != contextDomainId) (moderationApprovalOnContext, routingApprovalOnContext) else (None, None)
  }

  def calculateVisibility(
      doc: Document,
      viewsDomain: Domain,
      domainSet: DomainSet,
      user: Option[AuthedUser])
    : Metadata = {
    val vmStatus = moderationStatus(doc, viewsDomain)
    val raStatus = routingStatus(doc, viewsDomain)
    val moderationApproved = vmStatus.map(_ == ApprovalStatus.approved.status)
    val routingApproved = raStatus.map(_ == ApprovalStatus.approved.status)
    val fontanaApproved = if (!viewsDomain.hasFontanaApprovals) true else doc.isFontanaApproved
    val(moderationApprovalOnContext, routingApprovalOnContext) = contextApprovals(doc, viewsDomain, domainSet)
    val viewGrants = if (doc.grants.isEmpty) None else Some(doc.grants)
    val anonymousVis =
      doc.isPublic & doc.isPublished & !doc.isHiddenFromCatalog &
      routingApproved.getOrElse(true) & routingApprovalOnContext.getOrElse(true) &
      moderationApproved.getOrElse(true) & moderationApprovalOnContext.getOrElse(true) &
      fontanaApproved

    Metadata(
      domain = viewsDomain.cname,
      license = doc.license,
      isPublic = Some(doc.isPublic),
      isPublished = Some(doc.isPublished),
      isHidden = Some(doc.isHiddenFromCatalog),
      isModerationApproved = moderationApproved,
      isModerationApprovedOnContext = moderationApprovalOnContext,
      isRoutingApproved = routingApproved,
      isRoutingApprovedOnContext = routingApprovalOnContext,
      visibleToAnonymous = Some(anonymousVis),
      moderationStatus = moderationStatus(doc, viewsDomain),
      routingStatus = routingStatus(doc, viewsDomain),
      grants = viewGrants,
      approvals = approvals(doc, user)
    )
  }

  def documentSearchResult( // scalastyle:ignore method.length
      doc: Document,
      user: Option[AuthedUser],
      domainSet: DomainSet,
      locale: Option[String],
      score: Option[Float],
      showVisibility: Boolean)
    : Option[SearchResult] = {
    try {
      val viewsDomainId = doc.socrataId.domainId.toInt
      val domainIdCnames = domainSet.idMap.map { case (i, d) => i -> d.cname }
      val viewsDomain = domainSet.idMap.getOrElse(viewsDomainId, throw new NoSuchElementException)

      val viewLicense = doc.license
      val scorelessMetadata = if (showVisibility) {
        calculateVisibility(doc, viewsDomain, domainSet, user)
      } else {
        Metadata(viewsDomain.cname, viewLicense)
      }

      val metadata = scorelessMetadata.copy(score = score)

      val linkMap = links(
        domainIdCnames.getOrElse(doc.socrataId.domainId.toInt, ""),
        locale,
        Datatype(doc.datatype),
        doc.viewtype,
        doc.socrataId.datasetId,
        doc.customerCategory,
        doc.resource.name,
        doc.previewImageId)

      Some(SearchResult(
        doc.resource,
        Classification(
          doc.animlAnnotations.map(_.categoryNames).getOrElse(Seq.empty),
          doc.animlAnnotations.map(_.tagNames).getOrElse(Seq.empty),
          doc.customerCategory,
          doc.customerTags,
          doc.customerMetadataFlattened,
          domainPrivateMetadata(doc, user)),
        metadata,
        linkMap.getOrElse("permalink", ""),
        linkMap.getOrElse("link", ""),
        linkMap.get("previewImageUrl"),
        doc.owner)
      )
    }
    catch { case e: Exception =>
      logger.info(e.getMessage)
      None
    }
  }

  def parseHit(hit: SearchHit): Option[Document] = {
    val document = JsonUtil.parseJson[Document](hit.getSourceAsString)
    document match {
      case Right(d) => Some(d)
      case Left(e) =>
        logger.error(s"Cannot parse document because ${e.english}. Document is: ${hit.getSourceAsString}")
        None
    }
  }

  def formatDocumentResponse(
      searchResponse: SearchResponse,
      user: Option[AuthedUser],
      domainSet: DomainSet,
      formatParams: FormatParamSet)
    : SearchResults[SearchResult] = {
    val domainIdCnames = domainSet.idMap.map { case (i, d) => i -> d.cname }
    val hits = searchResponse.getHits

    val searchResult = hits.getHits().flatMap { hit =>
      parseHit(hit).flatMap { d =>
        val score = if (formatParams.showScore) Some(hit.getScore) else None
        documentSearchResult(d, user, domainSet, formatParams.locale, score, formatParams.showVisibility)
      }
    }

    SearchResults(searchResult, hits.getTotalHits)
  }
}
