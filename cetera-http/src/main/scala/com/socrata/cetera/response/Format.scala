package com.socrata.cetera.response

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.codec.DecodeError
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.jpath.JPath
import org.elasticsearch.action.search.SearchResponse
import org.slf4j.LoggerFactory

import com.socrata.cetera.auth.AuthedUser
import com.socrata.cetera.handlers.FormatParamSet
import com.socrata.cetera.types._

// scalastyle:ignore number.of.methods
object Format {
  lazy val logger = LoggerFactory.getLogger(Format.getClass)
  val UrlSegmentLengthLimit = 50

  private def decodeErrorToNone(err: DecodeError) = None

  private def parseJToString(jValue: JValue) = jValue match {
    case JString(s) => Some(s)
    case _ => None
  }

  private def parseJToBoolean(jValue: JValue) = jValue match {
    case JBoolean(b) => Option(b)
    case _ => None
  }

  private def parseJToArray(jValue: JValue) = jValue match {
    case JArray(list) => Some(list.collect{ case JString(s) => s }.toList)
    case _ => None
  }

  private def extractJString(decoded: Either[DecodeError, JValue]): Option[String] =
    decoded.fold(decodeErrorToNone, parseJToString)

  private def extractJBoolean(decoded: Either[DecodeError, JValue]): Option[Boolean] =
    decoded.fold(decodeErrorToNone, parseJToBoolean)

  private def extractJArray(decoded: Either[DecodeError, JValue]): Option[List[String]] =
    decoded.fold(decodeErrorToNone, parseJToArray)

  def hyphenize(text: String): String = Option(text) match {
    case Some(s) if s.nonEmpty => s.replaceAll("[^\\p{L}\\p{N}_]+", "-").take(UrlSegmentLengthLimit)
    case _ => "-"
  }

  def links(
      cname: String,
      locale: Option[String],
      datatype: Option[Datatype],
      viewtype: Option[String],
      datasetId: String,
      datasetCategory: Option[String],
      datasetName: String,
      previewImageId: Option[String]): Map[String, JString] = {

    val cnameWithLocale = locale.foldLeft(cname){ (path, locale) => s"$path/$locale" }

    val perma = (datatype, viewtype) match {
      case (Some(StoryDatatype), _)             => s"stories/s"
      case (Some(DatalensDatatype), _)          => s"view"
      case (_, Some(DatalensDatatype.singular)) => s"view"
      case _                                  => s"d"
    }

    val pretty = datatype match {
      // TODO: maybe someday stories will allow pretty seo links
      // stories don't have a viewtype today, but who knows...
      case Some(StoryDatatype) => perma
      case _ =>
        val category = datasetCategory.filter(s => s.nonEmpty).getOrElse(DatasetDatatype.singular)
        s"${hyphenize(category)}/${hyphenize(datasetName)}"
    }

    val previewImageUrl = previewImageId.map(id => JString(s"https://$cname/views/$datasetId/files/$id"))
    if (previewImageId.isEmpty) logger.debug(s"Missing previewImageId field for document $datasetId")

    Map(
      "permalink" -> JString(s"https://$cnameWithLocale/$perma/$datasetId"),
      "link" -> JString(s"https://$cnameWithLocale/$pretty/$datasetId")
    ) ++ previewImageUrl.map(url => ("previewImageUrl", url))
  }

  // TODO: rammy to rename customer_blah to domain_blah
  def domainCategory(j: JValue): Option[JValue] = j.dyn.customer_category.? match {
    case Left(e) => None
    case Right(jv) => Some(jv)
  }

  def domainCategoryString(j: JValue): Option[String] =
    domainCategory(j).flatMap {
      case JString(s) => Option(s)
      case _ => None
    }

  def domainTags(j: JValue): Option[JValue] = j.dyn.customer_tags.? match {
    case Left(e) => None
    case Right(jv) => Some(jv)
  }

  def domainMetadata(j: JValue): Option[JValue] = j.dyn.customer_metadata_flattened.? match {
    case Left(e) => None
    case Right(jv) => Some(jv)
  }

  def domainPrivateMetadata(j: JValue, user: Option[AuthedUser], domainId: Int): Option[JValue] =
    user.flatMap { case u: AuthedUser =>
      val ownsIt = extractJString(j.dyn.owner_id.?).exists(_ == u.id)
      val sharesIt = extractJArray(j.dyn.shared_to.?).exists(_.contains(u.id))
      val hasEnablingRole = u.canViewPrivateMetadata(domainId)
      if (ownsIt || sharesIt || hasEnablingRole) {
        j.dyn.private_customer_metadata_flattened.? match {
          case Left(e) => None
          case Right(jv) => Some(jv)
        }
      } else {
        None
      }
    }

  def categories(j: JValue): Seq[JValue] =
    new JPath(j).down("animl_annotations").down("categories").*.down("name").finish.distinct.toList

  def tags(j: JValue): Seq[JValue] =
    new JPath(j).down("animl_annotations").down("tags").*.down("name").finish.distinct.toList

  def cname(domainCnames: Map[Int,String], j: JValue): String = {
    domainId(j).flatMap { i =>
      domainCnames.get(i)
    }.getOrElse("") // if no domain was found, default to blank string
  }

  def grants(j: JValue): Option[Seq[JValue]] = {
    new JPath(j).down("grants").finish.headOption.flatMap(_.cast[JArray]).map(_.toSeq)
  }

  def datatype(j: JValue): Option[Datatype] =
    extractJString(j.dyn.datatype.?).flatMap(s => Datatype(s))

  def viewtype(j: JValue): Option[String] = extractJString(j.dyn.viewtype.?)

  def datasetId(j: JValue): Option[String] = extractJString(j.dyn.socrata_id.dataset_id.?)

  def datasetName(j: JValue): Option[String] = extractJString(j.dyn.resource.name.?)

  def domainId(j: JValue): Option[Int] = j.dyn.socrata_id.domain_id.! match {
    case jn: JNumber => Option(jn.toInt)
    case JArray(elems) => elems.lastOption.map(_.asInstanceOf[JNumber].toInt)
    case jv: JValue => throw new NoSuchElementException(s"Unexpected json value $jv")
  }

  def previewImageId(j: JValue): Option[String] = extractJString(j.dyn.preview_image_id.?)

  def license(j: JValue): Option[String] = extractJString(j.dyn.license.?)

  def isPublic(j: JValue): Boolean = extractJBoolean(j.dyn.is_public.?).exists(identity)

  def isPublished(j: JValue): Boolean = extractJBoolean(j.dyn.is_published.?).exists(identity)

  def isHidden(j: JValue): Boolean = extractJBoolean(j.dyn.hide_from_catalog.?).exists(identity)

  private def literallyApproved(j: JValue): Boolean = {
    val status = extractJString(j.dyn.moderation_status.?)
    status.contains(ApprovalStatus.approved.status)
  }

  private def approvalsContainId(j: JValue, id: Int): Boolean = {
    j.dyn.approving_domain_ids.! match {
      case jn: JNumber => jn.toInt == id
      case JArray(elems) => elems.map(_.asInstanceOf[JNumber].toInt).contains(id)
      case _ => false
    }
  }

  val datalensTypeStrings = List(DatalensDatatype, DatalensChartDatatype, DatalensMapDatatype).map(_.singular)
  def datalensStatus(j: JValue): Option[String] = {
    val isDatalens = extractJString(j.dyn.datatype.?).exists(datalensTypeStrings.contains(_))
    if (isDatalens) extractJString(j.dyn.moderation_status.?) else None
  }

  def datalensApproved(j: JValue): Option[Boolean] = {
    val isDatalens = extractJString(j.dyn.datatype.?).exists(datalensTypeStrings.contains(_))
    if (isDatalens) Some(literallyApproved(j)) else None
  }

  def moderationStatus(j: JValue, viewsDomain: Domain): Option[String] = {
    val isDefault = extractJBoolean(j.dyn.is_default_view.?).exists(identity)
    viewsDomain.moderationEnabled match {
      case true => if (isDefault) Some(ApprovalStatus.approved.status) else extractJString(j.dyn.moderation_status.?)
      case false => None
    }
  }

  def moderationApproved(j: JValue, viewsDomain: Domain): Option[Boolean] = {
    val isDefault = extractJBoolean(j.dyn.is_default_view.?).exists(identity)
    viewsDomain.moderationEnabled match {
      case true => Some(isDefault || literallyApproved(j))
      case false => None
    }
  }

  def moderationApprovedByContext(j: JValue, viewsDomain: Domain, domainSet: DomainSet): Option[Boolean] = {
    val isDefault = extractJBoolean(j.dyn.is_default_view.?).exists(identity)
    (domainSet.contextIsModerated, viewsDomain.moderationEnabled) match {
      // if a view comes from a moderated domain, it must be a default view or approved
      case (true, true) => Some(isDefault || literallyApproved(j))
      // if a view comes from an unmoderated domain, it must be a default view
      case (true, false) => Some(isDefault)
      // if the context isn't moderated, a views moderationApproval is decided by its parent domain, not the context
      case (false, _) => None
    }
  }

  def routingStatus(j: JValue, viewsDomain: Domain): Option[String] =
    viewsDomain.routingApprovalEnabled match {
      case true =>
        if (extractJBoolean(j.dyn.is_approved_by_parent_domain.?).exists(identity)) {
          Some(ApprovalStatus.approved.status)
        } else if (extractJBoolean(j.dyn.is_pending_on_parent_domain.?).exists(identity)) {
          Some(ApprovalStatus.pending.status)
        } else if (extractJBoolean(j.dyn.is_rejected_by_parent_domain.?).exists(identity)) {
          Some(ApprovalStatus.rejected.status)
        } else {
          None
        }
      case false => None
    }

  def routingApproved(j: JValue, viewsDomain: Domain): Option[Boolean] = {
    val viewDomainId = viewsDomain.domainId
    viewsDomain.routingApprovalEnabled match {
      case true => Some(approvalsContainId(j, viewsDomain.domainId))
      case false => None
    }
  }

  def routingApprovedByContext(j: JValue, viewsDomain: Domain, domainSet: DomainSet): Option[Boolean] = {
    val contextDomainId = domainSet.searchContext.map(d => d.domainId).getOrElse(0)
    domainSet.contextHasRoutingApproval match {
      case true => Some(approvalsContainId(j, contextDomainId))
      case false => None
    }
  }

  def contextApprovals(j: JValue, viewsDomain: Domain, domainSet: DomainSet): (Option[Boolean],Option[Boolean]) = {
    val viewDomainId = viewsDomain.domainId
    val contextDomainId = domainSet.searchContext.map(d => d.domainId).getOrElse(0)
    val moderationApprovalOnContext = moderationApprovedByContext(j, viewsDomain, domainSet)
    val routingApprovalOnContext = routingApprovedByContext(j, viewsDomain, domainSet)
    if (viewDomainId != contextDomainId) (moderationApprovalOnContext, routingApprovalOnContext) else (None, None)
  }

  def calculateVisibility(j: JValue, viewsDomain: Domain, domainSet: DomainSet): Metadata = {
    val viewDomainId = viewsDomain.domainId
    val contextDomainId = domainSet.searchContext.map(d => d.domainId).getOrElse(0)
    val public = isPublic(j)
    val published = isPublished(j)
    val hidden = isHidden(j)
    val raStatus = routingStatus(j, viewsDomain)
    val routingApproval = routingApproved(j, viewsDomain)
    val modStatus = moderationStatus(j, viewsDomain)
    val moderationApproval = moderationApproved(j, viewsDomain)
    val dlStatus = datalensStatus(j)
    val datalensApproval = datalensApproved(j)
    val(moderationApprovalOnContext, routingApprovalOnContext) = contextApprovals(j, viewsDomain, domainSet)
    val viewGrants = grants(j)
    val anonymousVis =
      public & published & !hidden &
      routingApproval.getOrElse(true) & routingApprovalOnContext.getOrElse(true) &
      moderationApproval.getOrElse(true) & moderationApprovalOnContext.getOrElse(true) &
      datalensApproval.getOrElse(true)
    val viewLicense = license(j)

    Metadata(
      domain = viewsDomain.domainCname,
      license = viewLicense,
      isPublic = Some(public),
      isPublished = Some(published),
      isHidden = Some(hidden),
      isModerationApproved = moderationApproval,
      isModerationApprovedOnContext = moderationApprovalOnContext,
      isRoutingApproved = routingApproval,
      isRoutingApprovedOnContext = routingApprovalOnContext,
      isDatalensApproved = datalensApproval,
      visibleToAnonymous = Some(anonymousVis),
      moderationStatus = modStatus,
      routingStatus = raStatus,
      datalensStatus = dlStatus,
      grants = viewGrants)
  }

  def resultOwner(j: JValue): Option[UserInfo] =
    j.dyn.owner.?.fold(decodeErrorToNone, UserInfo.fromJValue)

  def documentSearchResult( // scalastyle:ignore method.length
      j: JValue,
      user: Option[AuthedUser],
      domainSet: DomainSet,
      locale: Option[String],
      score: Option[JNumber],
      showVisibility: Boolean,
      owner: Option[UserInfo])
    : Option[SearchResult] = {
    try {
      val viewsDomainId = domainId(j).getOrElse(throw new NoSuchElementException)
      val domainIdCnames = domainSet.idMap.map { case (i, d) => i -> d.domainCname }
      val viewsDomain = domainSet.idMap.getOrElse(viewsDomainId, throw new NoSuchElementException)
      val viewLicense = license(j)
      val scoreMap = score.map(s => s.toBigDecimal)
      val scorelessMetadata = if (showVisibility) {
        calculateVisibility(j, viewsDomain, domainSet)
      } else {
        Metadata(viewsDomain.domainCname, viewLicense)
      }

      val metadata = scorelessMetadata.copy(score = score.map(_.toBigDecimal))

      val linkMap = links(
        cname(domainIdCnames, j),
        locale,
        datatype(j),
        viewtype(j),
        datasetId(j).get,
        domainCategoryString(j),
        datasetName(j).get,
        previewImageId(j))

      Some(SearchResult(
        j.dyn.resource.!,
        Classification(
          categories(j),
          tags(j),
          domainCategory(j),
          domainTags(j),
          domainMetadata(j),
          domainPrivateMetadata(j, user, viewsDomainId)),
        metadata,
        linkMap.getOrElse("permalink", JString("")),
        linkMap.getOrElse("link", JString("")),
        linkMap.get("previewImageUrl"),
        owner)
      )
    }
    catch { case e: Exception =>
      logger.info(e.getMessage)
      None
    }
  }

  // WARN: This will raise if a single document has a single missing path!
  def formatDocumentResponse(
      searchResponse: SearchResponse,
      user: Option[AuthedUser],
      domainSet: DomainSet,
      formatParams: FormatParamSet)
    : SearchResults[SearchResult] = {
    val domainIdCnames = domainSet.idMap.map { case (i, d) => i -> d.domainCname }
    val hits = searchResponse.getHits

    val searchResult = hits.hits().flatMap { hit =>
      val json = JsonReader.fromString(hit.sourceAsString())
      val score = if (formatParams.showScore) Some(JNumber(hit.score)) else None
      val owner = resultOwner(json)

      documentSearchResult(json, user, domainSet, formatParams.locale, score, formatParams.showVisibility, owner)
    }
    SearchResults(searchResult, hits.getTotalHits)
  }
}
