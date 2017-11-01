package com.socrata.cetera.types

import com.rojoma.json.v3.util._
import org.slf4j.LoggerFactory

import com.socrata.cetera.errors.JsonDecodeException

@JsonKeyStrategy(Strategy.Underscore)
case class SocrataId(
    datasetId: String,
    nbeId: Option[String],
    obeId: Option[String],
    parentDatasetId: Option[Set[String]],
    domainId: BigInt)

object SocrataId {
  implicit val jCodec = AutomaticJsonCodecBuilder[SocrataId]
}

@JsonKeyStrategy(Strategy.Underscore)
case class PageViews(
    pageViewsTotal: Long,
    pageViewsLastMonth: Long,
    pageViewsLastWeek: Long,
    pageViewsTotalLog: Option[Float] = None,
    pageViewsLastMonthLog: Option[Float] = None,
    pageViewsLastWeekLog: Option[Float] = None)
object PageViews { implicit val jCodec = AutomaticJsonCodecBuilder[PageViews] }

@JsonKeyStrategy(Strategy.Underscore)
case class Resource(
    name: String,
    id: String,
    parentFxf: Option[Set[String]],
    description: String,
    attribution: Option[String],
    @JsonKey("type") datatype: String,
    @JsonKey("updatedAt") updatedAt: String,
    @JsonKey("createdAt") createdAt: String,
    pageViews: Option[PageViews],
    columnsName: Seq[String],
    columnsFieldName: Seq[String],
    columnsDatatype: Seq[String],
    columnsDescription: Seq[String],
    downloadCount: Option[BigInt],
    provenance: Option[String])
object Resource { implicit val jCodec = AutomaticJsonCodecBuilder[Resource] }

@JsonKeyStrategy(Strategy.Underscore)
@NullForNone
case class Annotation(name: String, score: BigDecimal)

object Annotation {
  implicit val jCodec = AutomaticJsonCodecBuilder[Annotation]
}

@JsonKeyStrategy(Strategy.Underscore)
case class AnimlAnnotations(categories: Seq[Annotation], tags: Seq[Annotation]) {
  def categoryNames: Seq[String] = categories.map(_.name).distinct
  def tagNames: Seq[String] = tags.map(_.name).distinct
}
object AnimlAnnotations { implicit val jCodec = AutomaticJsonCodecBuilder[AnimlAnnotations] }

@JsonKeyStrategy(Strategy.Underscore)
case class IndexedMetadata(
    name: String,
    description: String,
    columnsFieldName: Seq[String],
    columnsDescription: Seq[String],
    columnsName: Seq[String])

object IndexedMetadata {
  implicit val jCodec = AutomaticJsonCodecBuilder[IndexedMetadata]
}

@JsonKeyStrategy(Strategy.Underscore)
case class ESGrant(
    @JsonKey("type") grantType: String,
    userId: String)

object ESGrant {
  implicit val jCodec = AutomaticJsonCodecBuilder[ESGrant]
}

@JsonKeyStrategy(Strategy.Underscore)
case class CustomerMetadataFlattened(key: String, value: String)

object CustomerMetadataFlattened {
  implicit val jCodec = AutomaticJsonCodecBuilder[CustomerMetadataFlattened]
}

@JsonKeyStrategy(Strategy.Underscore)
@NullForNone
case class FlattenedApproval(
    outcome: String,
    state: String,
    submittedAt: String,
    submitterId: String,
    submitterName: String,
    submitterEmail: Option[String],
    reviewedAutomatically: Option[Boolean],
    reviewedAt: Option[String],
    reviewerId: Option[String],
    reviewerName: Option[String],
    reviewerEmail: Option[String]
) {

  def removeEmails(): FlattenedApproval = this.copy(submitterEmail = None, reviewerEmail = None)
}

object FlattenedApproval {
  implicit val jCodec = AutomaticJsonCodecBuilder[FlattenedApproval]
}

@JsonKeyStrategy(Strategy.Underscore)
case class Document(
    pageViews: Option[PageViews], // with Some for log variants
    datatype: String,
    viewtype: String,
    isDefaultView: Boolean,
    isPublic: Boolean,
    isPublished: Boolean,
    indexedMetadata: IndexedMetadata,
    resource: Resource,
    socrataId: SocrataId,
    animlAnnotations: Option[AnimlAnnotations],
    approvingDomainIds: Option[Seq[Int]],
    rejectingDomainIds: Option[Seq[Int]],
    pendingDomainIds: Option[Seq[Int]],
    isApprovedByParentDomain: Boolean,
    isRejectedByParentDomain: Boolean,
    isPendingOnParentDomain: Boolean,
    approvals: Option[Seq[FlattenedApproval]],
    customerTags: Seq[String],
    customerCategory: Option[String],
    // If this comes back empty, it comes back as an empty object, not an empty array
    customerMetadataFlattened: Seq[CustomerMetadataFlattened],
    privateCustomerMetadataFlattened: Seq[CustomerMetadataFlattened],
    createdAt: String,
    updatedAt: String,
    indexedAt: Option[String],
    owner: UserInfo,
    sharedTo: Seq[String],
    attribution: Option[String],
    provenance: Option[String],
    previewImageId: Option[String],
    grants: Seq[ESGrant],
    hideFromCatalog: Option[Boolean],
    hideFromDataJson: Option[Boolean],
    moderationStatus: Option[String],
    license: Option[String]) {

  def isSharedOrOwned(userId: String): Boolean = owner.id == userId || sharedTo.contains(userId)
  def isStory: Boolean = datatype == StoryDatatype.singular
  def isHiddenFromCatalog: Boolean = hideFromCatalog.getOrElse(false)

  def isVmApproved: Boolean = moderationStatus.exists(s => s == ApprovalStatus.approved.status) & !isDefaultView
  def isVmRejected: Boolean = moderationStatus.exists(s => s == ApprovalStatus.rejected.status) & !isDefaultView
  def isVmPending: Boolean = moderationStatus.exists(s => s == ApprovalStatus.pending.status) & !isDefaultView

  def isRaApproved(domainId: Int): Boolean = approvingDomainIds.getOrElse(Seq.empty).contains(domainId)
  def isRaRejected(domainId: Int): Boolean = rejectingDomainIds.getOrElse(Seq.empty).contains(domainId)
  def isRaPending(domainId: Int): Boolean = pendingDomainIds.getOrElse(Seq.empty).contains(domainId)
  def isInRaQueue(domainId: Int): Boolean = isRaApproved(domainId) || isRaRejected(domainId) || isRaPending(domainId)
}

object Document {
  implicit val jCodec = AutomaticJsonCodecBuilder[Document]
  val logger = LoggerFactory.getLogger(getClass)

  def apply(source: String): Option[Document] = {
    Option(source).flatMap { s =>
      JsonUtil.parseJson[Document](s) match {
        case Right(doc) => Some(doc)
        case Left(err) =>
          logger.error(err.english)
          throw new JsonDecodeException(err)
      }
    }
  }
}
