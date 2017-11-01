package com.socrata.cetera.types

import scala.collection.JavaConverters._

import org.elasticsearch.search.SearchHit


// scalastyle:ignore number.of.types

/////////////////////////////////
// Sealed Traits for Cetera Types

// A field in a document in ES
sealed trait CeteraFieldType {
  val fieldName: String
}

sealed trait DocumentFieldType extends CeteraFieldType
sealed trait DomainFieldType extends CeteraFieldType
sealed trait UserFieldType extends CeteraFieldType

// A field that can be boosted (i.e, fields like title or description that can
// have extra weight given to then when matching keyword queries).
sealed trait Boostable extends CeteraFieldType

// A field that we allow to be be counted (e.g., how many documents of a given tag do we have?)
sealed trait Countable extends CeteraFieldType

// A field that we allow to be sorted on
sealed trait Sortable extends CeteraFieldType

// Used for nested fields like arrays or json blobs
sealed trait NestedField extends CeteraFieldType {
  protected val path: String
  protected lazy val keyName: String = this
    .getClass
    .getName
    .toLowerCase
    .split("\\$")
    .lastOption
    .getOrElse(throw new NoSuchElementException)

  val fieldName: String = s"$path.$keyName"
}

// These fields have a numeric score associated with them. For example, a
// document's category or tag might have a score of 0.95 to indicate that we
// are fairly confident about said category or tag. This score might be used as
// a default sort order when filtering.
sealed trait Scorable extends CeteraFieldType with Countable {
  val Name: NestedField
  val Score: NestedField
}

// For fields that have an additional "raw" version stored in ES
sealed trait Rawable extends CeteraFieldType {
  lazy val rawFieldName: String = fieldName + ".raw"
}

// For fields that are natively "raw", i.e. "not_analyzed"
sealed trait NativelyRawable extends Rawable {
  override lazy val rawFieldName: String = fieldName
}

// For fields with a corresponding autocomplete field
sealed trait Autocompletable extends CeteraFieldType {
  lazy val autocompleteFieldName: String = fieldName + ".autocomplete"
}

// For key-value things like custom metadata fields
sealed trait Mapable extends CeteraFieldType {
  val Key: NestedField
  val Value: NestedField
}

// For fields that we want to search the lowercase versions of
sealed trait Lowercasable extends CeteraFieldType {
  lazy val lowercaseFieldName: String = fieldName + ".lowercase"
}

// For sortable text fields where we want to ignore non-alphanumeric characters
sealed trait HasLowercaseAlphanumericSubfield extends CeteraFieldType {
  lazy val lowercaseAlphanumFieldName: String = fieldName + ".lowercase_alphanumeric"
}

///////////////////
// Full Text Search
case object FullTextSearchAnalyzedFieldType extends CeteraFieldType {
  val fieldName: String = "fts_analyzed"
}
case object FullTextSearchRawFieldType extends CeteraFieldType {
  val fieldName: String = "fts_raw"
}
case object PrivateFullTextSearchAnalyzedFieldType extends CeteraFieldType {
  val fieldName: String = "private_fts_analyzed"
}
case object PrivateFullTextSearchRawFieldType extends CeteraFieldType {
  val fieldName: String = "private_fts_raw"
}
case object DomainCnameFieldType extends DomainFieldType with Countable with Rawable {
  val fieldName: String = "domain_cname"
}
case object CnamesFieldType extends DomainFieldType with Countable with Rawable {
  val fieldName: String = "cnames"
}

////////////////////
// Categories & Tags

// Categories have a score, a raw field name, and a lower-cased field name available
case object CategoriesFieldType extends DocumentFieldType with Scorable with Rawable with Lowercasable {
  val fieldName: String = "animl_annotations.categories"

  case object Name extends NestedField with Rawable with Lowercasable {
    protected lazy val path: String = CategoriesFieldType.fieldName
  }

  case object Score extends NestedField with Rawable with Lowercasable {
    protected lazy val path: String = CategoriesFieldType.fieldName
  }
}

// Tags have a score, a raw field name, and a lower-cased field name available
case object TagsFieldType extends DocumentFieldType with Scorable with Rawable with Lowercasable {
  val fieldName: String = "animl_annotations.tags"

  case object Name extends NestedField with Rawable with Lowercasable {
    protected lazy val path: String = TagsFieldType.fieldName
  }

  case object Score extends NestedField with Rawable with Lowercasable {
    protected lazy val path: String = TagsFieldType.fieldName
  }
}

case object OwnerIdFieldType extends DocumentFieldType with Countable with NativelyRawable {
  val fieldName: String = "owner.id"
}

case object OwnerDisplayNameFieldType extends DocumentFieldType with Sortable {
  val fieldName: String = "owner.display_name"
}

case object SubmitterNameFieldType extends DocumentFieldType with Sortable {
  val fieldName: String = "approvals.submitter_name"
}

case object ReviewerNameFieldType extends DocumentFieldType with Sortable {
  val fieldName: String = "approvals.reviewer_name"
}

case object SharedToFieldType extends DocumentFieldType with NativelyRawable {
  val fieldName: String = "shared_to"
}

case object AttributionFieldType extends DocumentFieldType with Countable with Rawable {
  val fieldName: String = "attribution"
}

case object LicenseFieldType extends DocumentFieldType with Countable with NativelyRawable {
  val fieldName: String = "license"
}

/////////////////////
// Catalog Visibility

case object IsCustomerDomainFieldType extends DomainFieldType {
  val fieldName: String = "is_customer_domain"
}

case object IsModerationEnabledFieldType extends DomainFieldType {
  val fieldName: String = "moderation_enabled"
}

case object IsRoutingApprovalEnabledFieldType extends DocumentFieldType {
  val fieldName: String = "routing_approval_enabled"
}

case object IsDefaultViewFieldType extends DocumentFieldType {
  val fieldName: String = "is_default_view"
}

// TODO: deprecate. we're using the one below this.
case object IsModerationApprovedFieldType extends DocumentFieldType {
  val fieldName: String = "is_moderation_approved"
}

case object ModerationStatusFieldType extends DocumentFieldType {
  val fieldName: String = "moderation_status"
}

case object ApprovedByParentFieldType extends DocumentFieldType {
  val fieldName: String = "is_approved_by_parent_domain"
}

case object RejectedByParentFieldType extends DocumentFieldType {
  val fieldName: String = "is_rejected_by_parent_domain"
}

case object PendingOnParentFieldType extends DocumentFieldType {
  val fieldName: String = "is_pending_on_parent_domain"
}

case object ApprovingDomainIdsFieldType extends DocumentFieldType {
  val fieldName: String = "approving_domain_ids"
}

case object RejectingDomainIdsFieldType extends DocumentFieldType {
  val fieldName: String = "rejecting_domain_ids"
}

case object PendingDomainIdsFieldType extends DocumentFieldType {
  val fieldName: String = "pending_domain_ids"
}

case object SocrataIdDatasetIdFieldType extends DocumentFieldType {
  val fieldName: String = "socrata_id.dataset_id"
}

case object SocrataIdDomainIdFieldType extends DocumentFieldType {
  val fieldName: String = "socrata_id.domain_id"
}

case object SocrataIdObeIdFieldType extends DocumentFieldType {
  val fieldName: String = "socrata_id.obe_id"
}

case object SocrataIdNbeFieldType extends DocumentFieldType {
  val fieldName: String = "socrata_id.nbe_id"
}

case object ParentDatasetIdFieldType extends DocumentFieldType {
  val fieldName: String = "socrata_id.parent_dataset_id"
}

case object IsPublicFieldType extends DocumentFieldType {
  val fieldName: String = "is_public"
}

case object IsPublishedFieldType extends DocumentFieldType {
  val fieldName: String = "is_published"
}

case object HideFromCatalogFieldType extends DocumentFieldType {
  val fieldName: String = "hide_from_catalog"
}

case object HideFromDataJsonFieldType extends DocumentFieldType {
  val fieldName: String = "hide_from_data_json"
}


/////////////////////////////////////////////////
// Domain-specific Categories, Tags, and Metadata

// Domain tags are customer-defined tags (which surface as topics in the front end).
case object DomainTagsFieldType extends DocumentFieldType with Countable with Rawable with Lowercasable {
  val fieldName: String = "customer_tags"
}

// A domain category is a domain-specific (customer-specified) category (as
// opposed to a Socrata-specific canonical category).
case object DomainCategoryFieldType
  extends DocumentFieldType
  with Countable
  with Rawable
  with Lowercasable
  with HasLowercaseAlphanumericSubfield {

  val fieldName: String = "customer_category"
}

// domain_metadata_flattened Domain metadata allows customers to define their
// own facets and call them whatever they like, for example you could define
// Superheroes and select from a list of them. We support these fields as
// direct url params (e.g., Superheros=Batman).
case object DomainMetadataFieldType extends DocumentFieldType with Mapable with Rawable {
  val fieldName: String = "customer_metadata_flattened"

  case object Key extends NestedField with Rawable {
    protected lazy val path: String = DomainMetadataFieldType.fieldName
  }

  case object Value extends NestedField with Rawable {
    protected lazy val path: String = DomainMetadataFieldType.fieldName
  }

  def fieldSet: (String, String, String) = (fieldName, Key.rawFieldName, Value.rawFieldName)
}

case object DomainPrivateMetadataFieldType extends DocumentFieldType with Mapable with Rawable {
  val fieldName: String = "private_customer_metadata_flattened"

  case object Key extends NestedField with Rawable {
    protected lazy val path: String = DomainPrivateMetadataFieldType.fieldName
  }

  case object Value extends NestedField with Rawable {
    protected lazy val path: String = DomainPrivateMetadataFieldType.fieldName
  }

  def fieldSet: (String, String, String) = (fieldName, Key.rawFieldName, Value.rawFieldName)
}

/////////////
// Boostables

case object TitleFieldType
  extends DocumentFieldType
  with Boostable
  with Rawable
  with Autocompletable
  with HasLowercaseAlphanumericSubfield {

  val fieldName: String = "indexed_metadata.name"

  def fromSearchHit(searchHit: SearchHit): String = {
    val source = searchHit.getSource()
    val indexedMetadata = source.get("indexed_metadata").asInstanceOf[java.util.Map[String, Object]].asScala
    indexedMetadata.get("name").collect {
      case name: String => name
    }.get
  }
}

case object DescriptionFieldType extends DocumentFieldType with Boostable {
  val fieldName: String = "indexed_metadata.description"
}

case object ColumnNameFieldType extends DocumentFieldType with Lowercasable with Boostable {
  val fieldName: String = "indexed_metadata.columns_name"
}

case object ColumnDescriptionFieldType extends DocumentFieldType with Boostable {
  val fieldName: String = "indexed_metadata.columns_description"
}

case object ColumnFieldNameFieldType extends DocumentFieldType with Boostable {
  val fieldName: String = "indexed_metadata.columns_field_name"
}

case object DatatypeFieldType extends DocumentFieldType with Boostable {
  val fieldName: String = "datatype"
}

case object ProvenanceFieldType extends DocumentFieldType with Countable with NativelyRawable with Boostable {
  val fieldName: String = "provenance"
}


//////////////
// For sorting

case object PageViewsTotalFieldType extends DocumentFieldType with Sortable {
  val fieldName: String = "page_views.page_views_total"
}

case object PageViewsLastMonthFieldType extends DocumentFieldType with Sortable {
  val fieldName: String = "page_views.page_views_last_month"
}

case object PageViewsLastWeekFieldType extends DocumentFieldType with Sortable {
  val fieldName: String = "page_views.page_views_last_week"
}

case object UpdatedAtFieldType extends DocumentFieldType with Sortable {
  val fieldName: String = "updated_at"
}

case object CreatedAtFieldType extends DocumentFieldType with Sortable {
  val fieldName: String = "created_at"
}

case object SubmittedAtFieldType extends DocumentFieldType with Sortable {
  val fieldName: String = "approvals.submitted_at"
}

case object ReviewedAtFieldType extends DocumentFieldType with Sortable {
  val fieldName: String = "approvals.reviewed_at"
}

////////////////
// U'sarians

case object UserScreenName extends UserFieldType
  with Rawable
  with HasLowercaseAlphanumericSubfield
  with Autocompletable {

  val fieldName: String = "screen_name"
}

case object UserEmail extends UserFieldType
  with Rawable
  with HasLowercaseAlphanumericSubfield
  with Autocompletable {

  val fieldName: String = "email"
}

case object UserFlag extends UserFieldType {
  val fieldName: String = "flags"
}

case object UserRoleName extends UserFieldType with HasLowercaseAlphanumericSubfield {
  val fieldName: String = "roles.role_name"
}

case object UserDomainId extends UserFieldType {
  val fieldName: String = "roles.domain_id"
}

case object UserRoleId extends UserFieldType {
  val fieldName: String = "roles.role_id"
}

case object UserLastAuthenticatedAt extends UserFieldType {
  val fieldName: String = "roles.last_authenticated_at"
}
