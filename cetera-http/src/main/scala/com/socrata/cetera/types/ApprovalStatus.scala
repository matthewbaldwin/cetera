package com.socrata.cetera.types

import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder

case class ApprovalStatus(
    status: String,
    booleanValue: Option[Boolean],
    raQueueField: String,
    raAccordingToParentField: String)

object ApprovalStatus {
  implicit val jCodec = AutomaticJsonCodecBuilder[ApprovalStatus]

  val approved = ApprovalStatus(
    "approved", Some(true), ApprovingDomainIdsFieldType.fieldName, ApprovedByParentFieldType.fieldName)
  val rejected = ApprovalStatus(
    "rejected", None, RejectingDomainIdsFieldType.fieldName, RejectedByParentFieldType.fieldName)
  val pending = ApprovalStatus(
    "pending", Some(false), PendingDomainIdsFieldType.fieldName, PendingOnParentFieldType.fieldName)

  val all = List(approved, pending, rejected)
}
