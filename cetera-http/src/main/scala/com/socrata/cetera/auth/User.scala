package com.socrata.cetera.auth

import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder
import org.slf4j.LoggerFactory

import com.socrata.cetera.auth.AuthedUser._
import com.socrata.cetera.errors.UnauthorizedError
import com.socrata.cetera.types.{Document, Domain}

case class User(
    id: String,
    roleName: Option[String] = None,
    rights: Option[Set[String]] = None,
    flags: Option[Seq[String]] = None) {

  val logger = LoggerFactory.getLogger(getClass)

  def convertToAuthedUser(extendedHost: Option[Domain]): AuthedUser =
    extendedHost match {
      case Some(h) => AuthedUser(id, h, roleName, rights, flags)
      case None =>
        // in order to have a user, core must have authenticated the user with the extended host cname.
        // but to have no extended host `Domain` here implies we could not find the host in ES
        logger.warn(s"Unable to attach authenticating domain to user $id")
        throw UnauthorizedError(Some(id), "do anything without an authenticating domain")
    }
}

object User {
  implicit val jCodec = AutomaticJsonCodecBuilder[User]
}

case class AuthedUser(
    id: String,
    authenticatingDomain: Domain,
    roleName: Option[String] = None,
    rights: Option[Set[String]] = None,
    flags: Option[Seq[String]] = None) {

  def hasAnyRole: Boolean = roleName.exists(_.nonEmpty)
  def isSuperAdmin: Boolean = flags.exists(_.contains("admin"))
  def hasRight(right: String): Boolean = rights.exists(r => r.contains(right))

  def authorizedOnDomain(domainId: Int): Boolean =
    authenticatingDomain.id == domainId || isSuperAdmin

  def canViewResource(domainId: Int, isAuthorized: Boolean): Boolean =
    isAuthorized && authorizedOnDomain(domainId) || isSuperAdmin

  // permissions based on rights
  def canViewAllViews(domainId: Int): Boolean =
    canViewResource(domainId, hasRight(toViewOthersViews) && hasRight(toViewOthersStories))

  def canViewAllOfSomeViews(domainId: Int, isStory: Boolean): Boolean =
    if (isStory) {
      canViewResource(domainId, hasRight(toViewOthersStories))
    } else {
      canViewResource(domainId, hasRight(toViewOthersViews))
    }

  def canViewAllPrivateMetadata(domainId: Int): Boolean =
    canViewResource(domainId, hasRight(toEditOthersViews) && hasRight(toEditOthersStories))

  def canViewAllOfSomePrivateMetadata(domainId: Int, isStory: Boolean): Boolean =
    if (isStory) {
      canViewResource(domainId, hasRight(toEditOthersStories))
    } else {
      canViewResource(domainId, hasRight(toEditOthersViews))
    }

  def canViewAllOfSomeApprovalsNotes(domainId: Int, isStory: Boolean): Boolean =
    if (isStory) {
      canViewResource(domainId,
        hasRight(toReviewApprovals) || hasRight(toConfigureApprovals) || hasRight(toEditOthersStories))
    } else {
      canViewResource(domainId,
        hasRight(toReviewApprovals) || hasRight(toConfigureApprovals) || hasRight(toEditOthersViews))
    }

  def canViewAllUsers: Boolean =
    hasRight(toViewAllUsers) || isSuperAdmin

  // permissions based on roles (specifically being a domain member)
  def canViewLockedDownCatalog(domainId: Int): Boolean =
    canViewResource(domainId, hasAnyRole)

  def canViewUsersOnDomain(domainId: Int): Boolean =
    canViewResource(domainId, hasAnyRole)

  def owns(doc: Document): Boolean = doc.owner.id == id
  def shares(doc: Document): Boolean = doc.sharedTo.contains(id)
}

object AuthedUser {
  implicit val jCodec = AutomaticJsonCodecBuilder[AuthedUser]

  val toViewOthersViews = "view_others_datasets"
  val toViewOthersStories = "view_story"
  val toViewAllUsers = "manage_users"
  val toEditOthersViews = "edit_others_datasets"
  val toEditOthersStories = "edit_others_stories"
  val toReviewApprovals = "review_approvals"
  val toConfigureApprovals = "configure_approvals"
}
