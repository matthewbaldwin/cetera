package com.socrata.cetera.auth

import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder
import org.slf4j.LoggerFactory

import com.socrata.cetera.auth.AuthedUser._
import com.socrata.cetera.errors.UnauthorizedError
import com.socrata.cetera.types.Domain

case class User(
    id: String,
    roleName: Option[String] = None,
    rights: Option[Seq[String]] = None,
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
    rights: Option[Seq[String]] = None,
    flags: Option[Seq[String]] = None) {

  def hasRole: Boolean = roleName.exists(_.nonEmpty)
  def hasRole(role: String): Boolean = roleName.exists(r => r.startsWith(role))
  def hasOneOfRoles(roles: Seq[String]): Boolean = roles.map(hasRole(_)).fold(false)(_ || _)
  def isSuperAdmin: Boolean = flags.exists(_.contains("admin"))
  def isAdmin: Boolean = hasRole(Administrator) || isSuperAdmin

  def authorizedOnDomain(domainId: Int): Boolean = {
    authenticatingDomain.domainId == domainId || isSuperAdmin
  }

  def canViewResource(domainId: Int, isAuthorized: Boolean): Boolean =
    isAuthorized && authorizedOnDomain(domainId) || isSuperAdmin

  def canViewLockedDownCatalog(domainId: Int): Boolean =
    canViewResource(domainId, hasOneOfRoles(Seq(Editor, Publisher, Viewer, Administrator)))

  def canViewPrivateMetadata(domainId: Int): Boolean =
    canViewResource(domainId, hasOneOfRoles(Seq(Publisher, Administrator)))

  def canViewAllViews(domainId: Int): Boolean =
    canViewResource(domainId, hasOneOfRoles(Seq(Publisher, Designer, Viewer, Administrator)))

  def canViewAllUsers: Boolean =
    canViewResource(authenticatingDomain.domainId, isAdmin) || isSuperAdmin

  def canViewDomainUsers: Boolean =
    canViewResource(authenticatingDomain.domainId, hasRole) || isSuperAdmin

  def canViewUsers(domainId: Int): Boolean =
    canViewResource(domainId, hasRole)
}

object AuthedUser {
  implicit val jCodec = AutomaticJsonCodecBuilder[AuthedUser]

  val Administrator = "administrator"
  val Designer = "designer"
  val Editor = "editor"
  val Publisher = "publisher"
  val Viewer = "viewer"
}
