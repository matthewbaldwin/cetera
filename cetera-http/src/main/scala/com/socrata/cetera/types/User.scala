package com.socrata.cetera.types

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.codec.JsonDecode
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonKeyStrategy, JsonUtil, Strategy}
import org.slf4j.LoggerFactory

import com.socrata.cetera.errors.JsonDecodeException

@JsonKeyStrategy(Strategy.Underscore)
case class Role(
    domainId: Int,
    roleName: String,
    roleId: Option[Int] = None,
    lastAuthenticatedAt:Option[BigInt] = None)

object Role {
  implicit val codec = AutomaticJsonCodecBuilder[Role]
}

// NOTE: not including email address in this model; doing so would require that we check auth
@JsonKeyStrategy(Strategy.Underscore)
case class UserInfo(id: String, displayName: Option[String])

object UserInfo {
  implicit val codec = AutomaticJsonCodecBuilder[UserInfo]
  val logger = LoggerFactory.getLogger(getClass)

  def fromJValue(jval: JValue): Option[UserInfo] =
    JsonDecode.fromJValue(jval) match {
      case Right(user) => Some(user)
      case Left(err) =>
        logger.error(err.english)
        throw new JsonDecodeException(err)
    }
}

@JsonKeyStrategy(Strategy.Underscore)
case class EsUser(
    id: String,
    screenName: Option[String] = None,
    email: Option[String] = None,
    roles: Option[Set[Role]] = None,
    flags: Option[Seq[String]] = None,
    profileImageUrlLarge: Option[String] = None,
    profileImageUrlMedium: Option[String] = None,
    profileImageUrlSmall: Option[String] = None) {

  def roleName(domainId: Int): Option[String] =
    roles.flatMap(rs => rs.collectFirst { case r: Role if r.domainId == domainId => r.roleName })

  def roleId(domainId: Int): Option[Int] =
    roles.flatMap(rs => rs.collectFirst { case r: Role if r.domainId == domainId => r.roleId }).flatten

  def lastAuthenticatedAt(domainId: Int): Option[BigInt] =
    roles.flatMap(rs => rs.collectFirst { case r: Role if r.domainId == domainId => r.lastAuthenticatedAt }).flatten
}

object EsUser {
  implicit val codec = AutomaticJsonCodecBuilder[EsUser]
  val logger = LoggerFactory.getLogger(getClass)

  def apply(source: String): Option[EsUser] =
    Option(source).flatMap { s =>
      JsonUtil.parseJson[EsUser](s) match {
        case Right(user) => Some(user)
        case Left(err) =>
          logger.error(err.english)
          throw new JsonDecodeException(err)
      }
    }
}

@JsonKeyStrategy(Strategy.Underscore)
case class DomainUser(
    id: String,
    screenName: Option[String] = None,
    email: Option[String] = None,
    roleName: Option[String] = None,
    roleId: Option[Int] = None,
    lastAuthenticatedAt: Option[BigInt] = None,
    flags: Option[Seq[String]] = None,
    profileImageUrlLarge: Option[String] = None,
    profileImageUrlMedium: Option[String] = None,
    profileImageUrlSmall: Option[String] = None)

object DomainUser {
  implicit val codec = AutomaticJsonCodecBuilder[DomainUser]

  def apply(domain: Domain, esUser: EsUser): DomainUser =
    DomainUser(
      esUser.id,
      esUser.screenName,
      esUser.email,
      esUser.roleName(domain.domainId),
      esUser.roleId(domain.domainId),
      esUser.lastAuthenticatedAt(domain.domainId),
      esUser.flags,
      esUser.profileImageUrlLarge,
      esUser.profileImageUrlMedium,
      esUser.profileImageUrlSmall
  )
}

sealed trait UserType {
  def singular: String
  def plural: String
}

object UserType {
  val all = List(Owner.singular, Owner.plural)

  def apply(s: String): Option[UserType] =
    s.toLowerCase match {
      case Owner.singular | Owner.plural => Some(Owner)
      case _ => None
    }
}

case object Owner extends UserType {
  override val singular = "owner"
  override val plural = "owners"
}
