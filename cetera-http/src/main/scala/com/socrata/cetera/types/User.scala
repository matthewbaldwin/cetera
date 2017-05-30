package com.socrata.cetera.types

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.codec.JsonDecode
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonKeyStrategy, JsonUtil, Strategy}
import org.slf4j.LoggerFactory

import com.socrata.cetera.errors.JsonDecodeException

@JsonKeyStrategy(Strategy.Underscore)
case class Role(domainId: Int, roleName: String)

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
    screenName: Option[String],
    email: Option[String],
    roles: Option[Set[Role]],
    flags: Option[Seq[String]],
    profileImageUrlLarge: Option[String],
    profileImageUrlMedium: Option[String],
    profileImageUrlSmall: Option[String]) {
  def roleName(domainId: Int): Option[String] = {
    roles.flatMap(rs => rs.collect { case r: Role if r.domainId == domainId => r.roleName }.headOption)
  }
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
  screenName: Option[String],
  email: Option[String],
  roleName: Option[String],
  flags: Option[Seq[String]],
  profileImageUrlLarge: Option[String],
  profileImageUrlMedium: Option[String],
  profileImageUrlSmall: Option[String])

object DomainUser {
  implicit val codec = AutomaticJsonCodecBuilder[DomainUser]

  def apply(domain: Option[Domain], esUser: EsUser): Option[DomainUser] = {
    Some(
      DomainUser(
        esUser.id,
        esUser.screenName,
        esUser.email,
        domain.flatMap { d: Domain => esUser.roleName(d.domainId) },
        esUser.flags,
        esUser.profileImageUrlLarge,
        esUser.profileImageUrlMedium,
        esUser.profileImageUrlSmall
      )
    )
  }
}
