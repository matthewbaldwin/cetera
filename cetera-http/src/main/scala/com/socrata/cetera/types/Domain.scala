package com.socrata.cetera.types

import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonKey, JsonKeyStrategy, JsonUtil, Strategy}
import org.slf4j.LoggerFactory

import com.socrata.cetera.errors.JsonDecodeException

@JsonKeyStrategy(Strategy.Underscore)
case class Domain(
    @JsonKey("domain_id") id: Int,
    @JsonKey("domain_cname") cname: String,
    aliases: Option[Set[String]],
    siteTitle: Option[String],
    organization: Option[String],
    isCustomerDomain: Boolean,
    hasFontanaApprovals: Boolean,
    moderationEnabled: Boolean,
    routingApprovalEnabled: Boolean,
    lockedDown: Boolean,
    apiLockedDown: Boolean) {

  val isLocked = lockedDown || apiLockedDown
  val aliasSet = aliases.getOrElse(Set.empty) + cname
}

object Domain {
  implicit val jCodec = AutomaticJsonCodecBuilder[Domain]
  val logger = LoggerFactory.getLogger(getClass)

  def apply(source: String): Option[Domain] = {
    Option(source).flatMap { s =>
      JsonUtil.parseJson[Domain](s) match {
        case Right(domain) => Some(domain)
        case Left(err) =>
          logger.error(err.english)
          throw new JsonDecodeException(err)
      }
    }
  }
}
