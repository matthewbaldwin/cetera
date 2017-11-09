package com.socrata.cetera

import scala.io.Source

import com.socrata.cetera.types.{Document, Domain}

trait TestESDomains {

  val domains = {
    val domainTSV = Source.fromInputStream(getClass.getResourceAsStream("/domains.tsv"))
    val iter = domainTSV.getLines().map(_.split("\t"))
    iter.drop(1) // drop the header columns
    iter.map { tsvLine =>
      val domainId = tsvLine(0).toInt
      val aliases = domainId match {
        case 0 => Some(Set("pc.net"))
        case 1 => Some(Set("opendata.socrata.com"))
        case 2 => Some(Set("teal.org", "zaffre.org"))
        case _ => None
      }
      Domain(
        id = domainId,
        cname = tsvLine(1),
        aliases = aliases,
        siteTitle = Option(tsvLine(2)).filter(_.nonEmpty),
        organization = Option(tsvLine(3)).filter(_.nonEmpty),
        isCustomerDomain = tsvLine(4).toBoolean,
        hasFontanaApprovals = tsvLine(5).toBoolean,
        moderationEnabled = tsvLine(6).toBoolean,
        routingApprovalEnabled = tsvLine(7).toBoolean,
        lockedDown = tsvLine(8).toBoolean,
        apiLockedDown = tsvLine(9).toBoolean)
    }.toSeq
  }

  val fontanaDomain = domains(0)
  val vmDomain = domains(1)
  val raDomain = domains(2)
  val vmRaDomain = domains(3)
}
