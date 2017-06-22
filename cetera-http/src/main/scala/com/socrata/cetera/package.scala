package com.socrata

import com.socrata.http.server.responses._

package object cetera {
  val HeaderAclAllowOriginAll = Header("Access-Control-Allow-Origin", "*")
  val HeaderAuthorizationKey = "Authorization"
  val HeaderCookieKey = "Cookie"
  val HeaderSetCookieKey = "Set-Cookie"
  val HeaderUserAgent = "User-Agent"
  val HeaderXSocrataHostKey = "X-Socrata-Host"
  val HeaderXSocrataRequestIdKey = "X-Socrata-RequestId"

  val esDocumentType = "document"
  val esDomainType = "domain"
  val esUserType = "user"
}
