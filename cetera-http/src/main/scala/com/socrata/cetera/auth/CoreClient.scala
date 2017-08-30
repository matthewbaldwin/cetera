package com.socrata.cetera.auth

import scala.util.control.NonFatal

import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder
import com.rojoma.simplearm.v2.{ResourceScope, using}
import com.socrata.http.client.{HttpClientHttpClient, RequestBuilder}
import org.apache.http.HttpStatus._
import org.slf4j.LoggerFactory

import com.socrata.cetera._
import com.socrata.cetera.util.LogHelper

case class CoreError(message: String)
object CoreError {
  implicit val jCodec = AutomaticJsonCodecBuilder[CoreError]
}

class CoreClient(
    httpClient: HttpClientHttpClient,
    host: String,
    port: Int,
    connectTimeoutMs: Int,
    appToken: Option[String]) {

  val logger = LoggerFactory.getLogger(getClass)
  val headers = List(("Content-Type", "application/json; charset=utf-8")) ++
    appToken.map(("X-App-Token", _))

  val coreBaseRequest = RequestBuilder(host, secure = false)
    .port(port)
    .connectTimeoutMS(connectTimeoutMs)
    .addHeaders(headers)

  private def coreRequest(
      paths: Iterable[String],
      params: Map[String, String],
      domain: Option[String] = None,
      authParams: AuthParams = AuthParams(),
      requestId: Option[String] = None)
    : RequestBuilder = {
    val headers = List(
      domain.map((HeaderXSocrataHostKey, _)),
      requestId.map((HeaderXSocrataRequestIdKey, _))
    ).flatten ++ authParams.headers

    coreBaseRequest
      .addPaths(paths)
      .addParameters(params)
      .addHeaders(headers)
  }

  /**
   * Optionally authenticate a user.
   *
   * @param domain the domain of the user to authenticate
   * @param authParams the authentication params for the user
   * @param requestId a somewhat unique identifier that helps string requests together across services
   * @return (user if `authParams` are defined and valid, client cookies to set)
   */
  def optionallyAuthenticateUser(
      domain: Option[String],
      authParams: AuthParams,
      requestId: Option[String])
    : (Option[User], Seq[String]) =
    (domain, authParams) match {
      case (Some(_), AuthParams(None, None, None)) =>
        logger.debug("Search context was provided without auth params. Not authenticating user.")
        (None, Seq.empty)
      case (None, authParams) if authParams.areDefined =>
        logger.warn("Auth params provided without search context. Not authenticating user.")
        (None, Seq.empty)
      case (Some(cname), authParams) if authParams.areDefined =>
        authenticateUser(cname, authParams, requestId)
      case _ => (None, Seq.empty)
    }

  /**
   * Authenticate a user.
   *
   * @param domain the domain of the user to authenticate
   * @param authParams the authentication params for the user
   * @param requestId a somewhat unique identifier that helps string requests together across services
   * @return (user if `authParams` are defined and valid, client cookies to set)
   */
  def authenticateUser(
      domain: String,
      authParams: AuthParams,
      requestId: Option[String])
    : (Option[User], Seq[String]) = {
    val req = coreRequest(List("users.json"), Map("method" -> "getCurrent"), Some(domain), authParams, requestId)

    logger.info(s"Validating user on domain $domain")
    logger.debug(LogHelper.formatSimpleHttpRequestBuilderVerbose(req))
    using(new ResourceScope(s"retrieving current user from core")) { rs =>
      try {
        val res = httpClient.execute(req.get, rs)
        val setCookies = res.headers(HeaderSetCookieKey).toSeq
        res.resultCode match {
          case SC_OK => res.value[User]() match {
            case Right(u) => (Some(u), setCookies)
            case Left(err) =>
              logger.error(s"Could not parse core user data: \n ${err.english}")
              (None, setCookies)
          }
          case code: Int => res.value[CoreError]() match {
            case Right(m) =>
              logger.warn(s"Could not validate user. Core returned a $code with the message: ${m.message}")
              (None, setCookies)
            case Left(err) =>
              logger.error(s"Could not validate user. Core returned a $code with no message")
              (None, setCookies)
          }
        }
      } catch {
        case NonFatal(e) =>
          logger.error("Cannot reach core to validate user")
          (None, Seq.empty)
      }
    }
  }
}
