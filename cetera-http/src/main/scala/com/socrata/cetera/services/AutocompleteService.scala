package com.socrata.cetera.services

import scala.util.control.NonFatal

import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.{HttpRequest, HttpResponse, HttpService}
import org.slf4j.LoggerFactory

import com.socrata.cetera._
import com.socrata.cetera.auth.{AuthParams, CoreClient}
import com.socrata.cetera.errors.{DomainNotFoundError, ElasticsearchError, UnauthorizedError}
import com.socrata.cetera.handlers._
import com.socrata.cetera.handlers.util._
import com.socrata.cetera.response.JsonResponses._
import com.socrata.cetera.response._
import com.socrata.cetera.search.{DocumentClient, DomainClient}
import com.socrata.cetera.util.LogHelper

class AutocompleteService(documentClient: DocumentClient, domainClient: DomainClient, coreClient: CoreClient) {
  lazy val logger = LoggerFactory.getLogger(classOf[AutocompleteService])

  def doSearch(
      queryParameters: MultiQueryParams,
      authParams: AuthParams,
      extendedHost: Option[String],
      requestId: Option[String])
    : (StatusResponse, SearchResults[CompletionResult], InternalTimings, Seq[String]) = {

    val now = Timings.now()
    val (authorizedUser, setCookies) = coreClient.optionallyAuthenticateUser(extendedHost, authParams, requestId)
    val ValidatedQueryParameters(searchParams, scoringParams, pagingParams, _) = QueryParametersParser(queryParameters)

    val (domains, domainSearchTime) = domainClient.findSearchableDomains(
      searchParams.searchContext, extendedHost, searchParams.domains,
      excludeLockedDomains = true, authorizedUser, requestId
    )
    val domainSet = domains.addDomainBoosts(scoringParams.domainBoosts)
    val authedUser = authorizedUser.map(_.convertToAuthedUser(domainSet.extendedHost))

    val (completions, totalCount, suggestTime) =
      documentClient.suggest(domainSet, searchParams, scoringParams, pagingParams, authedUser)

    val formattedResults = SearchResults(completions, totalCount)
    val timings = InternalTimings(Timings.elapsedInMillis(now), Seq(domainSearchTime, suggestTime))

    (OK, formattedResults.copy(timings = Some(timings)), timings, setCookies)
  }

  def search(req: HttpRequest): HttpResponse = {
    logger.debug(LogHelper.formatHttpRequestVerbose(req))

    val authParams = AuthParams.fromHttpRequest(req)
    val extendedHost = req.header(HeaderXSocrataHostKey)
    val requestId = req.header(HeaderXSocrataRequestIdKey)

    try {
      val (status, formattedResults, timings, setCookies) =
        doSearch(req.multiQueryParams, authParams, extendedHost, requestId)

      logger.info(LogHelper.formatRequest(req, timings))
      Http.decorate(Json(formattedResults, pretty = true), status, setCookies)
    } catch {
      case e: IllegalArgumentException =>
        logger.error(e.getMessage)
        BadRequest ~> HeaderAclAllowOriginAll ~> jsonError(e.getMessage)
      case e: DomainNotFoundError =>
        logger.error(e.getMessage)
        NotFound ~> HeaderAclAllowOriginAll ~> jsonError(e.getMessage)
      case e: UnauthorizedError =>
        logger.error(e.getMessage)
        Unauthorized ~> HeaderAclAllowOriginAll ~> jsonError(e.getMessage)
      case NonFatal(e) =>
        val msg = "Cetera search service error"
        val esError = ElasticsearchError(e)
        logger.error(s"$msg: $esError")
        InternalServerError ~> HeaderAclAllowOriginAll ~> jsonError("We're sorry. Something went wrong.")
    }
  }

  // $COVERAGE-OFF$ jetty wiring
  case object Service extends SimpleResource {
    override def get: HttpService = search
  }

  // $COVERAGE-ON$
}
