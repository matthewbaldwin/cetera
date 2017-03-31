package com.socrata.cetera.services

import scala.util.control.NonFatal

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.codec.DecodeError
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.matcher.{PObject, Variable}
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.{HttpRequest, HttpResponse, HttpService}
import org.slf4j.LoggerFactory

import com.socrata.cetera._
import com.socrata.cetera.auth.{AuthParams, CoreClient}
import com.socrata.cetera.errors.{DomainNotFoundError, ElasticsearchError, JsonDecodeException, UnauthorizedError}
import com.socrata.cetera.handlers.util._
import com.socrata.cetera.handlers.{QueryParametersParser, ValidatedQueryParameters}
import com.socrata.cetera.response.JsonResponses.jsonError
import com.socrata.cetera.response._
import com.socrata.cetera.search.BaseDomainClient
import com.socrata.cetera.types.Count
import com.socrata.cetera.util.LogHelper

class DomainCountService(domainClient: BaseDomainClient, coreClient: CoreClient) {
  lazy val logger = LoggerFactory.getLogger(classOf[DomainCountService])

  private def extract(json: JValue): Either[DecodeError, Seq[JValue]] = {
    val buckets = Variable.decodeOnly[Seq[JValue]]
    val pattern = PObject(
      "aggregations" -> PObject(
        "domains" -> PObject(
          "buckets" -> buckets
        )
      )
    )

    pattern.matches(json).right.map(buckets)
  }

 private def format(counts: Seq[JValue]): SearchResults[Count] =
    SearchResults(counts.map { c => Count(c.dyn.key.!, c.dyn.documents.filtered.doc_count.!) }, counts.size)

 def doAggregate(
     queryParameters: MultiQueryParams,
     authParams: AuthParams,
     extendedHost: Option[String],
     requestId: Option[String])
   : (StatusResponse, SearchResults[Count], InternalTimings, Seq[String]) = {

   val now = Timings.now()
   val (authorizedUser, setCookies) = coreClient.optionallyAuthenticateUser(extendedHost, authParams, requestId)
   val searchParams = QueryParametersParser(queryParameters).searchParamSet
   val (domainSet, domainSearchTime) = domainClient.findSearchableDomains(
     searchParams.searchContext, extendedHost, searchParams.domains,
     excludeLockedDomains = true, authorizedUser, requestId
   )
   val authedUser = authorizedUser.map(u => u.copy(authenticatingDomain = domainSet.extendedHost))

   val search = domainClient.buildCountRequest(domainSet, searchParams, authedUser)
   logger.info(LogHelper.formatEsRequest(search))
   val res = search.execute.actionGet
   val timings = InternalTimings(Timings.elapsedInMillis(now), Seq(domainSearchTime, res.getTookInMillis))
   val json = JsonReader.fromString(res.toString)
   val counts = extract(json) match {
     case Right(extracted) => extracted
     case Left(error) =>
       logger.error(error.english)
       throw new JsonDecodeException(error)
   }
   val formattedResults: SearchResults[Count] = format(counts).copy(timings = Some(timings))
   (OK, formattedResults, timings, setCookies)
 }

  // $COVERAGE-OFF$ jetty wiring
  def aggregate()(req: HttpRequest): HttpResponse = {
    implicit val cEncode = Count.encode(esDomainType)

    try {
      val authParams = AuthParams.fromHttpRequest(req)
      val extendedHost = req.header(HeaderXSocrataHostKey)
      val requestId = req.header(HeaderXSocrataRequestIdKey)

      val (status, formattedResults, timings, setCookies) =
        doAggregate(req.multiQueryParams, authParams, extendedHost, requestId)
      logger.info(LogHelper.formatRequest(req, timings))
      Http.decorate(Json(formattedResults, pretty = true), status, setCookies)
    } catch {
      case e: IllegalArgumentException =>
        logger.info(e.getMessage)
        BadRequest ~> HeaderAclAllowOriginAll ~> jsonError(e.getMessage)
      case e: DomainNotFoundError =>
        val msg = s"Domain not found: $e"
        logger.error(msg)
        NotFound ~> HeaderAclAllowOriginAll ~> jsonError(msg)
      case e: UnauthorizedError =>
        logger.error(e.getMessage)
        Unauthorized ~> HeaderAclAllowOriginAll ~> jsonError(e.getMessage)
      case NonFatal(e) =>
        val esError = ElasticsearchError(e)
        logger.error(s"Database error: ${esError.getMessage}")
        InternalServerError ~> HeaderAclAllowOriginAll ~> jsonError("We're sorry. Something went wrong.")
    }
  }

  object Service extends SimpleResource {
    override def get: HttpService = aggregate()
  }

  // $COVERAGE-ON$
}
