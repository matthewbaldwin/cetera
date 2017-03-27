package com.socrata.cetera.services

import scala.util.control.NonFatal

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.codec.DecodeError
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.matcher.{FirstOf, PObject, Variable}
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.{HttpRequest, HttpResponse, HttpService}
import org.slf4j.LoggerFactory

import com.socrata.cetera._
import com.socrata.cetera.auth.{AuthParams, CoreClient}
import com.socrata.cetera.errors.{DomainNotFoundError, ElasticsearchError, UnauthorizedError}
import com.socrata.cetera.handlers.util._
import com.socrata.cetera.handlers.{QueryParametersParser, ValidatedQueryParameters}
import com.socrata.cetera.response.JsonResponses.jsonError
import com.socrata.cetera.response.{Http, InternalTimings, SearchResults, Timings}
import com.socrata.cetera.search.{BaseDocumentClient, BaseDomainClient}
import com.socrata.cetera.types._
import com.socrata.cetera.util.LogHelper

class CountService(
    documentClient: BaseDocumentClient,
    domainClient: BaseDomainClient,
    coreClient: CoreClient) {
  lazy val logger = LoggerFactory.getLogger(classOf[CountService])
  val bucketName = "buckets"

  // Possibly belongs in the client
  def extract(body: JValue): Either[DecodeError, Seq[JValue]] = {
    val buckets = Variable.decodeOnly[Seq[JValue]]

    // This defines how to extract the buckets from the search defined in DocumentAggregations.
    // The key in the aggregations object should match the 'terms' defined in the AggregationBuilders there.
    val pattern = PObject(
      "aggregations" -> FirstOf(
        PObject("domains" -> PObject(bucketName -> buckets)),
        PObject("domain_categories" -> PObject(bucketName -> buckets)),
        PObject("domain_tags" -> PObject(bucketName -> buckets)),
        PObject("annotations" -> PObject("names" -> PObject(bucketName -> buckets))),
        PObject("owners" -> PObject(bucketName -> buckets))
      )
    )

    pattern.matches(body).right.map(buckets)
  }

  // Unhandled exception on missing key
  def format(counts: Seq[JValue]): SearchResults[Count] =
    SearchResults(counts.map { c => Count(c.dyn.key.!, c.dyn.doc_count.!) }, counts.size)

  def doAggregate(
      field: DocumentFieldType with Countable with Rawable,
      queryParameters: MultiQueryParams,
      authParams: AuthParams,
      extendedHost: Option[String],
      requestId: Option[String])
    : (StatusResponse, SearchResults[Count], InternalTimings, Seq[String]) = {

    val now = Timings.now()

    val (authorizedUser, setCookies) = coreClient.optionallyAuthenticateUser(extendedHost, authParams, requestId)
    val ValidatedQueryParameters(searchParams, _, pagingParams, _) =
      QueryParametersParser(queryParameters)

    val (domainSet, domainSearchTime) = domainClient.findSearchableDomains(
      searchParams.searchContext, extendedHost, searchParams.domains,
      excludeLockedDomains = true, authorizedUser, requestId
    )
    val authedUser = authorizedUser.map(u => u.copy(authenticatingDomain = domainSet.extendedHost))

    val search = documentClient.buildCountRequest(
      field, domainSet, searchParams, pagingParams, authedUser, requireAuth = false)
    logger.info(LogHelper.formatEsRequest(search))

    val res = search.execute.actionGet
    val timings = InternalTimings(Timings.elapsedInMillis(now), Seq(domainSearchTime, res.getTookInMillis))
    val json = JsonReader.fromString(res.toString)
    val counts = extract(json) match {
      case Right(extracted) => extracted
      case Left(error) =>
        logger.error(error.english)
        throw new Exception("error!  ERROR!! DOES NOT COMPUTE")
    }
    val formattedResults: SearchResults[Count] = format(counts).copy(timings = Some(timings))
    (OK, formattedResults, timings, setCookies)
  }

  // $COVERAGE-OFF$ jetty wiring
  // scalastyle:ignore cyclomatic.complexity
  def aggregate(field: DocumentFieldType with Countable with Rawable)(req: HttpRequest): HttpResponse = {
    implicit val cEncode = field match {
      // TODO: we only ever use the first 4 of these. We should maybe reconsider our trait organization in
      // FieldTypes so it isn't necessary to include unused fields here.
      case CategoriesFieldType => Count.encode("category")
      case TagsFieldType => Count.encode("tag")
      case DomainCategoryFieldType => Count.encode("domain_category")
      case DomainTagsFieldType => Count.encode("domain_tag")
      case OwnerIdFieldType => Count.encode("owner_id")
      case AttributionFieldType => Count.encode("attribution")
      case ProvenanceFieldType => Count.encode("provenance")
      case LicenseFieldType => Count.encode("license")
    }

    try {
      val authParams = AuthParams.fromHttpRequest(req)
      val extendedHost = req.header(HeaderXSocrataHostKey)
      val requestId = req.header(HeaderXSocrataRequestIdKey)

      val (status, formattedResults, timings, setCookies) =
        doAggregate(field, req.multiQueryParams, authParams, extendedHost, requestId)
      logger.info(LogHelper.formatRequest(req, timings))
      Http.decorate(Json(formattedResults, pretty = true), status, setCookies)
    } catch {
      case e: IllegalArgumentException =>
        logger.info(e.getMessage)
        BadRequest ~> HeaderAclAllowOriginAll ~> jsonError(e.getMessage)
      case e: DomainNotFoundError =>
        logger.error(e.getMessage)
        NotFound ~> HeaderAclAllowOriginAll ~> jsonError(e.getMessage)
      case e: UnauthorizedError =>
        logger.error(e.getMessage)
        Unauthorized ~> HeaderAclAllowOriginAll ~> jsonError(e.getMessage)
      case NonFatal(e) =>
        val esError = ElasticsearchError(e)
        logger.error(s"Database error: ${esError.getMessage}")
        InternalServerError ~> HeaderAclAllowOriginAll ~> jsonError("We're sorry. Something went wrong.")
    }
  }

  case class Service(field: DocumentFieldType with Countable with Rawable) extends SimpleResource {
    override def get: HttpService = aggregate(field)
  }

  // $COVERAGE-ON$
}
