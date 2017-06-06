package com.socrata.cetera.services

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.{HttpRequest, HttpResponse, HttpService}
import org.elasticsearch.search.aggregations.bucket.filter.Filter
import org.elasticsearch.search.aggregations.bucket.nested.Nested
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.slf4j.LoggerFactory

import com.socrata.cetera._
import com.socrata.cetera.auth.{AuthParams, CoreClient}
import com.socrata.cetera.errors.{DomainNotFoundError, ElasticsearchError, UnauthorizedError}
import com.socrata.cetera.handlers.{QueryParametersParser, ValidatedQueryParameters}
import com.socrata.cetera.handlers.util._
import com.socrata.cetera.response.JsonResponses._
import com.socrata.cetera.response.{Http, InternalTimings, Timings}
import com.socrata.cetera.search.{BaseDocumentClient, BaseDomainClient}
import com.socrata.cetera.types._
import com.socrata.cetera.util.LogHelper

class FacetService(
    documentClient: BaseDocumentClient,
    domainClient: BaseDomainClient,
    coreClient: CoreClient) {
  lazy val logger = LoggerFactory.getLogger(classOf[FacetService])

  def doAggregate( // scalastyle:ignore method.length
      queryParameters: MultiQueryParams,
      cname: String,
      authParams: AuthParams,
      extendedHost: Option[String],
      requestId: Option[String])
    : (StatusResponse, Seq[FacetCount], InternalTimings, Seq[String]) = {

    val startMs = Timings.now()
    val (user, setCookies) = coreClient.optionallyAuthenticateUser(extendedHost, authParams, requestId)
    val ValidatedQueryParameters(searchParams, _, pagingParams, _) =
      QueryParametersParser(queryParameters)

    val (domainSet, domainSearchTime) = domainClient.findSearchableDomains(
      Some(cname), extendedHost, Some(Set(cname)),
      excludeLockedDomains = true, user, requestId
    )
    val authedUser = user.map(_.convertToAuthedUser(domainSet.extendedHost))

    domainSet.searchContext match {
      case None => // domain exists but user isn't authorized to see it
        throw UnauthorizedError(authedUser.map(_.id), s"search for facets on $cname")
      case Some(d) => // domain exists and is viewable by user
        val request = documentClient.buildFacetRequest(domainSet, searchParams, pagingParams, authedUser)
        logger.info(LogHelper.formatEsRequest(request))
        val res = request.execute().actionGet()

        val aggs = res.getAggregations.asMap().asScala

        val datatypesValues = aggs("datatypes").asInstanceOf[Terms].getBuckets.asScala.map { b =>
          ValueCount(b.getKey.asInstanceOf[String], b.getDocCount)
        }.filter(_.value.nonEmpty)

        val datatypesFacets = Seq(FacetCount("datatypes", datatypesValues.map(_.count).sum, datatypesValues))

        val categoriesValues = aggs("categories").asInstanceOf[Terms].getBuckets.asScala.map { b =>
          ValueCount(b.getKey.asInstanceOf[String], b.getDocCount)
        }.filter(_.value.nonEmpty)

        val categoriesFacets = Seq(FacetCount("categories", categoriesValues.map(_.count).sum, categoriesValues))

        val tagsValues = aggs("tags").asInstanceOf[Terms].getBuckets.asScala.map { b =>
          ValueCount(b.getKey.asInstanceOf[String], b.getDocCount)
        }.filter(_.value.nonEmpty)

        val tagsFacets = Seq(FacetCount("tags", tagsValues.map(_.count).sum, tagsValues))

        val metadataFacets = aggs("metadata").asInstanceOf[Nested]
          .getAggregations.get[Terms]("keys").getBuckets.asScala.map { b =>
          val values = b.getAggregations.get[Terms]("values").getBuckets.asScala.map { v =>
            ValueCount(v.getKey.asInstanceOf[String], v.getDocCount)
          }
          FacetCount(b.getKey.asInstanceOf[String], b.getDocCount, values)
        }

        val facets: Seq[FacetCount] = Seq.concat(datatypesFacets, categoriesFacets, tagsFacets, metadataFacets)
        val timings = InternalTimings(Timings.elapsedInMillis(startMs), Seq(domainSearchTime, res.getTookInMillis))
        (OK, facets, timings, setCookies)
    }
  }

  def aggregate(cname: String)(req: HttpRequest): HttpResponse = {
    val authParams = AuthParams.fromHttpRequest(req)
    val extendedHost = req.header(HeaderXSocrataHostKey)
    val requestId = req.header(HeaderXSocrataRequestIdKey)

    try {
      val (status, facets, timings, setCookies) =
        doAggregate(req.multiQueryParams, cname, authParams, extendedHost, requestId)
      logger.info(LogHelper.formatRequest(req, timings))
      Http.decorate(Json(facets, pretty = true), status, setCookies)
    } catch {
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

  // $COVERAGE-OFF$ jetty wiring
  case class Service(cname: String) extends SimpleResource {
    override def get: HttpService = aggregate(cname)
  }
  // $COVERAGE-ON$
}
