package com.socrata.cetera.handlers

import com.socrata.http.server.implicits._
import com.socrata.http.server.responses.NotFound
import com.socrata.http.server.routing.SimpleRouteContext._
import com.socrata.http.server.{HttpRequest, HttpResponse, HttpService}

import com.socrata.cetera._
import com.socrata.cetera.response.JsonResponses.jsonError
import com.socrata.cetera.types._

// $COVERAGE-OFF$ jetty wiring
// Now the router knows about our ES field names
class Router(
    versionResource: => HttpService,
    catalogResource: Boolean => HttpService,
    autocompleteResource: Boolean => HttpService,
    facetResource: String => HttpService,
    domainCountResource: => HttpService,
    countResource: DocumentFieldType with Countable with Rawable => HttpService,
    userSearchResource: => HttpService) {

  val routes = Routes(
    // /version is for internal use
    Route("/version", versionResource),

    // general document search
    Route("/catalog", catalogResource(false)),
    Route("/catalog/v1", catalogResource(false)),

    // general user search
    Route("/catalog/users", userSearchResource),
    Route("/catalog/v1/users", userSearchResource),

    // document counts for queries grouped by domain
    Route("/catalog/domains", domainCountResource),
    Route("/catalog/v1/domains", domainCountResource),

    // facets by domain
    Route("/catalog/domains/{String}/facets", facetResource),
    Route("/catalog/v1/domains/{String}/facets", facetResource),

    // document counts for queries grouped by category
    Route("/catalog/categories", countResource(CategoriesFieldType)),
    Route("/catalog/v1/categories", countResource(CategoriesFieldType)),

    // document counts for queries grouped by tag
    Route("/catalog/tags", countResource(TagsFieldType)),
    Route("/catalog/v1/tags", countResource(TagsFieldType)),

    // document counts for queries grouped by domain_category
    Route("/catalog/domain_categories", countResource(DomainCategoryFieldType)),
    Route("/catalog/v1/domain_categories", countResource(DomainCategoryFieldType)),

    // document counts for queries grouped by domain_tags
    Route("/catalog/domain_tags", countResource(DomainTagsFieldType)),
    Route("/catalog/v1/domain_tags", countResource(DomainTagsFieldType)),

    // catalog autocomplete
    Route("/catalog/autocomplete", autocompleteResource(false)),
    Route("/catalog/v1/autocomplete", autocompleteResource(false)),

    //
    // Internal endpoints
    //

    // internal catalog search
    // Note: this route can very easily be folded into the /catalog route when we are ready
    // to support /browse showing more than public/published/approved when users are signed in.
    Route("/internal_catalog", catalogResource(true)),
    Route("/internal_catalog/v1", catalogResource(true)),

    //  Routes we should deprecate
    Route("/personal_catalog", catalogResource(true)),  // TODO deprecate
    Route("/personal_catalog/v1", catalogResource(true))  // TODO deprecate
  )

  def route(req: HttpRequest): HttpResponse =
    routes(req.requestPath) match {
      case Some(s) => s(req)
      case None => NotFound ~> HeaderAclAllowOriginAll ~> jsonError("not found")
    }
}
// $COVERAGE-ON$
