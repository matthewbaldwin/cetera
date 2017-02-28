package com.socrata.cetera.search

import org.elasticsearch.action.search.{SearchRequestBuilder, SearchType}
import org.elasticsearch.index.query.{FilterBuilders, QueryBuilders}
import org.slf4j.LoggerFactory

import com.socrata.cetera._
import com.socrata.cetera.auth.{CoreClient, User}
import com.socrata.cetera.errors.DomainNotFoundError
import com.socrata.cetera.handlers.SearchParamSet
import com.socrata.cetera.search.DomainFilters.{cnamesFilter, idsFilter, isCustomerDomainFilter}
import com.socrata.cetera.types.{Domain, DomainCnameFieldType, DomainSet}
import com.socrata.cetera.util.LogHelper

trait BaseDomainClient {
  def fetch(id: Int): Option[Domain]

  def findSearchableDomains(
      searchContextCname: Option[String],
      extendedHost: Option[String],
      domainCnames: Option[Set[String]],
      excludeLockedDomains: Boolean,
      user: Option[User],
      requestId: Option[String])
    : (DomainSet, Long)

  def buildCountRequest(
      domainSet: DomainSet,
      searchParams: SearchParamSet,
      user: Option[User],
      requireAuth: Boolean)
  : SearchRequestBuilder
}

class DomainClient(esClient: ElasticSearchClient, coreClient: CoreClient, indexAliasName: String)
  extends BaseDomainClient {

  val logger = LoggerFactory.getLogger(getClass)

  def fetch(id: Int): Option[Domain] = {
    val get = esClient.client.prepareGet(indexAliasName, esDomainType, id.toString)
    logger.info("Elasticsearch request: " + get.request.toString)

    val res = get.execute.actionGet
    Domain(res.getSourceAsString)
  }

  def find(cname: String): (Option[Domain], Long) = {
    val (domains, timing) = find(Set(cname))
    (domains.headOption, timing)
  }

  def find(cnames: Set[String]): (Set[Domain], Long) = {
    if (cnames.isEmpty) {
      (Set.empty, 0L)
    } else {
      val query = QueryBuilders.termsQuery(DomainCnameFieldType.rawFieldName, cnames.toList: _*)

      val search = esClient.client.prepareSearch(indexAliasName).setTypes(esDomainType)
        .setQuery(query)
        .setSize(cnames.size)

      logger.info(LogHelper.formatEsRequest(search))

      val res = search.execute.actionGet
      val timing = res.getTookInMillis

      val domains = res.getHits.hits.flatMap { hit =>
        try {
          Domain(hit.sourceAsString)
        }
        catch {
          case e: Exception =>
            logger.info(e.getMessage)
            None
        }
      }.toSet

      (domains, timing)
    }
  }

  // This method returns a DomainSet including optional search context and list of Domains to search.
  // Each Domain will be visible to the requesting User or anonymous. If a Domain is locked-down and the
  // requesting User is not assigned a role it will be elided.
  // WARN: The search context may come back as None. This is to avoid leaking locked domains through the context.
  //       But if a search context was given this effectively pretends no context was given.
  def removeLockedDomainsForbiddenToUser(
      domainSet: DomainSet,
      user: Option[User],
      requestid: Option[String])
    : DomainSet = {
    val context = domainSet.searchContext
    val extendedHost = domainSet.extendedHost
    val contextLocked = context.exists(_.isLocked)
    val (lockedDomains, unlockedDomains) = domainSet.domains.partition(_.isLocked)

    if (!contextLocked && lockedDomains.isEmpty) {
      domainSet
    } else {
      val loggedInUser = user.map(u => u.copy(authenticatingDomain = extendedHost))
      val userCanViewLockedContext =
        loggedInUser.exists(u => context.exists(c => u.canViewLockedDownCatalog(c.domainId)))
      val viewableContext = context.filter(c => !contextLocked || userCanViewLockedContext)
      loggedInUser match {
        case Some(u) =>
          val viewableLockedDomains = lockedDomains.filter { d => u.canViewLockedDownCatalog(d.domainId) }
          DomainSet(unlockedDomains ++ viewableLockedDomains, viewableContext, extendedHost)
        case None => // user is not logged in and thus can see no locked data
          DomainSet(unlockedDomains, viewableContext, extendedHost)
      }
    }
  }


  // This method gets all of the domains you might care about in one go.  This includes:
  //  - the search context
  //  - the extended host used for authenticating (which needn't be the same as the context)
  //  - the domains.  these may be the domains provided, or if the optional flag to get customer
  //    domains is set and the domains are empty, all customer domains.
  // NOTE: if lock-down is a concern, use the 'findSearchableDomains' method in lieu of this one.
  def findDomainSet( // scalastyle:ignore cyclomatic.complexity
      searchContextCname: Option[String],
      extendedHost: Option[String],
      domainCnames: Option[Set[String]],
      optionallyGetCustomerDomains: Boolean = true)
  : (DomainSet, Long) = {
    // we want to treat an empty list of domains like a None
    val searchDomains = domainCnames.collect { case s: Set[String] if (s.nonEmpty) => s }
    // find all the domains in one go.
    val (foundDomains, timings) = (searchDomains, optionallyGetCustomerDomains) match {
      case (Some(cnames), _) => find(cnames ++ searchContextCname ++ extendedHost)
      case (None, false) => find(Set(searchContextCname, extendedHost).flatten)
      case (None, true) => customerDomainSearch(Set(searchContextCname, extendedHost).flatten)
    }

    // parse out which domains are which
    val searchContextDomain = searchContextCname.flatMap(cname => foundDomains.find(_.domainCname == cname))
    val extendedHostDomain = extendedHost.flatMap(cname => foundDomains.find(_.domainCname == cname))
    val domains = (searchDomains, optionallyGetCustomerDomains) match {
      case (Some(cnames), _) => foundDomains.filter(d => cnames.contains(d.domainCname))
      case (None, false) => Set.empty[Domain]
      case (None, true) => foundDomains.filter(d => d.isCustomerDomain)
    }

    // If any domains were asked for and we can't find it, we have to bail
    val foundDomainCnames = domains.map(_.domainCname)
    searchContextCname.foreach(c => if (searchContextDomain.isEmpty) throw new DomainNotFoundError(c))
    extendedHost.foreach(h => if (extendedHostDomain.isEmpty) throw new DomainNotFoundError(h))
    domainCnames.getOrElse(Set.empty).foreach(d => if (!foundDomainCnames.contains(d)) throw new DomainNotFoundError(d))

    (DomainSet(domains, searchContextDomain, extendedHostDomain), timings)
  }

  def findSearchableDomains(
      searchContextCname: Option[String],
      extendedHost: Option[String],
      domainCnames: Option[Set[String]],
      excludeLockedDomains: Boolean,
      user: Option[User],
      requestId: Option[String])
    : (DomainSet, Long) = {
    val (domainSet, timings) = findDomainSet(searchContextCname, extendedHost, domainCnames)
    if (excludeLockedDomains) {
      val restrictedDomainSet =
        removeLockedDomainsForbiddenToUser(domainSet, user, requestId)
      (restrictedDomainSet, timings)
    } else {
      (domainSet, timings)
    }
  }

  // TODO: handle unlimited domain count with aggregation or scan+scroll query
  // if we get 42 thousand customer domains before addressing this, most of us will probably be millionaires anyway.
  private val customerDomainSearchSize = 42000

  // when query doesn't define domain filter, we assume all customer domains.
  private def customerDomainSearch(additionalDomains: Set[String]): (Set[Domain], Long) = {
    val domainFilter = cnamesFilter(additionalDomains)
    val filter = FilterBuilders.boolFilter().should(isCustomerDomainFilter).should(domainFilter)
    val search = esClient.client.prepareSearch(indexAliasName).setTypes(esDomainType)
      .setQuery(QueryBuilders.filteredQuery(
        QueryBuilders.matchAllQuery(),
        filter)
      )
      .setSize(customerDomainSearchSize)
    logger.info(LogHelper.formatEsRequest(search))

    val res = search.execute.actionGet
    val timing = res.getTookInMillis

    val domains = res.getHits.hits.flatMap { hit =>
      try { Domain(hit.getSourceAsString) }
      catch { case e: Exception =>
        logger.info(e.getMessage)
        None
      }
    }.toSet

    (domains, timing)
  }

  // NOTE: this method will acknowledge all params except 'q' and 'q_internal'
  def buildCountRequest(
      domainSet: DomainSet,
      searchParams: SearchParamSet,
      user: Option[User],
      requireAuth: Boolean)
  : SearchRequestBuilder = {
    val domainFilter = DomainFilters.idsFilter(domainSet.domains.map(_.domainId))
    // NOTE: this only filters by search params that are translated into filters and not
    //       the 'q' param which is translated into a query
    val docFilters = DocumentFilters(forDomainSearch = true).compositeFilter(domainSet, searchParams, user, requireAuth)

    esClient.client.prepareSearch(indexAliasName).setTypes(esDomainType)
      .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), domainFilter))
      .addAggregation(DomainAggregations.domains(docFilters))
      .setSearchType(SearchType.COUNT)
      .setSize(0) // no docs, aggs only
  }
}
