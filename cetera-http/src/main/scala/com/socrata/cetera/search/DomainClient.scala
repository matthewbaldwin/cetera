package com.socrata.cetera.search

import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.index.query.QueryBuilders._
import org.slf4j.LoggerFactory

import com.socrata.cetera._
import com.socrata.cetera.auth.{AuthedUser, CoreClient, User}
import com.socrata.cetera.errors.DomainNotFoundError
import com.socrata.cetera.handlers.SearchParamSet
import com.socrata.cetera.search.DomainQueries.{cnamesQuery, idQuery, isCustomerDomainQuery}
import com.socrata.cetera.types.{CnamesFieldType, Domain, DomainSet}
import com.socrata.cetera.util.LogHelper

class DomainClient(esClient: ElasticSearchClient, coreClient: CoreClient, indexAliasName: String) {

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
      val query = termsQuery(CnamesFieldType.fieldName, cnames.toList: _*)

      val search = esClient.client.prepareSearch(indexAliasName)
        .setTypes(esDomainType)
        .setQuery(query)
        .setSize(cnames.size)

      logger.info(LogHelper.formatEsRequest(search))

      val res = search.execute.actionGet
      val timing = res.getTookInMillis

      val domains = res.getHits.getHits.flatMap { hit =>
        try {
          Domain(hit.getSourceAsString)
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
      user match {
        case Some(u) =>
          val authedUser = u.convertToAuthedUser(extendedHost)
          val viewableContext = context.filter(c => !c.isLocked || authedUser.canViewLockedDownCatalog(c.domainId))
          val viewableLockedDomains = lockedDomains.filter(d => authedUser.canViewLockedDownCatalog(d.domainId))
          DomainSet(unlockedDomains ++ viewableLockedDomains, viewableContext, extendedHost)
        case None => DomainSet(unlockedDomains, context.filterNot(_.isLocked), extendedHost)
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

    def findDomainFromCname(cname: String): Domain =
      foundDomains.find(_.aliasSet.contains(cname)).getOrElse(throw new DomainNotFoundError(cname))

    // parse out which domains are which and throw if we couldn't find any given domain
    val searchContextDomain = searchContextCname.map(findDomainFromCname)
    val extendedHostDomain = extendedHost.map(findDomainFromCname)
    val domains = (searchDomains, optionallyGetCustomerDomains) match {
      case (Some(cnames), _) => cnames.map(findDomainFromCname)
      case (None, false) => Set.empty[Domain]
      case (None, true) => foundDomains.filter(d => d.isCustomerDomain)
    }

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

  // when query doesn't define a domain set, we assume all customer domains.
  private def customerDomainSearch(additionalDomains: Set[String]): (Set[Domain], Long) = {
    val domainQuery = boolQuery()
      .should(isCustomerDomainQuery)
      .should(cnamesQuery(additionalDomains))

    val search = esClient.client.prepareSearch(indexAliasName)
      .setTypes(esDomainType)
      .setQuery(boolQuery().filter(domainQuery))
      .setSize(DomainClient.MaxDomainsCount)

    logger.info(LogHelper.formatEsRequest(search))

    val res = search.execute.actionGet
    val timing = res.getTookInMillis

    val domains = res.getHits.getHits.flatMap { hit =>
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
      user: Option[AuthedUser])
    : SearchRequestBuilder = {
    val domainQuery = idQuery(domainSet.domains.map(_.domainId))
    // NOTE: this limits the selection set based on facet constraints, but pays no attention to the
    // 'q' parameter
    val docQuery = DocumentQuery(forDomainSearch = true).compositeQuery(domainSet, searchParams, user)

    esClient.client.prepareSearch(indexAliasName)
      .setTypes(esDomainType)
      .setQuery(boolQuery().must(matchAllQuery()).filter(domainQuery))
      .addAggregation(DomainAggregations.domains(docQuery, DomainClient.MaxDomainsCount))
      .setSize(0)
  }
}

object DomainClient {
  // NOTE: rather than incurring the cost of an extra query to ES when we need a size parameter corresponding to the
  // number of domains, we keep this constant, which should be a reasonable upper bound for the number of domains
  val MaxDomainsCount = 10000
}
