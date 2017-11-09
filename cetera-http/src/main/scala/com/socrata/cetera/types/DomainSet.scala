package com.socrata.cetera.types

// NOTE: Please note that domainBoosts are empty by default. The caller must explicitly create
// a copy of the DomainSet via the `addDomainBoosts` method with the appropriate domainBoosts
// parameter in order for those boosts to have an effect.
case class DomainSet(
    domains: Set[Domain] = Set.empty,
    searchContext: Option[Domain] = None,
    extendedHost: Option[Domain] = None,
    domainBoosts: Map[String, Float] = Map.empty) {

  def addDomainBoosts(boosts: Map[String, Float]): DomainSet = copy(domainBoosts = boosts)

  def idMap: Map[Int, Domain] = {
    val allDomains = domains ++ searchContext ++ extendedHost
    allDomains.map(d => d.id -> d).toMap
  }

  def idCnameMap: Map[Int, String] = {
    val allDomains = domains ++ searchContext ++ extendedHost
    allDomains.map(d => d.id -> d.cname).toMap
  }

  def cnameIdMap: Map[String, Int] = idCnameMap.map(_.swap)

  def domainIdBoosts: Map[Int, Float] = {
    val idMap = cnameIdMap
    domainBoosts.flatMap { case (cname: String, weight: Float) =>
      idMap.get(cname).map(id => id -> weight)
    }
  }

  /**
    * @param predicate to determine which partition a Domain should go into
    * @return A tuple where the first element is all the ids of the domains that the predicate was
    *         true for and the second is all the ids of the domains that the predicate is false for
    */
  private def partitionIds(predicate: Domain => Boolean): (Set[Int], Set[Int]) = {
    val (trueDomains, falseDomains) = domains.partition(predicate)
    (trueDomains.map(_.id), falseDomains.map(_.id))
  }

  val contextIsModerated = searchContext.exists(_.moderationEnabled)
  val contextHasRoutingApproval = searchContext.exists(_.routingApprovalEnabled)
  val allIds = domains.map(_.id)
  val (moderationEnabledIds, moderationDisabledIds) = partitionIds(_.moderationEnabled)
  val (raEnabledIds, raDisabledIds) = partitionIds(_.routingApprovalEnabled)
  val (hasFontanaApprovals, lacksFontanaApprovals) = partitionIds(_.hasFontanaApprovals)
}
