package com.socrata.cetera.search

import com.rojoma.json.v3.codec.JsonDecode
import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.CompactJsonWriter
import org.mockserver.model.HttpRequest.request
import org.mockserver.model.HttpResponse.response
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, ShouldMatchers, WordSpec}

import com.socrata.cetera._
import com.socrata.cetera.auth.User
import com.socrata.cetera.errors.{DomainNotFoundError, UnauthorizedError}
import com.socrata.cetera.types.DomainSet

// Please see https://github.com/socrata/cetera/blob/master/cetera-http/src/test/resources/domains.tsv
// if you have any questions about which domains are being used in these tests
class DomainClientSpec extends WordSpec with ShouldMatchers with TestESData
  with BeforeAndAfterAll with BeforeAndAfterEach {

  val unlockedCustomerDomain = domains(0)
  val unlockedNonCustomerDomain = domains(1)
  val unlockedCustomerDomain2 = domains(2)
  val lockedNonCustomerDomain = domains(6)
  val apiLockedDomain = domains(7)
  val lockedCustomerDomain = domains(8)
  val unlockedDomains = domains.filter(d => !d.isLocked)
  val unlockedCustomerDomains = domains.filter(d => d.isCustomerDomain && !d.isLocked)

  override def beforeEach(): Unit = mockServer.reset()

  override protected def beforeAll(): Unit = bootstrapData()

  override protected def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
    mockServer.stop(true)
    httpClient.close()
  }

  def prepUserAuth(role: String): Option[User] = {
    val userBody =
      j"""{
        "id" : "boo-bear",
        "roleName" : $role
      }"""

    mockServer.when(
      request()
        .withMethod("GET")
        .withPath("/users/boo-bear")
    ).respond(
      response()
        .withStatusCode(200)
        .withHeader("Content-Type", "application/json; charset=utf-8")
        .withBody(CompactJsonWriter.toString(userBody))
    )
    JsonDecode.fromJValue[User](userBody).fold(_ => None, Some(_))
  }

  "find" should {
    "return the domain if it exists : petercetera.net" in {
      val expectedDomain = domains(0)
      val (actualDomain, _) = domainClient.find("petercetera.net")
      actualDomain.get should be(expectedDomain)
    }

    "return the domain if it exists when searching by any of its aliases: blue.org" in {
      val blueDomain = domains(2)
      val (actualDomainViaTealAlias, _) = domainClient.find("teal.org")
      val (actualDomainViaZaffreAlias, _) = domainClient.find("zaffre.org")

      actualDomainViaTealAlias should be(Some(blueDomain))
      actualDomainViaZaffreAlias should be(Some(blueDomain))
    }

    "return the domain if it exists : opendata-demo.socrata.com" in {
      val expectedDomain = domains(1)
      val (actualDomain, _) = domainClient.find("opendata-demo.socrata.com")
      actualDomain.get should be(expectedDomain)
    }

    "return None if the domain does not exist" in {
      val expectedDomain = None
      val (actualDomain, _) = domainClient.find("hellcat.com")
      actualDomain should be(expectedDomain)
    }

    "return None if searching for blank string" in {
      val expectedDomain = None
      val (actualDomain, _) = domainClient.find("")
      actualDomain should be(expectedDomain)
    }

    "return only domains with an exact match" in {
      val expectedDomain = domains(4)
      val (actualDomain, _) = domainClient.find("dylan.demo.socrata.com")
      actualDomain shouldBe Some(expectedDomain)
    }
  }

  "the findDomainSet method" should {
    "throw if the search context can't be found" in {
      intercept[DomainNotFoundError] {
        domainClient.findDomainSet(Some("iamnotarealcontext.wat"), None, None)
      }
    }

    "throw if the extended host can't be found" in {
      intercept[DomainNotFoundError] {
        domainClient.findDomainSet(None, Some("iamnotarealhost.wat"), None)
      }
    }

    "throw if any domain can't be found" in {
      intercept[DomainNotFoundError] {
        domainClient.findDomainSet(None, None, Some(Set("petercetera.net", "iamnotarealdomain.wat")))
      }
    }

    "return basically nothing if Nones are given and not opting to get customer domains and not take any search time doing it" in {
      val (actualDset, timing) = domainClient.findDomainSet(None, None, None, false)
      actualDset.domains should be('empty)
      actualDset.searchContext should be(None)
      actualDset.extendedHost should be(None)
      timing should be(0)
    }

    "return basically nothing if Nones/empty Sets are given and not opting to get customer domains and not take any search time doing it" in {
      val (actualDset, timing) = domainClient.findDomainSet(None, None, Some(Set.empty), false)
      actualDset.domains should be('empty)
      actualDset.searchContext should be(None)
      actualDset.extendedHost should be(None)
      timing should be(0)
    }

    "return customer domains only if nothing is given and opting to get customer domains" in {
      val expectedDomains = domains.filter(_.isCustomerDomain)
      val (actualDset, _) = domainClient.findDomainSet(None, None, Some(Set.empty), true)
      actualDset.domains should contain theSameElementsAs(expectedDomains)
      actualDset.searchContext should be(None)
      actualDset.extendedHost should be(None)
    }

    "return all the things if context and extend host are given but no domains are given and opting to get customer domains" in {
      val expectedDomains = domains.filter(_.isCustomerDomain)
      val expectedContext = domains(0)
      val expectedHost = domains(0)
      val (actualDset, _) = domainClient.findDomainSet(Some(expectedContext.cname), Some(expectedHost.cname), Some(Set.empty), true)
      actualDset.domains should contain theSameElementsAs(expectedDomains)
      actualDset.searchContext.get should be(expectedContext)
      actualDset.extendedHost.get should be(expectedHost)
    }

    "return most of the things if context and extend host are given but no domains are given and not opting to get customer domains" in {
      val expectedContext = domains(0)
      val expectedHost = domains(2)
      val (actualDset, _) = domainClient.findDomainSet(Some(expectedContext.cname), Some(expectedHost.cname), Some(Set.empty), false)
      actualDset.domains should be('empty)
      actualDset.searchContext.get should be(expectedContext)
      actualDset.extendedHost.get should be(expectedHost)
    }

    "return all the things if context and extend host and domains are given and not opting to get customer domains" in {
      val expectedDomains = Set(domains(0), domains(2), domains(3))
      val expectedContext = domains(0)
      val expectedHost = domains(2)
      val (actualDset, _) = domainClient.findDomainSet(Some(expectedContext.cname), Some(expectedHost.cname), Some(expectedDomains.map(_.cname)), true)
      actualDset.domains should contain theSameElementsAs(expectedDomains)
      actualDset.searchContext.get should be(expectedContext)
      actualDset.extendedHost.get should be(expectedHost)
    }
  }

  "finding the context from findSearchableDomains when no cnames are given (and we rely on customer domain search)" should {
    "return the context if it's unlocked and no auth is given" in {
      val (domainSet, _) = domainClient.findSearchableDomains(Some(unlockedCustomerDomain.cname), None, None, true, None, None)
      domainSet.searchContext.get should be(unlockedCustomerDomain)
    }

    "return None for the context if it's locked and no auth is given" in {
      val (domainSet, _) = domainClient.findSearchableDomains(Some(lockedCustomerDomain.cname), None, None, true, None, None)
      domainSet.searchContext should be(None)
    }

    "return the context if it's locked and auth is given for the context" in {
      val user = prepUserAuth("editor")
      val (domainSet, _) = domainClient.findSearchableDomains(
        Some(lockedCustomerDomain.cname), Some(lockedCustomerDomain.cname), None, true, user, None)

      domainSet.searchContext.get should be(lockedCustomerDomain)
    }

    "return None for the context if it's locked and auth is given, but it ain't for the context" in {
      val user = prepUserAuth("editor")
      val authingDomain = unlockedCustomerDomain2
      val (domainSet, _) = domainClient.findSearchableDomains(
        Some(lockedCustomerDomain.cname), Some(authingDomain.cname), None, true, user, None)

      domainSet.searchContext should be(None)
    }
  }

  "finding the extended host from findSearchableDomains when no cnames are given (and we rely on customer domain search)" should {
    "return the extended host regardless of state if auth isn't given" in {
      val (lockedCustomerDomainSet, _) = domainClient.findSearchableDomains(
        None, Some(lockedCustomerDomain.cname), None, true, None, None)
      val (lockedNonCustomerDomainSet, _) = domainClient.findSearchableDomains(
        None, Some(lockedNonCustomerDomain.cname), None, true, None, None)
      val (unlockedCustomerDomainSet, _) = domainClient.findSearchableDomains(
        None, Some(unlockedCustomerDomain.cname), None, true, None, None)
      val (unlockedNonCustomerDomainSet, _) = domainClient.findSearchableDomains(
        None, Some(unlockedNonCustomerDomain.cname), None, true, None, None)

      lockedCustomerDomainSet.extendedHost.get should be(lockedCustomerDomain)
      lockedNonCustomerDomainSet.extendedHost.get should be(lockedNonCustomerDomain)
      unlockedCustomerDomainSet.extendedHost.get should be(unlockedCustomerDomain)
      unlockedNonCustomerDomainSet.extendedHost.get should be(unlockedNonCustomerDomain)
    }

    "return the extended host regardless of state if auth is given" in {
      val user = prepUserAuth("administrator")

      val (lockedCustomerDomainSet, _) = domainClient.findSearchableDomains(
        None, Some(lockedCustomerDomain.cname), None, true, user, None)
      val (lockedNonCustomerDomainSet, _) = domainClient.findSearchableDomains(
        None, Some(lockedNonCustomerDomain.cname), None, true, user, None)
      val (unlockedCustomerDomainSet, _) = domainClient.findSearchableDomains(
        None, Some(unlockedCustomerDomain.cname), None, true, user, None)
      val (unlockedNonCustomerDomainSet, _) = domainClient.findSearchableDomains(
        None, Some(unlockedNonCustomerDomain.cname), None, true, user, None)

      lockedCustomerDomainSet.extendedHost.get should be(lockedCustomerDomain)
      lockedNonCustomerDomainSet.extendedHost.get should be(lockedNonCustomerDomain)
      unlockedCustomerDomainSet.extendedHost.get should be(unlockedCustomerDomain)
      unlockedNonCustomerDomainSet.extendedHost.get should be(unlockedNonCustomerDomain)
    }
  }

  "finding the domains to search from findSearchableDomains when no cnames are given (and we rely on customer domain search)" should {
    "return only unlocked customer domains if auth isn't given" in {
      val (domainSet, _) = domainClient.findSearchableDomains(None, None, None, true, None, None)
      domainSet.domains should contain theSameElementsAs(unlockedCustomerDomains)
    }

    "return unlocked customer domains + locked customer domains if auth is given for the locked domain" in {
      val expectedDomains = unlockedCustomerDomains ++ Some(lockedCustomerDomain)
      val user = prepUserAuth("editor")
      val (domainSet, _) = domainClient.findSearchableDomains(None, Some(lockedCustomerDomain.cname), None, true, user, None)

      domainSet.domains should contain theSameElementsAs(expectedDomains)
    }

    "return only unlocked customer domains if auth is given but it ain't for any locked domain" in {
      val user = prepUserAuth("editor")
      val (domainSet, _) = domainClient.findSearchableDomains(None, Some(lockedNonCustomerDomain.cname), None, true, user, None)

      domainSet.domains should contain theSameElementsAs(unlockedCustomerDomains)
    }
  }

  "finding the context from findSearchableDomains when cnames are given" should {
    val domainsToSearch = Some(domains.map(_.cname).toSet)

    "return the context if it's unlocked and no auth is given" in {
      val (domainSet, _) = domainClient.findSearchableDomains(Some(unlockedCustomerDomain.cname), None, domainsToSearch, true, None, None)
      domainSet.searchContext.get should be(unlockedCustomerDomain)
    }

    "return None for the context if it's locked and no auth is given" in {
      val (domainSet, _) = domainClient.findSearchableDomains(Some(lockedCustomerDomain.cname), None, domainsToSearch, true, None, None)
      domainSet.searchContext should be(None)
    }

    "return the context if it's locked and auth is given for the context" in {
      val user = prepUserAuth("editor")
      val (domainSet, _) = domainClient.findSearchableDomains(
        Some(lockedCustomerDomain.cname), Some(lockedCustomerDomain.cname), domainsToSearch, true, user, None)

      domainSet.searchContext.get should be(lockedCustomerDomain)
    }

    "return None for the context if it's locked and auth is given, but it ain't for the context" in {
      val user = prepUserAuth("editor")
      val authingDomain = unlockedCustomerDomain2
      val (domainSet, _) = domainClient.findSearchableDomains(
        Some(lockedCustomerDomain.cname), Some(authingDomain.cname), domainsToSearch, true, user, None)

      domainSet.searchContext should be(None)
    }
  }

  "finding the extended host from findSearchableDomains when cnames are given" should {
    val domainsToSearch = Some(domains.map(_.cname).toSet)

    "return the extended host regardless of state if auth isn't given" in {
      val (lockedCustomerDomainSet, _) = domainClient.findSearchableDomains(
        None, Some(lockedCustomerDomain.cname), domainsToSearch, true, None, None)
      val (lockedNonCustomerDomainSet, _) = domainClient.findSearchableDomains(
        None, Some(lockedNonCustomerDomain.cname), domainsToSearch, true, None, None)
      val (unlockedCustomerDomainSet, _) = domainClient.findSearchableDomains(
        None, Some(unlockedCustomerDomain.cname), domainsToSearch, true, None, None)
      val (unlockedNonCustomerDomainSet, _) = domainClient.findSearchableDomains(
        None, Some(unlockedNonCustomerDomain.cname), domainsToSearch, true, None, None)

      lockedCustomerDomainSet.extendedHost.get should be(lockedCustomerDomain)
      lockedNonCustomerDomainSet.extendedHost.get should be(lockedNonCustomerDomain)
      unlockedCustomerDomainSet.extendedHost.get should be(unlockedCustomerDomain)
      unlockedNonCustomerDomainSet.extendedHost.get should be(unlockedNonCustomerDomain)
    }

    "return the extended host regardless of state if auth is given" in {
      val user = prepUserAuth("administrator")

      val (lockedCustomerDomainSet, _) = domainClient.findSearchableDomains(
        None, Some(lockedCustomerDomain.cname), domainsToSearch, true, user, None)
      val (lockedNonCustomerDomainSet, _) = domainClient.findSearchableDomains(
        None, Some(lockedNonCustomerDomain.cname), domainsToSearch, true, user, None)
      val (unlockedCustomerDomainSet, _) = domainClient.findSearchableDomains(
        None, Some(unlockedCustomerDomain.cname), domainsToSearch, true, user, None)
      val (unlockedNonCustomerDomainSet, _) = domainClient.findSearchableDomains(
        None, Some(unlockedNonCustomerDomain.cname), domainsToSearch, true, user, None)

      lockedCustomerDomainSet.extendedHost.get should be(lockedCustomerDomain)
      lockedNonCustomerDomainSet.extendedHost.get should be(lockedNonCustomerDomain)
      unlockedCustomerDomainSet.extendedHost.get should be(unlockedCustomerDomain)
      unlockedNonCustomerDomainSet.extendedHost.get should be(unlockedNonCustomerDomain)
    }
  }

  "finding the domains to search from findSearchableDomains when cnames are given" should {
    val domainsToSearch = Some(domains.map(_.cname).toSet)

    "return only unlocked domains if auth isn't given" in {
      val (domainSet, _) = domainClient.findSearchableDomains(None, None, domainsToSearch, true, None, None)
      domainSet.domains should contain theSameElementsAs(unlockedDomains)
    }

    "return unlocked domains + locked domains if auth is given for the locked domain" in {
      val expectedDomains = unlockedDomains ++ Some(lockedCustomerDomain)
      val user = prepUserAuth("editor")
      val (domainSet, _) = domainClient.findSearchableDomains(None, Some(lockedCustomerDomain.cname), domainsToSearch, true, user, None)

      domainSet.domains should contain theSameElementsAs(expectedDomains)
    }

    "return only unlocked domains if auth is given but it ain't for any locked domain" in {
      val user = prepUserAuth("editor")
      val (domainSet, _) = domainClient.findSearchableDomains(None, Some(unlockedCustomerDomain.cname), domainsToSearch, true, user, None)

      domainSet.domains should contain theSameElementsAs(unlockedDomains)
    }
  }

  "findSearchableDomains" should {
    "return the context, extended host and domains even if they are all the same thing: opendata-demo.socrata.com" in {
      val domain = domains(1)
      val (domainSet, _) = domainClient.findSearchableDomains(
        Some(domain.cname), Some(domain.cname), Some(Set(domain.cname)), true, None, None)

      domainSet.searchContext.get should be(domain)
      domainSet.extendedHost.get should be(domain)
      domainSet.domains should be(Set(domain))
    }

    "return the context, extended host and domains even if they are all specified by aliases" in {
      val err = new Exception("domains are not properly set up with aliases")
      val context = domains(0).aliases.map(_.head).getOrElse(throw err)
      val xhost = domains(1).aliases.map(_.head).getOrElse(throw err)
      val doms = (0 to 2).map(i => domains(i).aliases.map(_.head).getOrElse(throw err)).toSet
      val (domainSet, _) = domainClient.findSearchableDomains(Some(context), Some(xhost), Some(doms), true, None, None)

      domainSet.searchContext should be(Some(domains(0)))
      domainSet.extendedHost should be(Some(domains(1)))
      domainSet.domains should be(Set(domains(0), domains(1), domains(2)))
    }

    "throw DomainNotFound exception when searchContext is missing" in {
      intercept[DomainNotFoundError] {
        domainClient.findSearchableDomains(Some("iamnotarealdomain.wat"), None, None, true, None, None)
      }
    }

    "throw DomainNotFound exception when searchContext is missing even if domains are found" in {
      intercept[DomainNotFoundError] {
        domainClient.findSearchableDomains(Some("iamnotarealdomain.wat"), None,
          Some(Set("dylan.demo.socrata.com")), true, None, None)
      }
    }
  }

  "removeLockedDomainsFromUnauthorizedUsers" should {
    "return None for a locked context if no user is given" in {
      val withoutDomains = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(searchContext = Some(lockedNonCustomerDomain)), None, None)
      withoutDomains should be(DomainSet())

      val withDomains = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(Set(domains(1)), Some(lockedNonCustomerDomain)), None, None)
      withDomains should be(DomainSet(domains = Set(domains(1))))
    }

    "throw for a locked context if the user cannot be converted to an AuthedUser b/c no extendedHost is given" in {
      // this is possible to test code, but would never come about in src, since to have a user implies
      // core had an extendedHost. And if we failed to find the extendHost, a DomainNotFoundError would be thrown first
      val user = User("lazy-bear", roleName = None, rights = None, flags = None)
      intercept[UnauthorizedError] {
        domainClient.removeLockedDomainsForbiddenToUser(
          DomainSet(searchContext = Some(lockedNonCustomerDomain), extendedHost = None), Some(user), None)
      }
    }

    "return None for a locked context if the user has no role" in {
      val user = User("lazy-bear", roleName = None, rights = None, flags = None)

      val withoutDomains = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(searchContext = Some(lockedCustomerDomain), extendedHost = Some(domains(0))), Some(user), None)
      withoutDomains should be(DomainSet(extendedHost = Some(domains(0))))

      val withDomains = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(Set(domains(1)), Some(lockedCustomerDomain), extendedHost = Some(domains(0))), Some(user), None)
      withDomains should be(DomainSet(domains = Set(domains(1)), extendedHost = Some(domains(0))))
    }

    "return the locked down context if the user is logged in and has a role" in {
      val user = prepUserAuth("publisher")
      val res = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(Set(lockedNonCustomerDomain), Some(lockedNonCustomerDomain), Some(lockedNonCustomerDomain)), user, None)
      res should be(DomainSet(Set(lockedNonCustomerDomain), Some(lockedNonCustomerDomain), Some(lockedNonCustomerDomain)))
    }

    "remove locked domains if no user is given" in {
      val relevantDomains = Set(unlockedCustomerDomain, lockedNonCustomerDomain)
      val withoutContext = domainClient.removeLockedDomainsForbiddenToUser(DomainSet(domains = relevantDomains), None, None)
      withoutContext should be(DomainSet(domains = Set(unlockedCustomerDomain)))

      val withContext = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(relevantDomains, Some(unlockedCustomerDomain), Some(unlockedCustomerDomain)), None, None)
      withContext should be(DomainSet(Set(unlockedCustomerDomain), Some(unlockedCustomerDomain), Some(unlockedCustomerDomain)))
    }

    "throw for locked domains if the user cannot be converted to an AuthedUser b/c no extendedHost is given" in {
      // this is possible to test code, but would never come about in src, since to have a user implies
      // core had an extendedHost. And if we failed to find the extendHost, a DomainNotFoundError would be thrown first
      val relevantDomains = Set(unlockedCustomerDomain2, apiLockedDomain)
      val user = User("lazy-bear", roleName = None, rights = None, flags = None)
      intercept[UnauthorizedError] {
        domainClient.removeLockedDomainsForbiddenToUser(
          DomainSet(relevantDomains, Some(apiLockedDomain), extendedHost = None), Some(user), None)
      }
    }

    "remove locked domains if the user has no role" in {
      val user = User("lazy-bear", roleName = None, rights = None, flags = None)
      val relevantDomains = Set(unlockedCustomerDomain2, apiLockedDomain)

      val res = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(relevantDomains, Some(apiLockedDomain), extendedHost = Some(unlockedCustomerDomain2)), Some(user), None)
      res should be(DomainSet(domains = Set(unlockedCustomerDomain2), extendedHost = Some(unlockedCustomerDomain2)))
    }

    "return the locked down domain if the user is logged in and has a role on that domain" in {
      val user = prepUserAuth("publisher")
      val relevantDomains = Set(unlockedCustomerDomain2, apiLockedDomain)
      val res = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(relevantDomains, None, extendedHost = Some(apiLockedDomain)), user, None)
      res should be(DomainSet(relevantDomains, None, extendedHost = Some(apiLockedDomain)))
    }

    "always return the extendhost - since this is used solely for auth" in {
      val noUser = None
      val roleLessUser = prepUserAuth("")
      val roledUser = prepUserAuth("publisher")
      val extendedHost = Some(apiLockedDomain)
      val resForNoUser = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(Set.empty, None, extendedHost), noUser, None)
      val resForRoleLessUser = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(Set.empty, None, extendedHost), roleLessUser, None)
      val resForRoledUser = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(Set.empty, None, extendedHost), roledUser, None)

      resForNoUser.extendedHost should be(extendedHost)
      resForRoleLessUser.extendedHost should be(extendedHost)
      resForRoledUser.extendedHost should be(extendedHost)
    }

    "return all the things if nothing is locked down" in {
      val noContextNoDomains = domainClient.removeLockedDomainsForbiddenToUser(DomainSet(), None, None)
      noContextNoDomains should be(DomainSet())

      val noDomains = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(searchContext = Some(unlockedCustomerDomain)), None, None)
      noDomains should be(DomainSet(searchContext = Some(unlockedCustomerDomain)))

      val noContext = domainClient.removeLockedDomainsForbiddenToUser(DomainSet(domains = Set(unlockedNonCustomerDomain)), None, None)
      noContext should be(DomainSet(domains = Set(unlockedNonCustomerDomain)))

      val bothUnsharedContext = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(Set(unlockedCustomerDomain2), Some(unlockedCustomerDomain)), None, None)
      bothUnsharedContext should be(DomainSet(Set(unlockedCustomerDomain2), Some(unlockedCustomerDomain)))

      val bothSharedContext = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(Set(unlockedNonCustomerDomain, unlockedCustomerDomain2), Some(unlockedNonCustomerDomain)), None, None)
      bothSharedContext should be(DomainSet(Set(unlockedNonCustomerDomain, unlockedCustomerDomain2), Some(unlockedNonCustomerDomain)))

    }
  }
}
