package com.socrata.cetera.types

import org.scalatest._

import com.socrata.cetera.TestESData

class DomainSpec extends WordSpec with ShouldMatchers with TestESData with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = bootstrapData()

  override protected def afterAll(): Unit = removeBootstrapData()

  "domain 0" should {
    "have the expected state" in {
      val dom = domains(0)
      dom.domainId should be(0)
      dom.domainCname should be("petercetera.net")
      dom.siteTitle.get should be("Temporary URI")
      dom.organization.get should be("org")
      dom.isCustomerDomain should be(true)
      dom.moderationEnabled should be(false)
      dom.routingApprovalEnabled should be(false)
      dom.isLocked should be(false)
      dom.aliasSet should contain theSameElementsAs(Set("petercetera.net", "pc.net"))

      // NOTE: domain 0 federates its data into domains 1 and 2 and accepts data from domain 2
    }
  }

  "domain 1" should {
    "have the expected state" in {
      val dom = domains(1)
      dom.domainId should be(1)
      dom.domainCname should be("opendata-demo.socrata.com")
      dom.siteTitle.get should be("And other things")
      dom.organization.get should be("org")
      dom.isCustomerDomain should be(false)
      dom.moderationEnabled should be(true)
      dom.routingApprovalEnabled should be(false)
      dom.isLocked should be(false)
      dom.aliasSet should contain theSameElementsAs(Set("opendata.socrata.com", "opendata-demo.socrata.com"))

      // NOTE: domain 1 accepts data from domain 0
    }
  }

  "domain 2" should {
    "have the expected state" in {
      val dom = domains(2)
      dom.domainId should be(2)
      dom.domainCname should be("blue.org")
      dom.siteTitle.get should be("Fame and Fortune")
      dom.organization.get should be("org")
      dom.isCustomerDomain should be(true)
      dom.moderationEnabled should be(false)
      dom.routingApprovalEnabled should be(true)
      dom.isLocked should be(false)
      dom.aliasSet should contain theSameElementsAs(Set("teal.org", "zaffre.org", "blue.org"))

      // NOTE: domain 2 federates its data into domains 0 and 3 and accepts data from domain 0
    }
  }

  "domain 3" should {
    "have the expected state" in {
      val dom = domains(3)
      dom.domainId should be(3)
      dom.domainCname should be("annabelle.island.net")
      dom.siteTitle.get should be("Socrata Demo")
      dom.organization.get should be("org")
      dom.isCustomerDomain should be(true)
      dom.moderationEnabled should be(true)
      dom.routingApprovalEnabled should be(true)
      dom.isLocked should be(false)
      dom.aliasSet should contain theSameElementsAs(Set("annabelle.island.net"))

      // NOTE: domain 3 accepts data from domain 2
    }
  }

  "domain 4" should {
    "have the expected state" in {
      val dom = domains(4)
      dom.domainId should be(4)
      dom.domainCname should be("dylan.demo.socrata.com")
      dom.siteTitle.get should be("Skid Row")
      dom.organization.get should be("Hair Metal Bands")
      dom.isCustomerDomain should be(true)
      dom.moderationEnabled should be(false)
      dom.routingApprovalEnabled should be(true)
      dom.isLocked should be(false)
      dom.aliasSet should contain theSameElementsAs(Set("dylan.demo.socrata.com"))
    }
  }

  "domain 5" should {
    "have the expected state" in {
      val dom = domains(5)
      dom.domainId should be(5)
      dom.domainCname should be("dylan2.demo.socrata.com")
      dom.siteTitle.get should be("Mötley Crüe")
      dom.organization.get should be("Hair Metal Bands")
      dom.isCustomerDomain should be(false)
      dom.moderationEnabled should be(false)
      dom.routingApprovalEnabled should be(false)
      dom.isLocked should be(false)
      dom.aliasSet should contain theSameElementsAs(Set("dylan2.demo.socrata.com"))
    }
  }

  "domain 6" should {
    "have the expected state" in {
      val dom = domains(6)
      dom.domainId should be(6)
      dom.domainCname should be("locked.demo.com")
      dom.siteTitle.get should be("Locked Down")
      dom.organization.get should be("org")
      dom.isCustomerDomain should be(false)
      dom.moderationEnabled should be(true)
      dom.routingApprovalEnabled should be(true)
      dom.isLocked should be(true)
      dom.aliasSet should contain theSameElementsAs(Set("locked.demo.com"))
    }
  }

  "domain 7" should {
    "have the expected state" in {
      val dom = domains(7)
      dom.domainId should be(7)
      dom.domainCname should be("api.locked.demo.com")
      dom.siteTitle.get should be("Api Locked Down")
      dom.organization.get should be("org")
      dom.isCustomerDomain should be(true)
      dom.moderationEnabled should be(true)
      dom.routingApprovalEnabled should be(false)
      dom.isLocked should be(true)
      dom.aliasSet should contain theSameElementsAs(Set("api.locked.demo.com"))
    }
  }

  "domain 8" should {
    "have the expected state" in {
      val dom = domains(8)
      dom.domainId should be(8)
      dom.domainCname should be("double.locked.demo.com")
      dom.siteTitle.get should be("Doubly Locked Down")
      dom.organization.get should be("org")
      dom.isCustomerDomain should be(true)
      dom.moderationEnabled should be(true)
      dom.routingApprovalEnabled should be(true)
      dom.isLocked should be(true)
      dom.aliasSet should contain theSameElementsAs(Set("double.locked.demo.com"))
    }
  }

  "domain 9" should {
    "have the expected state" in {
      val dom = domains(9)
      dom.domainId should be(9)
      dom.domainCname should be("robert.demo.socrata.com")
      dom.siteTitle.get should be("Robert's Demo Site")
      dom.organization.get should be("Awesometown USA")
      dom.isCustomerDomain should be(true)
      dom.moderationEnabled should be(false)
      dom.routingApprovalEnabled should be(false)
      dom.isLocked should be(false)
      dom.aliasSet should contain theSameElementsAs(Set("robert.demo.socrata.com"))
    }
  }
}

