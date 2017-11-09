package com.socrata.cetera.types

import org.scalatest._

import com.socrata.cetera.{TestESClient, TestESData}

class DocumentSpec extends WordSpec with ShouldMatchers with TestESData with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = bootstrapData()

  override protected def afterAll(): Unit = removeBootstrapData()

  def verifyVmApproved(doc: Document): Unit = {
    doc.isVmApproved should be(true)
    doc.isVmRejected should be(false)
    doc.isVmPending should be(false)
  }

  def verifyVmRejected(doc: Document): Unit = {
    doc.isVmApproved should be(false)
    doc.isVmRejected should be(true)
    doc.isVmPending should be(false)
  }

  def verifyVmPending(doc: Document): Unit = {
    doc.isVmApproved should be(false)
    doc.isVmRejected should be(false)
    doc.isVmPending should be(true)
  }

  def verifyRaApproved(doc: Document, domainId: Int): Unit = {
    doc.isRaApproved(domainId) should be(true)
    doc.isRaRejected(domainId) should be(false)
    doc.isRaPending(domainId) should be(false)
  }

  def verifyRaRejected(doc: Document, domainId: Int): Unit = {
    doc.isRaApproved(domainId) should be(false)
    doc.isRaRejected(domainId) should be(true)
    doc.isRaPending(domainId) should be(false)
  }

  def verifyRaPending(doc: Document, domainId: Int): Unit = {
    doc.isRaApproved(domainId) should be(false)
    doc.isRaRejected(domainId) should be(false)
    doc.isRaPending(domainId) should be(true)
  }

  def verifyRaMissing(doc: Document, domainId: Int): Unit = {
    doc.isRaApproved(domainId) should be(false)
    doc.isRaRejected(domainId) should be(false)
    doc.isRaPending(domainId) should be(false)
  }

  "domain 0 is a customer domain with neither VM or RA (but uses fontana_approvals) and that federates into domain 2, which has RA" should {
    "have the expected statuses for d0-v0" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d0-v0").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("robin-hood") should be(true)
      doc.isFontanaApproved should be(true)
      verifyRaPending(doc, 2)
    }

    "have the expected statuses for d0-v1" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d0-v1").get
      doc.isPublic should be(false)
      doc.isPublished should be(false)
      doc.isStory should be(true)
      doc.isHiddenFromCatalog should be(true)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("robin-hood") should be(true)
      doc.approvals should be(Some(List.empty))
      verifyRaApproved(doc, 2)
    }

    "have the expected statuses for d0-v2" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d0-v2").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(true)
      doc.isSharedOrOwned("robin-hood") should be(true)
      doc.isFontanaApproved should be(true)
      verifyRaRejected(doc, 2)
    }

    "have the expected statuses for d0-v3" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d0-v3").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(true)
      doc.isSharedOrOwned("robin-hood") should be(true)
      doc.isFontanaApproved should be(true)
      verifyRaRejected(doc, 2)
    }

    "have the expected statuses for d0-v4" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d0-v4").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("robin-hood") should be(true)
      doc.isFontanaApproved should be(true)
      verifyRaApproved(doc, 2)
    }

    "have the expected statuses for d0-v5" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d0-v5").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(true)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("lil-john") should be(true)
      doc.isSharedOrOwned("cook-mons") should be(true)
      doc.isSharedOrOwned("maid-marian") should be(true)
      doc.isFontanaPending should be(true)
      verifyRaPending(doc, 2)
    }

    "have the expected statuses for d0-v6" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d0-v6").get
      doc.isPublic should be(true)
      doc.isPublished should be(false)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(true)
      doc.isDefaultView should be(true)
      doc.isSharedOrOwned("cook-mons") should be(true)
      doc.isSharedOrOwned("Little John") should be(true)
      doc.isFontanaRejected should be(true)
      verifyRaMissing(doc, 2)  // isn't in 2's queue, b/c of RA bug ;)
    }

    "have the expected statuses for d0-v7" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d0-v7").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(true)
      doc.isSharedOrOwned("lil-john") should be(true)
      doc.isSharedOrOwned("King Richard") should be(true)
      doc.isFontanaApproved should be(true)
      verifyRaApproved(doc, 2)
    }

    "have the expected statuses for d0-v8" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d0-v8").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(true)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("lil-john") should be(true)
      doc.isSharedOrOwned("maid-marian") should be(true)
      doc.isFontanaApproved should be(true)
      verifyRaPending(doc, 2)
    }

    "have the expected statuses for d0-v9" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d0-v9").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("lil-john") should be(true)
      doc.isFontanaApproved should be(true)
      verifyRaApproved(doc, 2)
    }
  }

  "domain 1 is not a customer domain; it has VM, but no RA and does not federate anywhere" should {
    "have the expected statuses for d1-v0" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d1-v0").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("maid-marian") should be(true)
      verifyVmApproved(doc)
    }

    "have the expected statuses for d1-v1" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d1-v1").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("maid-marian") should be(true)
      doc.isSharedOrOwned("friar-tuck") should be(true)
      verifyVmRejected(doc)
      verifyRaMissing(doc, 2)
    }

    "have the expected statuses for d1-v2" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d1-v2").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("maid-marian") should be(true)
      doc.isSharedOrOwned("friar-tuck") should be(true)
      verifyVmPending(doc)
      verifyRaMissing(doc, 2)
    }

    "have the expected statuses for d1-v3" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d1-v3").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("maid-marian") should be(true)
      verifyVmApproved(doc)
      verifyRaRejected(doc, 2)
    }

    "have the expected statuses for d1-v4" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d1-v4").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("maid-marian") should be(true)
      verifyVmApproved(doc)
      verifyRaPending(doc, 2)
    }
  }


  "domain 2 is a customer domain with RA, but no VM, that federates into domain 0, 1 and 3" should {
    "have the expected statuses for d2-v0" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d2-v0").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(true)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("robin-hood") should be(true)
      verifyRaPending(doc, 2)
      verifyRaMissing(doc, 0)
      verifyRaRejected(doc, 3)
    }

    "have the expected statuses for d2-v1" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d2-v1").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("robin-hood") should be(true)
      verifyRaRejected(doc, 2)
      verifyRaMissing(doc, 0)
      verifyRaApproved(doc, 3)
    }

    "have the expected statuses for d2-v2" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d2-v2").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(true)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(true)
      doc.isSharedOrOwned("robin-hood") should be(true)
      verifyRaApproved(doc, 2)
      verifyRaMissing(doc, 0)
      verifyRaApproved(doc, 3)
    }

    "have the expected statuses for d2-v3" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d2-v3").get
      doc.isPublic should be(false)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(true)
      doc.isSharedOrOwned("robin-hood") should be(true)
      doc.isSharedOrOwned("Little John") should be(true)
      verifyRaRejected(doc, 2)
      verifyRaMissing(doc, 0)
      verifyRaRejected(doc, 3)
    }

    "have the expected statuses for d2-v4" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d2-v4").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("john-clan") should be(true)
      verifyRaApproved(doc, 2)
      verifyRaMissing(doc, 0)
      verifyRaApproved(doc, 3)
    }

    "have the expected statuses for d2-v5" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d2-v5").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(true)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("prince-john") should be(true)
      verifyRaApproved(doc, 2)
      verifyRaMissing(doc, 0)
      verifyRaRejected(doc, 3)
    }

    "have the expected statuses for d2-v6" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d2-v6").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("maid-marian") should be(true)
      doc.isSharedOrOwned("lil-john") should be(true)
      verifyRaRejected(doc, 2)
    }

    "have the expected statuses for d2-v7" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d2-v7").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("maid-marian") should be(true)
      doc.isSharedOrOwned("lil-john") should be(true)
      verifyRaPending(doc, 2)
    }
  }

  "domain 3 is a customer domain with both VM & RA that does not federate anywhere" should {
    "have the expected statuses for d3-v0" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d3-v0").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(true)
      doc.isSharedOrOwned("prince-john") should be(true)
      verifyRaRejected(doc, 3)
    }

    "have the expected statuses for d3-v1" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d3-v1").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("prince-john") should be(true)
      verifyVmPending(doc)
      verifyRaPending(doc, 3)
    }

    "have the expected statuses for d3-v2" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d3-v2").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isSharedOrOwned("prince-john") should be(true)
      verifyRaApproved(doc, 3)
    }

    "have the expected statuses for d3-v3" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d3-v3").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(true)
      doc.isSharedOrOwned("prince-john") should be(true)
      verifyRaApproved(doc, 3)
    }

    "have the expected statuses for d3-v4" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d3-v4").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("prince-john") should be(true)
      verifyVmApproved(doc)
      verifyRaApproved(doc, 3)
    }

    "have the expected statuses for d3-v5" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d3-v5").get
      doc.isPublic should be(false)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isPublic should be(false)
      doc.isSharedOrOwned("prince-john") should be(true)
      verifyVmApproved(doc)
      verifyRaApproved(doc, 3)
    }

    "have the expected statuses for d3-v6" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d3-v6").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("prince-john") should be(true)
      doc.isSharedOrOwned("lil-john") should be(true)
      verifyVmPending(doc)
      verifyRaApproved(doc, 3)
    }

    "have the expected statuses for d3-v7" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d3-v7").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("prince-john") should be(true)
      doc.isSharedOrOwned("lil-john") should be(true)
      verifyVmApproved(doc)
      verifyRaRejected(doc, 3)
    }

    "have the expected statuses for d3-v8" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d3-v8").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("maid-marian") should be(true)
      doc.isSharedOrOwned("lil-john") should be(true)
      verifyVmRejected(doc)
      verifyRaRejected(doc, 3)
      verifyRaPending(doc, 2)
    }

    "have the expected statuses for d3-v9" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d3-v9").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("maid-marian") should be(true)
      doc.isSharedOrOwned("lil-john") should be(true)
      verifyRaRejected(doc, 3)
      verifyRaPending(doc, 2)
    }
  }

  "domain 8 is locked-down customer domain with both VM & RA that does not federate anywhere" should {
    "have the expected statuses for d8-v0" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d8-v0").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("prince-john") should be(true)
      verifyVmApproved(doc)
      verifyRaApproved(doc, 8)
    }
  }

  "domain 9 is a customer domain with neither VM & RA that does not federate anywhere" should {
    "have the expected statuses for d9-v0" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d9-v0").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(true)
      doc.isSharedOrOwned("honorable.sheriff") should be(true)
    }

    "have the expected statuses for d9-v1" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d9-v1").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(true)
      doc.isSharedOrOwned("honorable.sheriff") should be(true)
    }

    "have the expected statuses for d9-v2" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d9-v2").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(true)
      doc.isSharedOrOwned("honorable.sheriff") should be(true)
    }

    "have the expected statuses for d9-v3" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d9-v3").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(true)
      doc.isSharedOrOwned("honorable.sheriff") should be(true)
    }

    "have the expected statuses for d9-v4" in {
      val doc = docs.find(d => d.socrataId.datasetId == "d9-v4").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isStory should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(true)
      doc.isSharedOrOwned("lil-john") should be(true)
    }
  }
}
