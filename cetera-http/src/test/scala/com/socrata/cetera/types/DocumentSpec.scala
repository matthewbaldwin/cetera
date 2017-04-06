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

  def verifyVmMissing(doc: Document): Unit = {
    doc.isVmApproved should be(false)
    doc.isVmRejected should be(false)
    doc.isVmPending should be(false)
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

  "domain 0 is a customer domain with neither VM or RA and that federates into domain 2, which has RA" should {
    "have the expected statuses for fxf-0" in {
      val doc = docs.find(d => d.socrataId.datasetId == "fxf-0").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isDatalens should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("robin-hood") should be(true)
      verifyVmMissing(doc)
      verifyRaMissing(doc, 0)
      verifyRaPending(doc, 2)
    }

    "have the expected statuses for fxf-4" in {
      val doc = docs.find(d => d.socrataId.datasetId == "fxf-4").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isDatalens should be(false)
      doc.isHiddenFromCatalog should be(true)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("robin-hood") should be(true)
      verifyVmMissing(doc)
      verifyRaMissing(doc, 0)
      verifyRaApproved(doc, 2)
    }

    "have the expected statuses for fxf-8" in {
      val doc = docs.find(d => d.socrataId.datasetId == "fxf-8").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isDatalens should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(true)
      doc.isSharedOrOwned("robin-hood") should be(true)
      verifyVmMissing(doc)
      verifyRaMissing(doc, 0)
      verifyRaRejected(doc, 2)
    }

    "have the expected statuses for zeta-0001" in {
      val doc = docs.find(d => d.socrataId.datasetId == "zeta-0001").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isDatalens should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("robin-hood") should be(true)
      verifyVmMissing(doc)
      verifyRaMissing(doc, 0)
      verifyRaApproved(doc, 2)
    }

    "have the expected statuses for zeta-0004" in {
      val doc = docs.find(d => d.socrataId.datasetId == "zeta-0004").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isDatalens should be(true)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("lil-john") should be(true)
      doc.isSharedOrOwned("cook-mons") should be(true)
      doc.isSharedOrOwned("maid-marian") should be(true)
      verifyVmPending(doc)  // b/c is a datalens
      verifyRaMissing(doc, 0)
      verifyRaPending(doc, 2)
    }

    "have the expected statuses for zeta-0006" in {
      val doc = docs.find(d => d.socrataId.datasetId == "zeta-0006").get
      doc.isPublic should be(true)
      doc.isPublished should be(false)
      doc.isDatalens should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(true)
      doc.isSharedOrOwned("cook-mons") should be(true)
      doc.isSharedOrOwned("Little John") should be(true)
      verifyVmMissing(doc)
      verifyRaMissing(doc, 0)
      verifyRaMissing(doc, 2)  // isn't in 2's queue, b/c of RA bug ;)
    }

    "have the expected statuses for zeta-0007" in {
      val doc = docs.find(d => d.socrataId.datasetId == "zeta-0007").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isDatalens should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(true)
      doc.isSharedOrOwned("lil-john") should be(true)
      doc.isSharedOrOwned("King Richard") should be(true)
      verifyVmMissing(doc)
      verifyRaMissing(doc, 0)
      verifyRaApproved(doc, 2)
    }

    "have the expected statuses for zeta-0011" in {
      val doc = docs.find(d => d.socrataId.datasetId == "zeta-0011").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isDatalens should be(true)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("lil-john") should be(true)
      doc.isSharedOrOwned("maid-marian") should be(true)
      verifyVmRejected(doc)  // b/c is a datalens
      verifyRaMissing(doc, 0)
      verifyRaPending(doc, 2)
    }

    "have the expected statuses for zeta-0012" in {
      val doc = docs.find(d => d.socrataId.datasetId == "zeta-0012").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isDatalens should be(true)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("lil-john") should be(true)
      verifyVmApproved(doc)  // b/c is a datalens
      verifyRaMissing(doc, 0)
      verifyRaApproved(doc, 2)
    }
  }

  "domain 1 is not a customer domain; it has VM, but no RA and does not federate anywhere" should {
    "have the expected statuses for fxf-1" in {
      val doc = docs.find(d => d.socrataId.datasetId == "fxf-1").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isDatalens should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("maid-marian") should be(true)
      verifyVmApproved(doc)
      verifyRaMissing(doc, 1)
    }

    "have the expected statuses for fxf-5" in {
      val doc = docs.find(d => d.socrataId.datasetId == "fxf-5").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isDatalens should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("maid-marian") should be(true)
      doc.isSharedOrOwned("friar-tuck") should be(true)
      verifyVmRejected(doc)
      verifyRaMissing(doc, 1)
    }

    "have the expected statuses for fxf-9" in {
      val doc = docs.find(d => d.socrataId.datasetId == "fxf-9").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isDatalens should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("maid-marian") should be(true)
      doc.isSharedOrOwned("friar-tuck") should be(true)
      verifyVmPending(doc)
      verifyRaMissing(doc, 1)
    }
  }


  "domain 2 is a customer domain with RA, but no VM, that federates into domain 0 (which has neither VM or RA) and domain 3 (which has both VM & RA)" should {
    "have the expected statuses for fxf-2" in {
      val doc = docs.find(d => d.socrataId.datasetId == "fxf-2").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isDatalens should be(true)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("robin-hood") should be(true)
      verifyVmPending(doc)  // b/c is a datalens
      verifyRaPending(doc, 2)
      verifyRaMissing(doc, 0)
      verifyRaRejected(doc, 3)
    }

    "have the expected statuses for fxf-6" in {
      val doc = docs.find(d => d.socrataId.datasetId == "fxf-6").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isDatalens should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("robin-hood") should be(true)
      verifyVmMissing(doc)
      verifyRaRejected(doc, 2)
      verifyRaMissing(doc, 0)
      verifyRaApproved(doc, 3)
    }

    "have the expected statuses for fxf-10" in {
      val doc = docs.find(d => d.socrataId.datasetId == "fxf-10").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isDatalens should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(true)
      doc.isSharedOrOwned("robin-hood") should be(true)
      verifyVmMissing(doc) // is default view
      verifyRaApproved(doc, 2)
      verifyRaMissing(doc, 0)
      verifyRaApproved(doc, 3)
    }

    "have the expected statuses for zeta-0003" in {
      val doc = docs.find(d => d.socrataId.datasetId == "zeta-0003").get
      doc.isPublic should be(false)
      doc.isPublished should be(true)
      doc.isDatalens should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(true)
      doc.isSharedOrOwned("robin-hood") should be(true)
      doc.isSharedOrOwned("Little John") should be(true)
      verifyVmMissing(doc)
      verifyRaRejected(doc, 2)
      verifyRaMissing(doc, 0)
      verifyRaRejected(doc, 3)
    }

    "have the expected statuses for zeta-0005" in {
      val doc = docs.find(d => d.socrataId.datasetId == "zeta-0005").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isDatalens should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("john-clan") should be(true)
      verifyVmMissing(doc)
      verifyRaApproved(doc, 2)
      verifyRaMissing(doc, 0)
      verifyRaApproved(doc, 3)
    }

    "have the expected statuses for zeta-0010" in {
      val doc = docs.find(d => d.socrataId.datasetId == "zeta-0010").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isDatalens should be(false)
      doc.isHiddenFromCatalog should be(true)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("prince-john") should be(true)
      verifyVmMissing(doc)
      verifyRaApproved(doc, 2)
      verifyRaMissing(doc, 0)
      verifyRaRejected(doc, 3)
    }
  }

  "domain 3 is a customer domain with both VM & RA that does not federate anywhere" should {
    "have the expected statuses for fxf-3" in {
      val doc = docs.find(d => d.socrataId.datasetId == "fxf-3").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isDatalens should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(true)
      doc.isSharedOrOwned("prince-john") should be(true)
      verifyVmMissing(doc) // b/c is a default view
      verifyRaRejected(doc, 3)
    }

    "have the expected statuses for fxf-7" in {
      val doc = docs.find(d => d.socrataId.datasetId == "fxf-7").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isDatalens should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("prince-john") should be(true)
      verifyVmPending(doc)
      verifyRaPending(doc, 3)
    }

    "have the expected statuses for fxf-11" in {
      val doc = docs.find(d => d.socrataId.datasetId == "fxf-11").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isDatalens should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isPublic should be(true)
      doc.isSharedOrOwned("prince-john") should be(true)
      verifyVmMissing(doc)  // is default view
      verifyRaApproved(doc, 3)
    }

    "have the expected statuses for fxf-12" in {
      val doc = docs.find(d => d.socrataId.datasetId == "fxf-12").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isDatalens should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(true)
      doc.isSharedOrOwned("prince-john") should be(true)
      verifyVmMissing(doc)  // is default view
      verifyRaApproved(doc, 3)
    }

    "have the expected statuses for zeta-0002" in {
      val doc = docs.find(d => d.socrataId.datasetId == "zeta-0002").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isDatalens should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("prince-john") should be(true)
      verifyVmApproved(doc)
      verifyRaApproved(doc, 3)
    }

    "have the expected statuses for zeta-0009" in {
      val doc = docs.find(d => d.socrataId.datasetId == "zeta-0009").get
      doc.isPublic should be(false)
      doc.isPublished should be(true)
      doc.isDatalens should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isPublic should be(false)
      doc.isSharedOrOwned("prince-john") should be(true)
      verifyVmApproved(doc)
      verifyRaApproved(doc, 3)
    }

    "have the expected statuses for zeta-0013" in {
      val doc = docs.find(d => d.socrataId.datasetId == "zeta-0013").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isDatalens should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("prince-john") should be(true)
      doc.isSharedOrOwned("lil-john") should be(true)
      verifyVmPending(doc)
      verifyRaApproved(doc, 3)
    }

    "have the expected statuses for zeta-0014" in {
      val doc = docs.find(d => d.socrataId.datasetId == "zeta-0014").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isDatalens should be(true)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("prince-john") should be(true)
      doc.isSharedOrOwned("lil-john") should be(true)
      verifyVmApproved(doc)
      verifyRaRejected(doc, 3)
    }
  }

  "domain 8 is locked-down customer domain with both VM & RA that does not federate anywhere" should {
    "have the expected statuses for zeta-0008" in {
      val doc = docs.find(d => d.socrataId.datasetId == "zeta-0008").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isDatalens should be(true)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(false)
      doc.isSharedOrOwned("prince-john") should be(true)
      verifyVmApproved(doc)
      verifyRaApproved(doc, 8)
    }
  }

  "domain 9 is a customer domain with neither VM & RA that does not federate anywhere" should {
    "have the expected statuses for 1234-5678" in {
      val doc = docs.find(d => d.socrataId.datasetId == "1234-5678").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isDatalens should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(true)
      doc.isSharedOrOwned("honorable.sheriff") should be(true)
    }

    "have the expected statuses for 1234-5679" in {
      val doc = docs.find(d => d.socrataId.datasetId == "1234-5679").get
      doc.isPublic should be(true)
      doc.isPublished should be(true)
      doc.isDatalens should be(false)
      doc.isHiddenFromCatalog should be(false)
      doc.isDefaultView should be(true)
      doc.isSharedOrOwned("honorable.sheriff") should be(true)
    }
  }
}

