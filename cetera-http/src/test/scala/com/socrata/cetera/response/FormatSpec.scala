package com.socrata.cetera.response

import scala.io.Source

import com.rojoma.json.v3.ast.{JArray, JNumber, JObject, JString}
import com.rojoma.json.v3.codec.JsonDecode
import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.JsonReader
import org.scalatest._

import com.socrata.cetera.TestESDomains
import com.socrata.cetera.auth.{AuthedUser, User}
import com.socrata.cetera.types.{Datatype, DomainSet}

class FormatSpec extends WordSpec with ShouldMatchers with TestESDomains {

  val drewRawString = Source.fromInputStream(getClass.getResourceAsStream("/drewRaw.json")).getLines().mkString("\n")
  val drewRawJson = JsonReader.fromString(drewRawString)

  "The hyphenize method" should {
    "return a single hyphen if given an empty string" in {
      val hyphenized = Format.hyphenize("")
      hyphenized should be("-")
    }

    "return the given string if a single alphanumeric word" in {
      val string = "hello"
      val hyphenized = Format.hyphenize(string)
      hyphenized should be(string)
    }

    "return the given string if a single alphanumeric word with unicode" in {
      val string = "Καλημέρα"
      val hyphenized = Format.hyphenize(string)
      hyphenized should be(string)
    }

    "add no hyphens to underscorized strings" in {
      val string = "hello_world"
      val hyphenized = Format.hyphenize(string)
      hyphenized should be(string)
    }

    "add hyphens where non-alphanumeric characters are present" in {
      val hyphenized1 = Format.hyphenize("hello\n\rworld")
      val hyphenized2 = Format.hyphenize("hello**world")
      val hyphenized3 = Format.hyphenize("hello world!")
      val hyphenized4 = Format.hyphenize("hello world. How ya doin'?")

      val expected1 = "hello-world"
      val expected2 = "hello-world"
      val expected3 = "hello-world-"
      val expected4 = "hello-world-How-ya-doin-"

      hyphenized1 should be(expected1)
      hyphenized2 should be(expected2)
      hyphenized3 should be(expected3)
      hyphenized4 should be(expected4)
    }

    "truncate long strings" in {
      val string = "Well hello there world!\n How are you doing on this fine gray Seattle day?"
      val hyphenized = Format.hyphenize(string)
      val expected = "Well-hello-there-world-How-are-you-doing-on-this-f"
      hyphenized should be(expected)
    }
  }

  "The links method" should {
    def testLinks(
      datatype: Option[Datatype],
      viewType: Option[String],
      category: Option[String],
      name: String,
      expectedPermaPath: String,
      expectedSeoPath: String)
    : Unit =
      s"return the correct seo and perma links for \n" +
        s"datatype=$datatype, viewType=$viewType, category=$category and name='$name'" in {
        val cname = "tempuri.org"
        val id = "1234-abcd"
        val expectedPermalink = s"https://$cname/$expectedPermaPath/$id"
        val expectedSeolink = s"https://$cname/$expectedSeoPath/$id"
        val previewImageId = "123456789"
        val expectedPreviewImageUrl = s"https://$cname/views/$id/files/$previewImageId"

        val urls = Format.links(cname, None, datatype, viewType, id, category, name, Some(previewImageId))
        urls.get("permalink").map(_.string) should be(Some(expectedPermalink))
        urls.get("link").map(_.string) should be(Some(expectedSeolink))
        urls.get("previewImageUrl").map(_.string) should be(Some(expectedPreviewImageUrl))
      }

    // datatype/viewtype tests
    val dt = "datatype"
    val vt = "viewtype"
    val xp = "expectedPermaPath"
    val xpDefault = "d"
    val xs = "expectedSeoPath"
    val xsDefault = "Public-Safety/SPD-911"

    Seq(
      Map(dt -> "calendar"),
      Map(dt -> "chart"),
      Map(dt -> "datalens", xp -> "view"),
      Map(dt -> "chart", vt -> "datalens", xp -> "view"),
      Map(dt -> "map", vt -> "datalens", xp -> "view"),
      Map(dt -> "dataset"),
      Map(dt -> "file"),
      Map(dt -> "filter"),
      Map(dt -> "form"),
      Map(dt -> "map", vt -> "geo"),
      Map(dt -> "map", vt -> "tabular"),
      Map(dt -> "href"),
      Map(dt -> "story", xp -> "stories/s", xs -> "stories/s")
    ).foreach { t =>
      val category = Some("Public Safety")
      val name = "SPD 911"
      testLinks(Datatype(t.get(dt)), t.get(vt), category, name, t.getOrElse(xp, xpDefault), t.getOrElse(xs, xsDefault))
    }

    // category tests
    Seq(None, Some("")).foreach { category =>
      val name = "this is a name"
      testLinks(Datatype("dataset"), None, category, name, "d", "dataset/this-is-a-name")
    }

    // name tests
    Seq(null, "").foreach { name =>
      val category = Some("this-is-a-category")
      testLinks(Datatype("dataset"), None, category, name, "d", "this-is-a-category/-")
    }

    // length test
    val longCategory = Some("A super long category name is not very likely but we will protect against it anyway")
    val longName = "More commonly customers may write a title that is excessively verbose and it will hit this limit"
    val limitedCategory = "A-super-long-category-name-is-not-very-likely-but-"
    val limitedName = "More-commonly-customers-may-write-a-title-that-is-"
    testLinks(Datatype("dataset"), None, longCategory, longName, "d", s"$limitedCategory/$limitedName")

    // unicode test
    val unicodeCategory = Some("بيانات عن الجدات")
    val unicodeName = "愛"
    testLinks(Datatype("dataset"), None, unicodeCategory, unicodeName, "d", "بيانات-عن-الجدات/愛")

    "return requested locale if specified" in {
      val cname = "fu.bar"
      val locale = "pirate"
      val id = "1234-abcd"
      val name = "Pirates are awesome"

      val expectedPermaLink = s"https://$cname/$locale/d/$id"
      val expectedSeoLink = s"https://$cname/$locale/dataset/${Format.hyphenize(name)}/$id"

      val urls = Format.links(cname, Some(locale), None, None, id, None, name, None)

      urls.get("permalink").get.string should be(expectedPermaLink)
      urls.get("link").get.string should be(expectedSeoLink)
    }
  }

  "the domainCategory method" should {
    "return None if passed unexpected json" in {
      val domainCategory = Format.domainCategory(j"""{"huh":"this ain't right"}""")
      domainCategory should be(None)
    }

    "return the expected JValue if passed good json" in {
      val domainCategory = Format.domainCategory(drewRawJson)
      domainCategory should be(Some(JString("Finance")))
    }
  }

  "the domainCategoryString method" should {
    val esDrewString = Source.fromInputStream(getClass.getResourceAsStream("/drewRaw.json")).getLines().mkString("\n")
    val esDrewJson = JsonReader.fromString(esDrewString)

    "return None if passed unexpected json" in {
      val domainCategory = Format.domainCategoryString(j"""{"huh":"this ain't right"}""")
      domainCategory should be(None)
    }

    "return the expected string if passed good json" in {
      val domainCategory = Format.domainCategoryString(esDrewJson)
      domainCategory should be(Some("Finance"))
    }
  }

  "the domainTags method" should {
    "return None if passed unexpected json" in {
      val domainTags = Format.domainTags(j"""{"huh":"this ain't right"}""")
      domainTags should be(None)
    }

    "return the expected JValue if passed good json" in {
      val domainTags = Format.domainTags(drewRawJson)
      domainTags should be(Some(JArray(List(JString("pie")))))
    }
  }

  "the domainMetadata method" should {
    "return None if passed unexpected json" in {
      val domainMetadata = Format.domainMetadata(j"""{"huh":"this ain't right"}""")
      domainMetadata should be(None)
    }

    "return the expected JValue if passed good json" in {
      val domainMetadata = Format.domainMetadata(drewRawJson)
      val publisherObject = JObject(Map("value" -> JString("City of Redmond"), "key" -> JString("Common-Core_Publisher")))
      val emailObject = JObject(Map("value" -> JString("drew@redmond.gov"), "key" -> JString("Common-Core_Contact-Email")))
      val metadataArray = JArray(List(publisherObject, emailObject))

      domainMetadata should be(Some(metadataArray))
    }
  }

  "the domainPrivateMetadata method" should {
    val viewsDomainId = 0
    val privateMetadata = JArray(List(
      JObject(Map("value" -> JString("No looky"),
                  "key" -> JString("Private-Metadata_Thing")))
    ))

    "return None if no user is provided" in {
      val metadata = Format.domainPrivateMetadata(drewRawJson, None, viewsDomainId)
      metadata should be(None)
    }

    "return None if the user has no claim on the private metadata" in {
      val user = Some(AuthedUser("some-user", domains(0)))  // who isn't a publisher or admin and doesn't own/share the data
      val metadata = Format.domainPrivateMetadata(drewRawJson, user, viewsDomainId)
      metadata should be(None)
    }

    "return None if the user has a role, but it isn't on the view's domain" in {
      val user = Some(AuthedUser("some-user", domains(1), Some("publisher"))) // drewRaw is on domain 0
      val metadata = Format.domainPrivateMetadata(drewRawJson, user, viewsDomainId)
      metadata should be(None)
    }

    "return the expected JValue if the user owns the view" in {
      val user = Some(AuthedUser("7kqh-9s5a", domains(1)))
      val metadata = Format.domainPrivateMetadata(drewRawJson, user, viewsDomainId)
      metadata should be(Some(privateMetadata))
    }

    "return the expected JValue if the user shares the view" in {
      val user = Some(AuthedUser("ti9x-irmy", domains(1)))
      val metadata = Format.domainPrivateMetadata(drewRawJson, user, viewsDomainId)
      metadata should be(Some(privateMetadata))
    }

    "return the expected JValue if the user is a super admin" in {
      val user = Some(AuthedUser("super-user", domains(0), flags = Some(List("admin"))))
      val metadata = Format.domainPrivateMetadata(drewRawJson, user, viewsDomainId)
      metadata should be(Some(privateMetadata))
    }

    "return the expected JValue if the user is an admin on the view's domain" in {
      val user = Some(AuthedUser("some-user", domains(0), Some("administrator")))
      val metadata = Format.domainPrivateMetadata(drewRawJson, user, viewsDomainId)
      metadata should be(Some(privateMetadata))
    }

    "return the expected JValue if the user is a publisher on the view's domain" in {
      val user = Some(AuthedUser("some-user", domains(0), Some("publisher")))
      val metadata = Format.domainPrivateMetadata(drewRawJson, user, viewsDomainId)
      metadata should be(Some(privateMetadata))
    }
  }

  "the categories method" should {
    "return the expected JValues if passed good json" in {
      val categories = Format.categories(drewRawJson)
      categories should be(List(JString("Finance")))
    }
  }

  "the tags method" should {
    "return the expected distinct JValues if passed good json" in {
      val tags = Format.tags(drewRawJson)
      tags should be(List(JString("finance"), JString("pie")))
    }
  }

  "the cname method" should {
    "return an empty string if passed good json but a incomplete domainId map" in {
      val cname = Format.cname(Map.empty[Int, String], drewRawJson)
      cname should be("")
    }

    "return the expected cname if passed good json and complete domainId map" in {
      val drewCname = "data.redmond.gov"
      val cname = Format.cname(Map(0 -> drewCname), drewRawJson)
      cname should be(drewCname)
    }
  }

  "the datatype method" should {
    "return None if passed unexpected json" in {
      val datatype = Format.datatype(j"""{"huh":"this ain't right"}""")
      datatype should be(None)
    }

    "return the expected cname if passed good json" in {
      val datatype = Format.datatype(drewRawJson)
      datatype.get.singular should be("chart")
    }
  }

  "the viewtype method" should {
    "return None if passed unexpected json" in {
      val viewtype = Format.viewtype(j"""{"huh":"this ain't right"}""")
      viewtype should be(None)
    }

    "return the expected cname if passed good json" in {
      val viewtype = Format.viewtype(drewRawJson)
      viewtype should be(Some("pie"))
    }
  }

  "the datasetId method" should {
    "return None if passed unexpected json" in {
      val datasetId = Format.datasetId(j"""{"huh":"this ain't right"}""")
      datasetId should be(None)
    }

    "return the expected cname if passed good json" in {
      val datasetId = Format.datasetId(drewRawJson)
      datasetId should be(Some("94sr-dmsy"))
    }
  }

  "the isPublic method" should {
    "return false if the is_public flag is missing" in {
      val view = j"""{ }"""
      Format.isPublic(view) should be(false)
    }

    "return false if the is_public flag is false" in {
      val view = j"""{ "is_public": false }"""
      Format.isPublic(view) should be(false)
    }

    "return true if the is_public flag is true" in {
      val view = j"""{ "is_public": true }"""
      Format.isPublic(view) should be(true)
    }
  }

  "the isPublished method" should {
    "return false if the is_published flag is missing" in {
      val view = j"""{ }"""
      Format.isPublished(view) should be(false)
    }

    "return false if the is_published flag is false" in {
      val view = j"""{ "is_published": false }"""
      Format.isPublished(view) should be(false)
    }

    "return true if the is_published flag is true" in {
      val view = j"""{ "is_published": true }"""
      Format.isPublished(view) should be(true)
    }
  }

  "the datalensStatus method" should {
    "return None if the view isn't a datalens" in {
      val view = j"""{ "datatype": "chart" }"""
      val fakeDatalens = j"""{ "datatype": "datalens_fake_thing" }"""

      Format.datalensStatus(view) should be(None)
      Format.datalensStatus(fakeDatalens) should be(None)
    }

    "return rejected if the datalens is rejected" in {
      val view = j"""{ "datatype": "datalens", "moderation_status": "rejected" }"""
      Format.datalensStatus(view) should be(Some("rejected"))
    }

    "return pending if the datalens is pending" in {
      val view = j"""{ "datatype": "datalens", "moderation_status": "pending" }"""
      Format.datalensStatus(view) should be(Some("pending"))
    }

    "return approved if the datalens is approved (for all datalens types)" in {
      val datalens = j"""{ "datatype": "datalens", "moderation_status": "approved" }"""
      val datalensChart = j"""{ "datatype": "datalens_chart", "moderation_status": "approved" }"""
      val datalensMap = j"""{ "datatype": "datalens_map", "moderation_status": "approved" }"""

      Format.datalensStatus(datalens) should be(Some("approved"))
      Format.datalensStatus(datalensChart) should be(Some("approved"))
      Format.datalensStatus(datalensMap) should be(Some("approved"))
    }
  }

  "the datalensApproved method" should {
    "return None if the view isn't a datalens" in {
      val view = j"""{ "datatype": "chart" }"""
      val fakeDatalens = j"""{ "datatype": "datalens_fake_thing" }"""

      Format.datalensApproved(view) should be(None)
      Format.datalensApproved(fakeDatalens) should be(None)
    }

    "return false if the datalens is rejected" in {
      val view = j"""{ "datatype": "datalens", "moderation_status": "rejected" }"""
      Format.datalensApproved(view) should be(Some(false))
    }

    "return false if the datalens is pending" in {
      val view = j"""{ "datatype": "datalens", "moderation_status": "pending" }"""
      Format.datalensApproved(view) should be(Some(false))
    }

    "return true if the datalens is approved (for all datalens types)" in {
      val datalens = j"""{ "datatype": "datalens", "moderation_status": "approved" }"""
      val datalensChart = j"""{ "datatype": "datalens_chart", "moderation_status": "approved" }"""
      val datalensMap = j"""{ "datatype": "datalens_map", "moderation_status": "approved" }"""

      Format.datalensApproved(datalens) should be(Some(true))
      Format.datalensApproved(datalensChart) should be(Some(true))
      Format.datalensApproved(datalensMap) should be(Some(true))
    }
  }

  "the moderationStatus method" should {
    "return None if the domain does not have moderation enabled" in {
      val view = j"""{ "datatype": "chart", "moderation_status": "approved" }"""
      val unmoderatedDomain = domains(0)
      Format.moderationStatus(view, unmoderatedDomain) should be(None)
    }

    "return the appropriate status for a derived view if domain has moderation enabled" in {
      val approvedView = j"""{ "datatype": "chart", "moderation_status": "approved" }"""
      val pendingView = j"""{ "datatype": "chart", "moderation_status": "pending" }"""
      val rejectedView = j"""{ "datatype": "chart", "moderation_status": "rejected" }"""

      val moderatedDomain = domains(1)
      Format.moderationStatus(approvedView, moderatedDomain) should be(Some("approved"))
      Format.moderationStatus(pendingView, moderatedDomain) should be(Some("pending"))
      Format.moderationStatus(rejectedView, moderatedDomain) should be(Some("rejected"))
    }

    "return 'approved' if the view is a dataset and the domain has moderation enabled" in {
      val view = j"""{ "datatype": "dataset", "is_default_view": true }"""
      val moderatedDomain = domains(1)
      Format.moderationStatus(view, moderatedDomain) should be(Some("approved"))
    }
  }

  "the moderationApproved method" should {
    "return None if the domain does not have moderation enabled" in {
      val view = j"""{ "datatype": "chart", "moderation_status": "approved" }"""
      val unmoderatedDomain = domains(0)
      Format.moderationApproved(view, unmoderatedDomain) should be(None)
    }

    "return false if the view is rejected and the domain has moderation enabled" in {
      val view = j"""{ "datatype": "chart", "moderation_status": "rejected" }"""
      val moderatedDomain = domains(1)
      Format.moderationApproved(view, moderatedDomain) should be(Some(false))
    }

    "return false if the view is pending and the domain has moderation enabled" in {
      val view = j"""{ "datatype": "chart", "moderation_status": "pending" }"""
      val moderatedDomain = domains(1)
      Format.moderationApproved(view, moderatedDomain) should be(Some(false))
    }

    "return true if the view is a dataset and the domain has moderation enabled" in {
      val view = j"""{ "datatype": "dataset", "is_default_view": true }"""
      val moderatedDomain = domains(1)
      Format.moderationApproved(view, moderatedDomain) should be(Some(true))
    }

    "return true if the view is approved and the domain has moderation enabled)" in {
      val view = j"""{ "datatype": "chart", "moderation_status": "approved" }"""
      val moderatedDomain = domains(1)
      Format.moderationApproved(view, moderatedDomain) should be(Some(true))
    }
  }

  "the moderationApprovedByContext method" should {
    "return None if the context doesn't have view moderation" in {
      val view = j"""{ "datatype": "chart", "moderation_status": "approved" }"""
      val unmoderatedDomain0 = domains(0)
      val unmoderatedDomain2 = domains(2)
      val moderatedDomain = domains(1)
      val context = DomainSet(searchContext = Some(unmoderatedDomain0))
      Format.moderationApprovedByContext(view, unmoderatedDomain2, context) should be(None)
      Format.moderationApprovedByContext(view, moderatedDomain, context) should be(None)
    }

    "return false if the view is rejected and the domain and context have moderation enabled" in {
      val view = j"""{ "datatype": "chart", "moderation_status": "rejected" }"""
      val moderatedDomain1 = domains(1)
      val moderatedDomain3 = domains(3)
      val context = DomainSet(searchContext = Some(moderatedDomain1))
      Format.moderationApprovedByContext(view, moderatedDomain3, context) should be(Some(false))
    }

    "return false if the view is pending and the domain and context have moderation enabled" in {
      val view = j"""{ "datatype": "chart", "moderation_status": "pending" }"""
      val moderatedDomain1 = domains(1)
      val moderatedDomain3 = domains(3)
      val context = DomainSet(searchContext = Some(moderatedDomain1))
      Format.moderationApprovedByContext(view, moderatedDomain3, context) should be(Some(false))
    }

    "return true if the view is a dataset and the domain and context have moderation enabled" in {
      val view = j"""{ "datatype": "dataset", "is_default_view": true }"""
      val moderatedDomain1 = domains(1)
      val moderatedDomain3 = domains(3)
      val context = DomainSet(searchContext = Some(moderatedDomain1))
      Format.moderationApprovedByContext(view, moderatedDomain3, context) should be(Some(true))
    }

    "return true if the view is approved and the domain and context have moderation enabled" in {
      val view = j"""{ "datatype": "chart", "moderation_status": "approved" }"""
      val moderatedDomain1 = domains(1)
      val moderatedDomain3 = domains(3)
      val context = DomainSet(searchContext = Some(moderatedDomain1))
      Format.moderationApprovedByContext(view, moderatedDomain3, context) should be(Some(true))
    }

    "return false if the view is not default and the context has moderation enabled, but not the view's domain" in {
      val view = j"""{ "datatype": "chart", "is_default_view": false }"""
      val moderatedDomain = domains(1)
      val viewsDomain = domains(0)
      val context = DomainSet(searchContext = Some(moderatedDomain))
      Format.moderationApprovedByContext(view, viewsDomain, context) should be(Some(false))
    }

    "return true if the view is a dataset and the context has moderation enabled, but not the view's domain" in {
      val view = j"""{ "datatype": "dataset", "is_default_view": true }"""
      val moderatedDomain = domains(1)
      val viewsDomain = domains(0)
      val context = DomainSet(searchContext = Some(moderatedDomain))
      Format.moderationApprovedByContext(view, viewsDomain, context) should be(Some(true))
    }
  }


  "the routingStatus method" should {
    "return None if the domain does not have R&A enabled" in {
      val view = j"""{ }"""
      val unroutedDomain = domains(0)
      Format.routingStatus(view, unroutedDomain) should be(None)
    }

    "return the appropriate status if domain has R&A enabled" in {
      val approvedView = j"""{ "datatype": "dataset", "is_approved_by_parent_domain": true }"""
      val pendingView = j"""{ "datatype": "dataset", "is_pending_on_parent_domain": true }"""
      val rejectedView = j"""{ "datatype": "dataset", "is_rejected_by_parent_domain": true }"""

      val routedDomain = domains(2)
      Format.routingStatus(approvedView, routedDomain) should be(Some("approved"))
      Format.routingStatus(pendingView, routedDomain) should be(Some("pending"))
      Format.routingStatus(rejectedView, routedDomain) should be(Some("rejected"))
    }
  }

  "the routingApproved method" should {
    "return None if the domain does not have R&A enabled" in {
      val view = j"""{ }"""
      val unroutedDomain = domains(0)
      Format.routingApproved(view, unroutedDomain) should be(None)
    }

    "return false if the dataset isn't approved by its parent domain" in {
      val view = j"""{ "datatype": "dataset", "approving_domain_ids": [ 0 ] }"""
      val unroutedDomain = domains(0)
      val routedDomain = domains(2)
      Format.routingApproved(view, routedDomain) should be(Some(false))
    }

    "return true if the dataset is approved by its parent domain" in {
      val view = j"""{ "datatype": "dataset", "approving_domain_ids": [ 2 ] }"""
      val routedDomain = domains(2)
      Format.routingApproved(view, routedDomain) should be(Some(true))
    }
  }

  "the routingApprovedByContext method" should {
    "return None if there is no search context regardless of parent domain" in {
      val view = j"""{ "datatype": "dataset", "approving_domain_ids": [ 0, 2 ] }"""
      val unroutedDomain = domains(0)
      val routedDomain = domains(2)
      val noContext = DomainSet(searchContext = None)
      Format.routingApprovedByContext(view, unroutedDomain, noContext) should be(None)
      Format.routingApprovedByContext(view, routedDomain, noContext) should be(None)
    }

    "return None if the context doesn't have R&A enabled, regardless of parent domain" in {
      val view = j"""{ "datatype": "dataset", "approving_domain_ids": [ 0, 2 ] }"""
      val unroutedDomain = domains(0)
      val routedDomain = domains(2)
      val unroutedContext = DomainSet(searchContext = Some(domains(1)))
      Format.routingApprovedByContext(view, unroutedDomain, unroutedContext) should be(None)
      Format.routingApprovedByContext(view, routedDomain, unroutedContext) should be(None)
    }

    "return false if the dataset isn't approved by the RA-enabled context" in {
      val view = j"""{ "datatype": "dataset", "approving_domain_ids": [ 3 ] }"""
      val viewsDomain = domains(3)
      val routedDomain = domains(2)
      val routedContext = DomainSet(searchContext = Some(routedDomain))

      Format.routingApprovedByContext(view, viewsDomain, routedContext) should be(Some(false))
    }

    "return true if the dataset is approved by the RA-enabled context" in {
      val view = j"""{ "datatype": "dataset", "approving_domain_ids": [ 2, 3 ] }"""
      val viewsDomain = domains(3)
      val routedDomain = domains(2)
      val routedContext = DomainSet(searchContext = Some(routedDomain))

      Format.routingApprovedByContext(view, viewsDomain, routedContext) should be(Some(true))
    }
  }

  "the contextApprovals method" should {
    "return None for both VM and R&A if the view's domainId is the same as the search context's id" in {
      val view = j"""{ "datatype": "chart", "moderation_status": "approved" }"""
      val moderatedDomain = domains(1)
      val context = DomainSet(searchContext = Some(moderatedDomain))
      val (vmContextApproval, raContextApproval) = Format.contextApprovals(view, moderatedDomain, context)
      vmContextApproval should be(None)
      raContextApproval should be(None)
    }

    "return the expected VM and R&A context approvals if the view's domainId is not the same as the search context's id" in {
      val view = j"""{ "datatype": "dataset", "is_default_view": true, "approving_domain_ids": [ 3 ] }"""
      val viewsDomain = domains(1)
      val contextDomain = domains(3)
      val context = DomainSet(searchContext = Some(contextDomain))
      val (vmContextApproval, raContextApproval) = Format.contextApprovals(view, viewsDomain, context)
      vmContextApproval should be(Some(true))
      raContextApproval should be(Some(true))
    }
  }

  "the documentSearchResult method" should {
    "return the expected payload if passed good json" in {
      val unmoderatedUnroutedContext = DomainSet(domains = Set(domains(0)), searchContext = Some(domains(0)))
      val actualResult = Format.documentSearchResult(drewRawJson, None, unmoderatedUnroutedContext, None, Some(JNumber(.98)), true, None).get
      val drewFormattedString = Source.fromInputStream(getClass.getResourceAsStream("/drewFormatted.json")).getLines().mkString("\n")
      val drewFormattedJson = JsonReader.fromString(drewFormattedString)

      val expectedResult = JsonDecode.fromJValue[SearchResult](drewFormattedJson).right.get
      actualResult.resource should be(expectedResult.resource)
      actualResult.link should be(expectedResult.link)
      actualResult.permalink should be(expectedResult.permalink)
      actualResult.metadata should be(expectedResult.metadata)

      // the classifications don't directly compare, since rojoma reads Lists in as Vectors
      actualResult.classification.categories(0) should be(expectedResult.classification.categories(0))
      actualResult.classification.tags(0) should be(expectedResult.classification.tags(0))
      actualResult.classification.tags(1) should be(expectedResult.classification.tags(1))
      actualResult.classification.domainCategory should be(expectedResult.classification.domainCategory)
      actualResult.classification.domainTags should be(expectedResult.classification.domainTags)
      actualResult.classification.domainMetadata should be(expectedResult.classification.domainMetadata)
    }

    "return the expected payload if passed good json and a user with rights to see the private metadata" in {
      val user = Some(AuthedUser("some-user", domains(0), Some("publisher")))
      val unmoderatedUnroutedContext = DomainSet(domains = Set(domains(0)), searchContext = Some(domains(0)))
      val actualResult = Format.documentSearchResult(drewRawJson, user, unmoderatedUnroutedContext, None, Some(JNumber(.98)), true, None).get
      val drewFormattedString = Source.fromInputStream(getClass.getResourceAsStream("/drewFormattedWithPrivateMetadata.json")).getLines().mkString("\n")
      val drewFormattedJson = JsonReader.fromString(drewFormattedString)
      val expectedResult = JsonDecode.fromJValue[SearchResult](drewFormattedJson).right.get

      actualResult.classification.domainPrivateMetadata should be(expectedResult.classification.domainPrivateMetadata)
    }

    "return None if passed bad json" in {
      val view = j"""{"bad": "json", "socrata_id": { "domain_id": 0} }"""
      val unmoderatedUnroutedContext = DomainSet(domains = Set(domains(0)), searchContext = Some(domains(0)))
      val actualResult = Format.documentSearchResult(view, None, unmoderatedUnroutedContext, None, None, false, None)
      actualResult should be(None)
    }
  }
}
