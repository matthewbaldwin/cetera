package com.socrata.cetera.response

import scala.io.Source

import com.rojoma.json.v3.codec.JsonDecode
import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.util.JsonUtil
import org.apache.commons.io.FileUtils
import org.elasticsearch.action.search.{SearchResponse, ShardSearchFailure}
import org.elasticsearch.common.bytes.BytesArray
import org.elasticsearch.common.text.Text
import org.elasticsearch.search.aggregations.InternalAggregations
import org.elasticsearch.search.internal.InternalSearchResponse
import org.elasticsearch.search.profile.{ProfileShardResult, SearchProfileShardResults}
import org.elasticsearch.search.suggest.Suggest
import org.elasticsearch.search.{SearchHit, SearchHitField, SearchHits}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, _}

import com.socrata.cetera.TestESData
import com.socrata.cetera.auth.AuthedUser
import com.socrata.cetera.handlers.{FormatParamSet, PagingParamSet, ScoringParamSet, SearchParamSet}
import com.socrata.cetera.types.{CustomerMetadataFlattened, Datatype, Document, Domain, DomainSet}
import scala.collection.JavaConverters._

class FormatSpec extends WordSpec with ShouldMatchers
  with TestESData
  with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    bootstrapData()
  }

  override protected def afterAll(): Unit = {
    removeBootstrapData()
  }

  val drewRawString = Source.fromInputStream(getClass.getResourceAsStream("/drewRaw.json")).getLines().mkString("\n")
  val drewRawJson = JsonReader.fromString(drewRawString)
  val storyRawString = Source.fromInputStream(getClass.getResourceAsStream("/views/d2-v2.json")).getLines().mkString("\n")
  val storyRawJson = JsonReader.fromString(storyRawString)
  val emptySearchHitMap = Map[String, SearchHitField]().asJava
  val domainSet = DomainSet(domains = (0 to 2).map(domains(_)).toSet)

  "The formatDocumentResponse method" should {
    "extract and format resources from SearchResponse" in {
      val req = documentClient.buildSearchRequest(domainSet, SearchParamSet(user = Some("john-clan")), ScoringParamSet(), PagingParamSet(), None)
      val res = req.execute.actionGet
      val internalSearchHits = new SearchHits(res.getHits.getHits, 3037, 1.0f)
      val internalSearchResponse = new InternalSearchResponse(
        internalSearchHits, new InternalAggregations(Nil.asJava), new Suggest(Nil.asJava),
        new SearchProfileShardResults(Map.empty[String, ProfileShardResult].asJava),
        false, false, 0)
      val searchResponse = new SearchResponse(internalSearchResponse, "", 15, 15, 4, Array[ShardSearchFailure]())
      val searchResults = Format.formatDocumentResponse(searchResponse, None, domainSet, FormatParamSet())

      searchResults.resultSetSize should be(searchResponse.getHits.getTotalHits)
      searchResults.timings should be(None) // not yet added

      val results = searchResults.results
      results should be('nonEmpty)
      results.size should be(1)

      val expectedDoc = docs.filter(d => d.isSharedOrOwned("john-clan")).headOption.get
      val datasetResponse = results(0)
      datasetResponse.resource should be(expectedDoc.resource)
      datasetResponse.classification should be(Classification(Vector(), Vector(), Some("Fun"), Vector(), Vector(), None))

      datasetResponse.metadata.domain should be("blue.org")
    }

    "not throw on bad documents, but rather just ignore them" in {
      val score = 0.54321f
      val badResource = "\"badResource\":{\"name\": \"Just A Test\", \"I'm\":\"NOT OK\",\"you'll\":\"never know\"}"
      val goodResource = "\"resource\":{\"name\": \"Just A Test\", \"I'm\":\"OK\",\"you're\":\"so-so\"}"
      val datasetSocrataId = "\"socrata_id\":{\"domain_id\":[0],\"dataset_id\":\"four-four\"}"
      val badDatasetSocrataId = "\"socrata_id\":{\"domain_id\":\"i am a string\",\"dataset_id\":\"four-four\"}"
      val datasetDatatype = "\"datatype\":\"dataset\""
      val datasetViewtype = "\"viewtype\":\"\""
      val badResourceDatasetSource = new BytesArray("{" +
        List(badResource, datasetDatatype, datasetViewtype, datasetSocrataId).mkString(",") +
        "}")
      val badSocrataIdDatasetSource = new BytesArray("{" +
        List(goodResource, datasetDatatype, datasetViewtype, badDatasetSocrataId).mkString(",")
        + "}")

      // A result missing the resource field should get filtered (a bogus doc is often missing expected fields)
      val badResourceDatasetHit = new SearchHit(1, "46_3yu6-fka7", new Text("dataset"), emptySearchHitMap)
      badResourceDatasetHit.sourceRef(badResourceDatasetSource)
      badResourceDatasetHit.score(score)

      // A result with corrupted (unparseable) field should also get skipped (instead of raising)
      val badSocrataIdDatasetHit = new SearchHit(1, "46_3yu6-fka7", new Text("dataset"), emptySearchHitMap)
      badSocrataIdDatasetHit.sourceRef(badSocrataIdDatasetSource)
      badSocrataIdDatasetHit.score(score)

      val hits = Array[SearchHit](badResourceDatasetHit, badSocrataIdDatasetHit)
      val internalSearchHits = new SearchHits(hits, 3037, 1.0f)
      val internalSearchResponse = new InternalSearchResponse(
        internalSearchHits, new InternalAggregations(List.empty.asJava),
        new Suggest(List.empty.asJava), new SearchProfileShardResults(Map.empty[String, ProfileShardResult].asJava),
        false, false, 0)
      val badSearchResponse =  new SearchResponse(internalSearchResponse, "", 15, 15, 4, Array[ShardSearchFailure]())
      val domain = Domain(1, "tempuri.org", None, Some("Title"), Some("Temp Org"), isCustomerDomain = true, moderationEnabled = false, routingApprovalEnabled = false, lockedDown = false, apiLockedDown = false)

      val searchResults = Format.formatDocumentResponse(badSearchResponse, None, domainSet, FormatParamSet())
      val results = searchResults.results
      results.size should be(0)
    }
  }

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
      viewType: String,
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
        urls.get("permalink") should be(Some(expectedPermalink))
        urls.get("link") should be(Some(expectedSeolink))
        urls.get("previewImageUrl") should be(Some(expectedPreviewImageUrl))
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
      testLinks(Datatype(t.get(dt)), t.getOrElse(vt, ""), category, name, t.getOrElse(xp, xpDefault), t.getOrElse(xs, xsDefault))
    }

    // category tests
    Seq(None, Some("")).foreach { category =>
      val name = "this is a name"
      testLinks(Datatype("dataset"), "viewtype", category, name, "d", "dataset/this-is-a-name")
    }

    // name tests
    Seq(null, "").foreach { name =>
      val category = Some("this-is-a-category")
      testLinks(Datatype("dataset"), "viewtype", category, name, "d", "this-is-a-category/-")
    }

    // length test
    val longCategory = Some("A super long category name is not very likely but we will protect against it anyway")
    val longName = "More commonly customers may write a title that is excessively verbose and it will hit this limit"
    val limitedCategory = "A-super-long-category-name-is-not-very-likely-but-"
    val limitedName = "More-commonly-customers-may-write-a-title-that-is-"
    testLinks(Datatype("dataset"), "viewtype", longCategory, longName, "d", s"$limitedCategory/$limitedName")

    // unicode test
    val unicodeCategory = Some("بيانات عن الجدات")
    val unicodeName = "愛"
    testLinks(Datatype("dataset"), "viewtype", unicodeCategory, unicodeName, "d", "بيانات-عن-الجدات/愛")

    "return requested locale if specified" in {
      val cname = "fu.bar"
      val locale = "pirate"
      val id = "1234-abcd"
      val name = "Pirates are awesome"

      val expectedPermaLink = s"https://$cname/$locale/d/$id"
      val expectedSeoLink = s"https://$cname/$locale/dataset/${Format.hyphenize(name)}/$id"

      val urls = Format.links(cname, Some(locale), None, "viewtype", id, None, name, None)

      urls("permalink") should be(expectedPermaLink)
      urls("link") should be(expectedSeoLink)
    }
  }

  "the domainPrivateMetadata method" should {
    val viewsDomainId = 0
    val privateMetadata = List(CustomerMetadataFlattened("No looky", "Private-Metadata_Thing"))
    val nonStory = docs.filter(d => d.socrataId.domainId == 0 && !d.isStory).headOption.get
    val story = docs.filter(d => d.socrataId.domainId == 0 && d.isStory).headOption.get
    val docWithPrivateMetadata = nonStory.copy(privateCustomerMetadataFlattened = privateMetadata)
    val storyWithPrivateMetadata = story.copy(privateCustomerMetadataFlattened = privateMetadata)

    "return None if no user is provided" in {
      Format.domainPrivateMetadata(docWithPrivateMetadata, None, viewsDomainId) should be(None)
    }

    "return None if the user doesn't own/share it and has no edit rights for non-stories" in {
      Format.domainPrivateMetadata(docWithPrivateMetadata, Some(userWithAllTheStoriesRights(viewsDomainId)), viewsDomainId) should be(None)
      Format.domainPrivateMetadata(docWithPrivateMetadata, Some(userWithAllTheViewRights(viewsDomainId)), viewsDomainId) should be(None)
      Format.domainPrivateMetadata(docWithPrivateMetadata, Some(userWithOnlyManageUsersRight(viewsDomainId)), viewsDomainId) should be(None)
      Format.domainPrivateMetadata(docWithPrivateMetadata, Some(userWithRoleButNoRights(viewsDomainId)), viewsDomainId) should be(None)
      Format.domainPrivateMetadata(docWithPrivateMetadata, Some(userWithNoRoleAndNoRights(viewsDomainId)), viewsDomainId) should be(None)
    }

    "return None if the user doesn't own/share it and has no edit rights for stories" in {
      Format.domainPrivateMetadata(storyWithPrivateMetadata, Some(userWithAllTheNonStoriesRights(viewsDomainId)), viewsDomainId) should be(None)
      Format.domainPrivateMetadata(storyWithPrivateMetadata, Some(userWithAllTheViewRights(viewsDomainId)), viewsDomainId) should be(None)
      Format.domainPrivateMetadata(storyWithPrivateMetadata, Some(userWithOnlyManageUsersRight(viewsDomainId)), viewsDomainId) should be(None)
      Format.domainPrivateMetadata(storyWithPrivateMetadata, Some(userWithRoleButNoRights(viewsDomainId)), viewsDomainId) should be(None)
      Format.domainPrivateMetadata(storyWithPrivateMetadata, Some(userWithNoRoleAndNoRights(viewsDomainId)), viewsDomainId) should be(None)
    }

    "return None if the user has edit rights for non-stories, but it isn't on the view's domain" in {
      Format.domainPrivateMetadata(docWithPrivateMetadata, Some(userWithAllTheRights(8)), viewsDomainId) should be(None)
      Format.domainPrivateMetadata(docWithPrivateMetadata, Some(userWithAllTheNonStoriesRights(8)), viewsDomainId) should be(None)
    }

    "return None if the user has edit rights for stories, but it isn't on the view's domain" in {
      Format.domainPrivateMetadata(storyWithPrivateMetadata, Some(userWithAllTheRights(8)), viewsDomainId) should be(None)
      Format.domainPrivateMetadata(storyWithPrivateMetadata, Some(userWithAllTheStoriesRights(8)), viewsDomainId) should be(None)
    }

    "return the expected value if the user owns the view" in {
      val user = Some(AuthedUser("robin-hood", domains(0)))
      val metadata = Format.domainPrivateMetadata(docWithPrivateMetadata, user, viewsDomainId)
      metadata should be(Some(privateMetadata))
    }

    "return the expected value if the user shares the view" in {
      val user = Some(AuthedUser("friar-tuck", domains(1)))
      val sharedDoc = docs.filter(d => d.socrataId.domainId == 1 && d.isSharedBy("friar-tuck")).headOption.get
      val metadata = Format.domainPrivateMetadata(sharedDoc.copy(privateCustomerMetadataFlattened = privateMetadata), user, viewsDomainId)
      metadata should be(Some(privateMetadata))
    }

    "return the expected value if the user is a super admin" in {
      val metadata = Format.domainPrivateMetadata(docWithPrivateMetadata, Some(superAdminUser(viewsDomainId)), viewsDomainId)
      metadata should be(Some(privateMetadata))
    }

    "return the expected value if the user has both edit rights on the view's domain" in {
      val metadata = Format.domainPrivateMetadata(docWithPrivateMetadata, Some(userWithAllTheRights(viewsDomainId)), viewsDomainId)
      metadata should be(Some(privateMetadata))
    }

    "return the expected value if the user has the non-stories edit right on the view's domain and we are looking at a non-story" in {
      Format.domainPrivateMetadata(docWithPrivateMetadata, Some(userWithAllTheRights(viewsDomainId)), viewsDomainId) should be(Some(privateMetadata))
      Format.domainPrivateMetadata(docWithPrivateMetadata, Some(userWithAllTheNonStoriesRights(viewsDomainId)), viewsDomainId) should be(Some(privateMetadata))
    }

    "return the expected val if the user has the stories edit right on the view's domain and we are looking at a story" in {
      Format.domainPrivateMetadata(storyWithPrivateMetadata, Some(userWithAllTheRights(viewsDomainId)), viewsDomainId) should be(Some(privateMetadata))
      Format.domainPrivateMetadata(storyWithPrivateMetadata, Some(userWithAllTheStoriesRights(viewsDomainId)), viewsDomainId) should be(Some(privateMetadata))
    }
  }

  "the moderationStatus method" should {
    "return None if the domain does not have moderation enabled" in {
      val unmoderatedDomain = domains(0)
      Format.moderationStatus(docs(0), unmoderatedDomain) should be(None)
    }

    "return the appropriate status for a derived view if domain has moderation enabled" in {
      val moderatedDomain = domains(1)
      Format.moderationStatus(docs(1).copy(moderationStatus = Some("approved"), datatype = "chart"), moderatedDomain) should be(Some("approved"))
      Format.moderationStatus(docs(1).copy(moderationStatus = Some("pending"), datatype = "chart"), moderatedDomain) should be(Some("pending"))
      Format.moderationStatus(docs(1).copy(moderationStatus = Some("rejected"), datatype = "chart"), moderatedDomain) should be(Some("rejected"))
    }

    "return 'approved' if the view is a dataset and the domain has moderation enabled" in {
      val moderatedDomain = domains(1)
      Format.moderationStatus(docs(1).copy(isDefaultView = true, datatype = "dataset"), moderatedDomain) should be(Some("approved"))
    }
  }

  "the moderationApproved method" should {
    "return None if the domain does not have moderation enabled" in {
      val unmoderatedDomain = domains(0)
      Format.moderationApproved(docs(0).copy(moderationStatus = Some("approved"), datatype = "chart"), unmoderatedDomain) should be(None)
    }

    "return false if the view is rejected and the domain has moderation enabled" in {
      val moderatedDomain = domains(1)
      Format.moderationApproved(docs(1).copy(moderationStatus = Some("rejected"), datatype = "chart"), moderatedDomain) should be(Some(false))
    }

    "return false if the view is pending and the domain has moderation enabled" in {
      val moderatedDomain = domains(1)
      Format.moderationApproved(docs(1).copy(moderationStatus = Some("pending"), datatype = "chart"), moderatedDomain) should be(Some(false))
    }

    "return true if the view is a dataset and the domain has moderation enabled" in {
      val moderatedDomain = domains(1)
      Format.moderationApproved(docs(1).copy(isDefaultView = true, datatype = "dataset"), moderatedDomain) should be(Some(true))
    }

    "return true if the view is approved and the domain has moderation enabled)" in {
      val moderatedDomain = domains(1)
      Format.moderationApproved(docs(1).copy(moderationStatus = Some("approved"), datatype = "chart"), moderatedDomain) should be(Some(true))
    }
  }

  "the moderationApprovedByContext method" should {
    "return None if the context doesn't have view moderation" in {
      val view = docs(1).copy(moderationStatus = Some("approved"), datatype = "chart")
      val unmoderatedDomain0 = domains(0)
      val unmoderatedDomain2 = domains(2)
      val moderatedDomain = domains(1)
      val context = DomainSet(searchContext = Some(unmoderatedDomain0))
      Format.moderationApprovedByContext(view, unmoderatedDomain2, context) should be(None)
      Format.moderationApprovedByContext(view, moderatedDomain, context) should be(None)
    }

    "return false if the view is rejected and the domain and context have moderation enabled" in {
      val view = docs(1).copy(moderationStatus = Some("rejected"), datatype = "chart")
      val moderatedDomain1 = domains(1)
      val moderatedDomain3 = domains(3)
      val context = DomainSet(searchContext = Some(moderatedDomain1))
      Format.moderationApprovedByContext(view, moderatedDomain3, context) should be(Some(false))
    }

    "return false if the view is pending and the domain and context have moderation enabled" in {
      val view = docs(1).copy(moderationStatus = Some("pending"), datatype = "chart")
      val moderatedDomain1 = domains(1)
      val moderatedDomain3 = domains(3)
      val context = DomainSet(searchContext = Some(moderatedDomain1))
      Format.moderationApprovedByContext(view, moderatedDomain3, context) should be(Some(false))
    }

    "return true if the view is a dataset and the domain and context have moderation enabled" in {
      val view = docs(1).copy(isDefaultView = true, datatype = "dataset")
      val moderatedDomain1 = domains(1)
      val moderatedDomain3 = domains(3)
      val context = DomainSet(searchContext = Some(moderatedDomain1))
      Format.moderationApprovedByContext(view, moderatedDomain3, context) should be(Some(true))
    }

    "return true if the view is approved and the domain and context have moderation enabled" in {
      val view = docs(1).copy(moderationStatus = Some("approved"), datatype = "chart")
      val moderatedDomain1 = domains(1)
      val moderatedDomain3 = domains(3)
      val context = DomainSet(searchContext = Some(moderatedDomain1))
      Format.moderationApprovedByContext(view, moderatedDomain3, context) should be(Some(true))
    }

    "return false if the view is not default and the context has moderation enabled, but not the view's domain" in {
      val view = docs(0).copy(isDefaultView = false, datatype = "chart")
      val moderatedDomain = domains(1)
      val viewsDomain = domains(0)
      val context = DomainSet(searchContext = Some(moderatedDomain))
      Format.moderationApprovedByContext(view, viewsDomain, context) should be(Some(false))
    }

    "return true if the view is a dataset and the context has moderation enabled, but not the view's domain" in {
      val view = docs(0).copy(isDefaultView = true, datatype = "dataset")
      val moderatedDomain = domains(1)
      val viewsDomain = domains(0)
      val context = DomainSet(searchContext = Some(moderatedDomain))
      Format.moderationApprovedByContext(view, viewsDomain, context) should be(Some(true))
    }
  }


  "the routingStatus method" should {
    "return None if the domain does not have R&A enabled" in {
      val unroutedDomain = domains(0)
      Format.routingStatus(docs(0), unroutedDomain) should be(None)
    }

    "return the appropriate status if domain has R&A enabled" in {
      val routedDomain = domains(2)
      Format.routingStatus(docs(2).copy(datatype = "dataset", isApprovedByParentDomain = true), routedDomain) should be(Some("approved"))
      Format.routingStatus(docs(2).copy(datatype = "dataset", isPendingOnParentDomain = true), routedDomain) should be(Some("pending"))
      Format.routingStatus(docs(2).copy(datatype = "dataset", isPendingOnParentDomain = false, isRejectedByParentDomain = true), routedDomain) should be(Some("rejected"))
    }
  }

  "the routingApproved method" should {
    "return None if the domain does not have R&A enabled" in {
      val unroutedDomain = domains(0)
      Format.routingApproved(docs(0), unroutedDomain) should be(None)
    }

    "return false if the dataset isn't approved by its parent domain" in {
      val unroutedDomain = domains(0)
      val routedDomain = domains(2)
      Format.routingApproved(docs(2).copy(datatype = "dataset", approvingDomainIds = Some(List(0))), routedDomain) should be(Some(false))
    }

    "return true if the dataset is approved by its parent domain" in {
      val routedDomain = domains(2)
      Format.routingApproved(docs(2).copy(datatype = "dataset", approvingDomainIds = Some(List(2))), routedDomain) should be(Some(true))
    }
  }

  "the routingApprovedByContext method" should {
    "return None if there is no search context regardless of parent domain" in {
      val unroutedDomain = domains(0)
      val routedDomain = domains(2)
      val noContext = DomainSet(searchContext = None)
      Format.routingApprovedByContext(docs(0).copy(datatype = "dataset", approvingDomainIds = Some(List(0, 2))), unroutedDomain, noContext) should be(None)
      Format.routingApprovedByContext(docs(2).copy(datatype = "dataset", approvingDomainIds = Some(List(0, 2))), routedDomain, noContext) should be(None)
    }

    "return None if the context doesn't have R&A enabled, regardless of parent domain" in {
      val unroutedDomain = domains(0)
      val routedDomain = domains(2)
      val unroutedContext = DomainSet(searchContext = Some(domains(1)))
      Format.routingApprovedByContext(docs(0).copy(datatype = "dataset", approvingDomainIds = Some(List(0, 2))), unroutedDomain, unroutedContext) should be(None)
      Format.routingApprovedByContext(docs(2).copy(datatype = "dataset", approvingDomainIds = Some(List(0, 2))), routedDomain, unroutedContext) should be(None)
    }

    "return false if the dataset isn't approved by the RA-enabled context" in {
      val viewsDomain = domains(3)
      val routedDomain = domains(2)
      val routedContext = DomainSet(searchContext = Some(routedDomain))

      Format.routingApprovedByContext(docs(3).copy(datatype = "dataset", approvingDomainIds = Some(List(3))), viewsDomain, routedContext) should be(Some(false))
    }

    "return true if the dataset is approved by the RA-enabled context" in {
      val viewsDomain = domains(3)
      val routedDomain = domains(2)
      val routedContext = DomainSet(searchContext = Some(routedDomain))

      Format.routingApprovedByContext(docs(3).copy(datatype = "dataset", approvingDomainIds = Some(List(2, 3))), viewsDomain, routedContext) should be(Some(true))
    }
  }

  "the contextApprovals method" should {
    "return None for both VM and R&A if the view's domainId is the same as the search context's id" in {
      val view = docs(1).copy(moderationStatus = Some("approved"), datatype = "chart")
      val moderatedDomain = domains(1)
      val context = DomainSet(searchContext = Some(moderatedDomain))
      val (vmContextApproval, raContextApproval) = Format.contextApprovals(view, moderatedDomain, context)
      vmContextApproval should be(None)
      raContextApproval should be(None)
    }

    "return the expected VM and R&A context approvals if the view's domainId is not the same as the search context's id" in {
      val view = docs(1).copy(datatype = "dataset", isDefaultView = true, approvingDomainIds = Some(List(3)))
      val viewsDomain = domains(1)
      val contextDomain = domains(3)
      val context = DomainSet(searchContext = Some(contextDomain))
      val (vmContextApproval, raContextApproval) = Format.contextApprovals(view, viewsDomain, context)
      vmContextApproval should be(Some(true))
      raContextApproval should be(Some(true))
    }
  }

  "the documentSearchResult method" should {
    val drewDoc = JsonUtil.parseJson[Document](drewRawString).right.get

    "return the expected payload if passed good json" in {
      val unmoderatedUnroutedContext = DomainSet(domains = Set(domains(0)), searchContext = Some(domains(0)))
      val actualResult = Format.documentSearchResult(drewDoc, None, unmoderatedUnroutedContext, None, Some(.98F), true).get
      val drewFormattedString = Source.fromInputStream(getClass.getResourceAsStream("/drewFormatted.json")).getLines().mkString("\n")
      val drewFormattedJson = JsonReader.fromString(drewFormattedString)

      val expectedResult = JsonDecode.fromJValue[SearchResult](drewFormattedJson).right.get
      actualResult.resource should be(expectedResult.resource)
      actualResult.link should be(expectedResult.link)
      actualResult.permalink should be(expectedResult.permalink)
      actualResult.metadata should be(expectedResult.metadata)
      actualResult.classification should be(expectedResult.classification)
    }

    "return the expected payload if passed good json and a user with rights to see the private metadata" in {
      val unmoderatedUnroutedContext = DomainSet(domains = Set(domains(0)), searchContext = Some(domains(0)))
      val actualResult = Format.documentSearchResult(drewDoc, Some(userWithAllTheRights(0)), unmoderatedUnroutedContext, None, Some(.98F), true).get
      val drewFormattedString = Source.fromInputStream(getClass.getResourceAsStream("/drewFormattedWithPrivateMetadata.json")).getLines().mkString("\n")
      val drewFormattedJson = JsonReader.fromString(drewFormattedString)
      val expectedResult = JsonDecode.fromJValue[SearchResult](drewFormattedJson).right.get

      actualResult.classification.domainPrivateMetadata should be(expectedResult.classification.domainPrivateMetadata)
    }
  }
}
