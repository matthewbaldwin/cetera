package com.socrata.cetera.auth

import org.scalatest.{ShouldMatchers, WordSpec}

import com.socrata.cetera.TestESDomains

class UserSpec extends WordSpec with ShouldMatchers with TestESDomains {
  val superAdmin = AuthedUser("", domains(0), roleName = None, rights = None, flags = Some(List("admin")))
  val customerAdmin = AuthedUser("", domains(8), roleName = Some("administrator"), rights = None, flags = None)
  val customerEditor = AuthedUser("", domains(8), roleName = Some("editor"), rights = None, flags = None)
  val customerEditorStories = AuthedUser("", domains(8), roleName = Some("editor_stories"), rights = None, flags = None)
  val customerPublisher = AuthedUser("", domains(8), roleName = Some("publisher"), rights = None, flags = None)
  val customerPublisherStories = AuthedUser("", domains(8), roleName = Some("publisher_stories"), rights = None, flags = None)
  val customerViewer = AuthedUser("", domains(8), roleName = Some("viewer"), rights = None, flags = None)
  val customerDesigner = AuthedUser("", domains(8), roleName = Some("designer"), rights = None, flags = None)

  "The authorizedOnDomain method" should {
    "return false if the user has an authenticating domain but is trying to authenticate on a different domain" in  {
      customerAdmin.authorizedOnDomain(1) should be(false)
    }

    "return true if the user has an authenticating domain and is trying to authenticate on that domain" in  {
      customerAdmin.authorizedOnDomain(8) should be(true)
    }
  }

  "The canViewResource method" should {
    "return false if the user has an authenticating domain but is trying to authenticate on a different domain" in  {
      customerAdmin.canViewResource(1, isAuthorized = true) should be(false)
    }

    "return false if the user has an authenticating domain and is trying to authenticate on that domain but is not authorized" in  {
      customerAdmin.canViewResource(8, isAuthorized = false) should be(false)
    }

    "return true if the user is a super admin" in  {
      domains.foreach { d =>
        superAdmin.canViewResource(d.domainId, isAuthorized = false) should be(true)
      }
    }

    "return true if the user has an authenticating domain and is trying to authenticate on that domain and is authorized" in  {
      customerAdmin.canViewResource(8, isAuthorized = true) should be(true)
    }
  }

  "The canViewLockedDownCatalog method" should {
    "return true if the user is a super admin" in {
      superAdmin.canViewLockedDownCatalog(6) should be(true)
    }

    "return true if the user has a supported role from the authenticating domain" in {
      customerAdmin.canViewLockedDownCatalog(8) should be(true)
      customerEditor.canViewLockedDownCatalog(8) should be(true)
      customerEditorStories.canViewLockedDownCatalog(8) should be(true)
      customerPublisher.canViewLockedDownCatalog(8) should be(true)
      customerPublisherStories.canViewLockedDownCatalog(8) should be(true)
      customerViewer.canViewLockedDownCatalog(8) should be(true)
    }

    "return false if the user has a role from the authenticating domain but it isn't supported" in {
      customerDesigner.canViewLockedDownCatalog(8) should be(false)
    }

    "return false if the user has a role but it isn't from the authenticating domain" in {
      customerAdmin.canViewLockedDownCatalog(7) should be(false)
      customerEditor.canViewLockedDownCatalog(7) should be(false)
      customerEditorStories.canViewLockedDownCatalog(7) should be(false)
      customerPublisher.canViewLockedDownCatalog(7) should be(false)
      customerPublisherStories.canViewLockedDownCatalog(7) should be(false)
      customerViewer.canViewLockedDownCatalog(7) should be(false)
      customerDesigner.canViewLockedDownCatalog(7) should be(false)
    }
  }

  "The canViewAllViews method" should {
    "return true if the user is a super admin" in {
      superAdmin.canViewAllViews(6) should be(true)
    }

    "return true if the user has a supported role from the authenticating domain" in {
      customerAdmin.canViewAllViews(8) should be(true)
      customerDesigner.canViewAllViews(8) should be(true)
      customerPublisher.canViewAllViews(8) should be(true)
      customerPublisherStories.canViewAllViews(8) should be(true)
      customerViewer.canViewAllViews(8) should be(true)
    }

    "return false if the user has a role from the authenticating domain but it isn't supported" in {
      customerEditor.canViewAllViews(8) should be(false)
      customerEditorStories.canViewAllViews(8) should be(false)
    }

    "return false if the user has a role but it isn't from the authenticating domain" in {
      customerAdmin.canViewAllViews(7) should be(false)
      customerEditor.canViewAllViews(7) should be(false)
      customerEditorStories.canViewAllViews(7) should be(false)
      customerPublisher.canViewAllViews(7) should be(false)
      customerPublisherStories.canViewAllViews(7) should be(false)
      customerViewer.canViewAllViews(7) should be(false)
      customerDesigner.canViewAllViews(7) should be(false)
    }
  }

  "The canViewUsers method" should {
    "return true if the use is a super admin" in {
      superAdmin.canViewAllUsers should be(true)
    }

    "return true if the user is a customer admin" in {
      customerAdmin.canViewAllUsers should be(true)
    }

    "return false if the user has any other role" in {
      customerEditor.canViewAllUsers should be(false)
      customerEditorStories.canViewAllUsers should be(false)
      customerPublisher.canViewAllUsers should be(false)
      customerPublisherStories.canViewAllUsers should be(false)
      customerViewer.canViewAllUsers should be(false)
    }
  }
}
