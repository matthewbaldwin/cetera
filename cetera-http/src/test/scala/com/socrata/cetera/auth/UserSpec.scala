package com.socrata.cetera.auth

import org.scalatest.{ShouldMatchers, WordSpec}

import com.socrata.cetera.TestESUsers

class UserSpec extends WordSpec with ShouldMatchers with TestESUsers {

  "The authorizedOnDomain method" should {
    val domId = 0

    "return false if the user is trying to authenticate on a different domain" in  {
      val otherDomainId = 1
      userWithAllTheRights(domId).authorizedOnDomain(otherDomainId) should be(false)
      userWithAllTheStoriesRights(domId).authorizedOnDomain(otherDomainId) should be(false)
      userWithAllTheNonStoriesRights(domId).authorizedOnDomain(otherDomainId) should be(false)
      userWithAllTheViewRights(domId).authorizedOnDomain(otherDomainId) should be(false)
      userWithOnlyManageUsersRight(domId).authorizedOnDomain(otherDomainId) should be(false)
      userWithRoleButNoRights(domId).authorizedOnDomain(otherDomainId) should be(false)
      userWithNoRoleAndNoRights(domId).authorizedOnDomain(otherDomainId) should be(false)
    }

    "return true if the user is a super admin" in {
      domains.foreach { d =>
        superAdminUser(domId).authorizedOnDomain(d.domainId) should be(true)
      }
    }

    "return true if the user is trying to authenticate on that domain" in  {
      userWithAllTheRights(domId).authorizedOnDomain(domId) should be(true)
      userWithAllTheStoriesRights(domId).authorizedOnDomain(domId) should be(true)
      userWithAllTheNonStoriesRights(domId).authorizedOnDomain(domId) should be(true)
      userWithAllTheViewRights(domId).authorizedOnDomain(domId) should be(true)
      userWithOnlyManageUsersRight(domId).authorizedOnDomain(domId) should be(true)
      userWithRoleButNoRights(domId).authorizedOnDomain(domId) should be(true)
      userWithNoRoleAndNoRights(domId).authorizedOnDomain(domId) should be(true)
    }
  }

  "The canViewResource method" should {
    val domId = 0
    "return false if the user is asking about a resource on a domain that is not their authenticating domain" in  {
      val otherDomainId = 1
      userWithAllTheRights(domId).canViewResource(otherDomainId, true) should be(false)
      userWithAllTheStoriesRights(domId).canViewResource(otherDomainId, true) should be(false)
      userWithAllTheNonStoriesRights(domId).canViewResource(otherDomainId, true) should be(false)
      userWithAllTheViewRights(domId).canViewResource(otherDomainId, true) should be(false)
      userWithOnlyManageUsersRight(domId).canViewResource(otherDomainId, true) should be(false)
      userWithRoleButNoRights(domId).canViewResource(otherDomainId, true) should be(false)
      userWithNoRoleAndNoRights(domId).canViewResource(otherDomainId, true) should be(false)
    }

    "return false if the user is asking about a resource on a domain that is their authenticating domain, but they aren't authorized" in  {
      userWithAllTheRights(domId).canViewResource(domId, false) should be(false)
      userWithAllTheStoriesRights(domId).canViewResource(domId, false) should be(false)
      userWithAllTheNonStoriesRights(domId).canViewResource(domId, false) should be(false)
      userWithAllTheViewRights(domId).canViewResource(domId, false) should be(false)
      userWithOnlyManageUsersRight(domId).canViewResource(domId, false) should be(false)
      userWithRoleButNoRights(domId).canViewResource(domId, false) should be(false)
      userWithNoRoleAndNoRights(domId).canViewResource(domId, false) should be(false)
    }

    "return true if the user is a super admin" in  {
      domains.foreach { d =>
        superAdminUser(d.domainId).canViewResource(d.domainId, isAuthorized = false) should be(true)
      }
    }

    "return true if the user is trying to authenticate on that domain and is authorized" in  {
      val authingDomainId = 0
      userWithAllTheRights(authingDomainId).canViewResource(authingDomainId, true) should be(true)
      userWithAllTheStoriesRights(authingDomainId).canViewResource(authingDomainId, true) should be(true)
      userWithAllTheNonStoriesRights(authingDomainId).canViewResource(authingDomainId, true) should be(true)
      userWithAllTheViewRights(authingDomainId).canViewResource(authingDomainId, true) should be(true)
      userWithOnlyManageUsersRight(authingDomainId).canViewResource(authingDomainId, true) should be(true)
      userWithRoleButNoRights(authingDomainId).canViewResource(authingDomainId, true) should be(true)
      userWithNoRoleAndNoRights(authingDomainId).canViewResource(authingDomainId, true) should be(true)
    }
  }

  "The canViewAllViews method" should {
    val domId = 6

    "return true if the user is a super admin" in {
      superAdminUser(domId).canViewAllViews(domId) should be(true)
    }

    "return true if asking about the user's authenticating domain and the user can view both stories and non-stories" in {
      userWithAllTheRights(domId).canViewAllViews(domId) should be(true)
      userWithAllTheViewRights(domId).canViewAllViews(domId) should be(true)
    }

    "return false if asking about the user's authenticating domain but the user lacks the rights" in {
      userWithAllTheStoriesRights(domId).canViewAllViews(domId) should be(false)
      userWithAllTheNonStoriesRights(domId).canViewAllViews(domId) should be(false)
      userWithOnlyManageUsersRight(domId).canViewAllViews(domId) should be(false)
      userWithRoleButNoRights(domId).canViewAllViews(domId) should be(false)
      userWithNoRoleAndNoRights(domId).canViewAllViews(domId) should be(false)
    }

    "return false if asking about a domain other than the user's authenticating domain" in {
      val otherDomainId = 1
      userWithAllTheRights(otherDomainId).canViewAllViews(domId)should be(false)
      userWithAllTheStoriesRights(otherDomainId).canViewAllViews(domId)should be(false)
      userWithAllTheNonStoriesRights(otherDomainId).canViewAllViews(domId)should be(false)
      userWithAllTheViewRights(otherDomainId).canViewAllViews(domId)should be(false)
      userWithOnlyManageUsersRight(otherDomainId).canViewAllViews(domId)should be(false)
      userWithRoleButNoRights(otherDomainId).canViewAllViews(domId)should be(false)
      userWithNoRoleAndNoRights(otherDomainId).canViewAllViews(domId)should be(false)
    }
  }

  "The canViewAllOfSomeViews method" should {
    val domId = 6

    "return true if the user is a super admin" in {
      superAdminUser(domId).canViewAllOfSomeViews(domId, isStory = true) should be(true)
      superAdminUser(domId).canViewAllOfSomeViews(domId, isStory = false) should be(true)
    }

    "return true if asking about the user's authenticating domain and stories and the user can view stories" in {
      userWithAllTheRights(domId).canViewAllOfSomeViews(domId, isStory = true) should be(true)
      userWithAllTheStoriesRights(domId).canViewAllOfSomeViews(domId, isStory = true) should be(true)
      userWithAllTheViewRights(domId).canViewAllOfSomeViews(domId, isStory = true) should be(true)
    }

    "return true if asking about the user's authenticating domain and non-stories and the user can view non-stories" in {
      userWithAllTheRights(domId).canViewAllOfSomeViews(domId, isStory = false) should be(true)
      userWithAllTheNonStoriesRights(domId).canViewAllOfSomeViews(domId, isStory = false) should be(true)
      userWithAllTheViewRights(domId).canViewAllOfSomeViews(domId, isStory = false) should be(true)
    }

    "return false if asking about the user's authenticating domain and stories, but the user lacks the rights" in {
      userWithAllTheNonStoriesRights(domId).canViewAllOfSomeViews(domId, isStory = true) should be(false)
      userWithOnlyManageUsersRight(domId).canViewAllOfSomeViews(domId, isStory = true) should be(false)
      userWithRoleButNoRights(domId).canViewAllOfSomeViews(domId, isStory = true) should be(false)
      userWithNoRoleAndNoRights(domId).canViewAllOfSomeViews(domId, isStory = true) should be(false)
    }

    "return false if asking about the user's authenticating domain and non-stories, but the user lacks the rights" in {
      userWithAllTheStoriesRights(domId).canViewAllOfSomeViews(domId, isStory = false) should be(false)
      userWithOnlyManageUsersRight(domId).canViewAllOfSomeViews(domId, isStory = false) should be(false)
      userWithRoleButNoRights(domId).canViewAllOfSomeViews(domId, isStory = false) should be(false)
      userWithNoRoleAndNoRights(domId).canViewAllOfSomeViews(domId, isStory = false) should be(false)
    }

    "return false if asking about a domain other than the user's authenticating domain and stories" in {
      val otherDomainId = 1
      userWithAllTheRights(otherDomainId).canViewAllOfSomeViews(domId, isStory = true) should be(false)
      userWithAllTheStoriesRights(otherDomainId).canViewAllOfSomeViews(domId, isStory = true) should be(false)
      userWithAllTheNonStoriesRights(otherDomainId).canViewAllOfSomeViews(domId, isStory = true) should be(false)
      userWithAllTheViewRights(otherDomainId).canViewAllOfSomeViews(domId, isStory = true) should be(false)
      userWithOnlyManageUsersRight(otherDomainId).canViewAllOfSomeViews(domId, isStory = true) should be(false)
      userWithRoleButNoRights(otherDomainId).canViewAllOfSomeViews(domId, isStory = true) should be(false)
      userWithNoRoleAndNoRights(otherDomainId).canViewAllOfSomeViews(domId, isStory = true) should be(false)
    }

    "return false if asking about a domain other than the user's authenticating domain and non-stories" in {
      val otherDomainId = 1
      userWithAllTheRights(otherDomainId).canViewAllOfSomeViews(domId, isStory = false) should be(false)
      userWithAllTheStoriesRights(otherDomainId).canViewAllOfSomeViews(domId, isStory = false) should be(false)
      userWithAllTheNonStoriesRights(otherDomainId).canViewAllOfSomeViews(domId, isStory = false) should be(false)
      userWithAllTheViewRights(otherDomainId).canViewAllOfSomeViews(domId, isStory = false) should be(false)
      userWithOnlyManageUsersRight(otherDomainId).canViewAllOfSomeViews(domId, isStory = false) should be(false)
      userWithRoleButNoRights(otherDomainId).canViewAllOfSomeViews(domId, isStory = false) should be(false)
      userWithNoRoleAndNoRights(otherDomainId).canViewAllOfSomeViews(domId, isStory = false) should be(false)
    }
  }

  "The canViewAllPrivateMetadata method" should {
    val domId = 6

    "return true if the user is a super admin" in {
      superAdminUser(domId).canViewAllPrivateMetadata(domId) should be(true)
    }

    "return true if asking about the user's authenticating domain and the user can view both stories and non-stories metadata" in {
      userWithAllTheRights(domId).canViewAllPrivateMetadata(domId) should be(true)
    }

    "return false if asking about the user's authenticating domain but the user lacks the rights" in {
      userWithAllTheStoriesRights(domId).canViewAllPrivateMetadata(domId) should be(false)
      userWithAllTheNonStoriesRights(domId).canViewAllPrivateMetadata(domId) should be(false)
      userWithAllTheViewRights(domId).canViewAllPrivateMetadata(domId) should be(false)
      userWithOnlyManageUsersRight(domId).canViewAllPrivateMetadata(domId) should be(false)
      userWithRoleButNoRights(domId).canViewAllPrivateMetadata(domId) should be(false)
      userWithNoRoleAndNoRights(domId).canViewAllPrivateMetadata(domId) should be(false)
    }

    "return false if asking about a domain other than the user's authenticating domain" in {
      val otherDomainId = 1
      userWithAllTheRights(otherDomainId).canViewAllPrivateMetadata(domId)should be(false)
      userWithAllTheStoriesRights(otherDomainId).canViewAllPrivateMetadata(domId)should be(false)
      userWithAllTheNonStoriesRights(otherDomainId).canViewAllPrivateMetadata(domId)should be(false)
      userWithAllTheViewRights(otherDomainId).canViewAllPrivateMetadata(domId)should be(false)
      userWithOnlyManageUsersRight(otherDomainId).canViewAllPrivateMetadata(domId)should be(false)
      userWithRoleButNoRights(otherDomainId).canViewAllPrivateMetadata(domId)should be(false)
      userWithNoRoleAndNoRights(otherDomainId).canViewAllPrivateMetadata(domId)should be(false)
    }
  }

  "The canViewAllOfSomePrivateMetadata method" should {
    val domId = 6

    "return true if the user is a super admin" in {
      superAdminUser(domId).canViewAllOfSomePrivateMetadata(domId, isStory = true) should be(true)
      superAdminUser(domId).canViewAllOfSomePrivateMetadata(domId, isStory = false) should be(true)
    }

    "return true if asking about the user's authenticating domain and stories and the user can view stories metadata" in {
      userWithAllTheRights(domId).canViewAllOfSomePrivateMetadata(domId, isStory = true) should be(true)
      userWithAllTheStoriesRights(domId).canViewAllOfSomePrivateMetadata(domId, isStory = true) should be(true)
    }

    "return true if asking about the user's authenticating domain and non-stories and the user can view non-stories metadata" in {
      userWithAllTheRights(domId).canViewAllOfSomePrivateMetadata(domId, isStory = false) should be(true)
      userWithAllTheNonStoriesRights(domId).canViewAllOfSomePrivateMetadata(domId, isStory = false) should be(true)
    }

    "return false if asking about the user's authenticating domain and stories, but the user lacks the rights" in {
      userWithAllTheViewRights(domId).canViewAllOfSomePrivateMetadata(domId, isStory = true) should be(false)
      userWithAllTheNonStoriesRights(domId).canViewAllOfSomePrivateMetadata(domId, isStory = true) should be(false)
      userWithOnlyManageUsersRight(domId).canViewAllOfSomePrivateMetadata(domId, isStory = true) should be(false)
      userWithRoleButNoRights(domId).canViewAllOfSomePrivateMetadata(domId, isStory = true) should be(false)
      userWithNoRoleAndNoRights(domId).canViewAllOfSomePrivateMetadata(domId, isStory = true) should be(false)
    }

    "return false if asking about the user's authenticating domain and non-stories, but the user lacks the rights" in {
      userWithAllTheViewRights(domId).canViewAllOfSomePrivateMetadata(domId, isStory = false) should be(false)
      userWithAllTheStoriesRights(domId).canViewAllOfSomePrivateMetadata(domId, isStory = false) should be(false)
      userWithOnlyManageUsersRight(domId).canViewAllOfSomePrivateMetadata(domId, isStory = false) should be(false)
      userWithRoleButNoRights(domId).canViewAllOfSomePrivateMetadata(domId, isStory = false) should be(false)
      userWithNoRoleAndNoRights(domId).canViewAllOfSomePrivateMetadata(domId, isStory = false) should be(false)
    }

    "return false if asking about a domain other than the user's authenticating domain and stories" in {
      val otherDomainId = 1
      userWithAllTheRights(otherDomainId).canViewAllOfSomePrivateMetadata(domId, isStory = true) should be(false)
      userWithAllTheStoriesRights(otherDomainId).canViewAllOfSomePrivateMetadata(domId, isStory = true) should be(false)
      userWithAllTheNonStoriesRights(otherDomainId).canViewAllOfSomePrivateMetadata(domId, isStory = true) should be(false)
      userWithAllTheViewRights(otherDomainId).canViewAllOfSomePrivateMetadata(domId, isStory = true) should be(false)
      userWithOnlyManageUsersRight(otherDomainId).canViewAllOfSomePrivateMetadata(domId, isStory = true) should be(false)
      userWithRoleButNoRights(otherDomainId).canViewAllOfSomePrivateMetadata(domId, isStory = true) should be(false)
      userWithNoRoleAndNoRights(otherDomainId).canViewAllOfSomePrivateMetadata(domId, isStory = true) should be(false)
    }

    "return false if asking about a domain other than the user's authenticating domain and non-stories" in {
      val otherDomainId = 1
      userWithAllTheRights(otherDomainId).canViewAllOfSomeViews(domId, isStory = false) should be(false)
      userWithAllTheStoriesRights(otherDomainId).canViewAllOfSomeViews(domId, isStory = false) should be(false)
      userWithAllTheNonStoriesRights(otherDomainId).canViewAllOfSomeViews(domId, isStory = false) should be(false)
      userWithAllTheViewRights(otherDomainId).canViewAllOfSomeViews(domId, isStory = false) should be(false)
      userWithOnlyManageUsersRight(otherDomainId).canViewAllOfSomeViews(domId, isStory = false) should be(false)
      userWithRoleButNoRights(otherDomainId).canViewAllOfSomeViews(domId, isStory = false) should be(false)
      userWithNoRoleAndNoRights(otherDomainId).canViewAllOfSomeViews(domId, isStory = false) should be(false)
    }
  }

  "The canViewAllUsers method" should {
    val domId = 0
    "return true if the use is a super admin" in {
      superAdminUser(domId).canViewAllUsers should be(true)
    }

    "return true if the user has the manage_users right" in {
      userWithAllTheRights(domId).canViewAllUsers should be(true)
      userWithAllTheViewRights(domId).canViewAllUsers should be(true)
      userWithOnlyManageUsersRight(domId).canViewAllUsers should be(true)
    }

    "return false if the user lacks the manage_users right" in {
      userWithAllTheStoriesRights(domId).canViewAllUsers should be(false)
      userWithAllTheNonStoriesRights(domId).canViewAllUsers should be(false)
      userWithRoleButNoRights(domId).canViewAllUsers should be(false)
      userWithNoRoleAndNoRights(domId).canViewAllUsers should be(false)
    }
  }

  "The canViewLockedDownCatalog method" should {
    val lockedDomainId = 8

    "return true if the user is a super admin" in {
      superAdminUser(lockedDomainId).canViewLockedDownCatalog(lockedDomainId) should be(true)
    }

    "return true if the user has any role from the authenticating domain" in {
      userWithAllTheRights(lockedDomainId).canViewLockedDownCatalog(lockedDomainId) should be(true)
      userWithAllTheStoriesRights(lockedDomainId).canViewLockedDownCatalog(lockedDomainId) should be(true)
      userWithAllTheNonStoriesRights(lockedDomainId).canViewLockedDownCatalog(lockedDomainId) should be(true)
      userWithAllTheViewRights(lockedDomainId).canViewLockedDownCatalog(lockedDomainId) should be(true)
      userWithOnlyManageUsersRight(lockedDomainId).canViewLockedDownCatalog(lockedDomainId) should be(true)
      userWithRoleButNoRights(lockedDomainId).canViewLockedDownCatalog(lockedDomainId) should be(true)
    }

    "return false if the user has no role from the authenticating domain" in {
      userWithNoRoleAndNoRights(lockedDomainId).canViewLockedDownCatalog(lockedDomainId) should be(false)
    }

    "return false if the user has a role but it isn't from the authenticating domain" in {
      val otherDomainId = 1
      userWithAllTheRights(otherDomainId).canViewLockedDownCatalog(lockedDomainId) should be(false)
      userWithAllTheStoriesRights(otherDomainId).canViewLockedDownCatalog(lockedDomainId) should be(false)
      userWithAllTheNonStoriesRights(otherDomainId).canViewLockedDownCatalog(lockedDomainId) should be(false)
      userWithAllTheViewRights(otherDomainId).canViewLockedDownCatalog(lockedDomainId) should be(false)
      userWithOnlyManageUsersRight(otherDomainId).canViewLockedDownCatalog(lockedDomainId) should be(false)
      userWithRoleButNoRights(otherDomainId).canViewLockedDownCatalog(lockedDomainId) should be(false)
      userWithNoRoleAndNoRights(otherDomainId).canViewLockedDownCatalog(lockedDomainId) should be(false)
    }
  }

  "The canViewUsersOnDomain method" should {
    val domId = 0

    "return true if the user is a super admin" in {
      superAdminUser(domId).canViewLockedDownCatalog(domId) should be(true)
    }

    "return true if the user has any role from the authenticating domain" in {
      userWithAllTheRights(domId).canViewLockedDownCatalog(domId) should be(true)
      userWithAllTheStoriesRights(domId).canViewLockedDownCatalog(domId) should be(true)
      userWithAllTheNonStoriesRights(domId).canViewLockedDownCatalog(domId) should be(true)
      userWithAllTheViewRights(domId).canViewLockedDownCatalog(domId) should be(true)
      userWithOnlyManageUsersRight(domId).canViewLockedDownCatalog(domId) should be(true)
      userWithRoleButNoRights(domId).canViewLockedDownCatalog(domId) should be(true)
    }

    "return false if the user has no role from the authenticating domain" in {
      userWithNoRoleAndNoRights(domId).canViewLockedDownCatalog(domId) should be(false)
    }

    "return false if the user has a role but it isn't from the authenticating domain" in {
      val otherDomainId = 1
      userWithAllTheRights(otherDomainId).canViewLockedDownCatalog(domId) should be(false)
      userWithAllTheStoriesRights(otherDomainId).canViewLockedDownCatalog(domId) should be(false)
      userWithAllTheNonStoriesRights(otherDomainId).canViewLockedDownCatalog(domId) should be(false)
      userWithAllTheViewRights(otherDomainId).canViewLockedDownCatalog(domId) should be(false)
      userWithOnlyManageUsersRight(otherDomainId).canViewLockedDownCatalog(domId) should be(false)
      userWithRoleButNoRights(otherDomainId).canViewLockedDownCatalog(domId) should be(false)
      userWithNoRoleAndNoRights(otherDomainId).canViewLockedDownCatalog(domId) should be(false)
    }
  }
}
