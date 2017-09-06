package com.socrata.cetera.search

import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.JsonReader
import org.scalatest.{ShouldMatchers, WordSpec}

import com.socrata.cetera.{TestESDomains, TestESUsers}
import com.socrata.cetera.auth.{AuthedUser, User}
import com.socrata.cetera.errors.UnauthorizedError
import com.socrata.cetera.handlers.{SearchParamSet, UserSearchParamSet}
import com.socrata.cetera.types.{ApprovalStatus, Domain, DomainSet, SimpleQuery}

class UserQueriesSpec extends WordSpec with ShouldMatchers with TestESUsers {

  "the idQuery" should {
    "return the expected query" in {
      val query = UserQueries.idQuery(Some(Set("foo-bar"))).get
      val actual = JsonReader.fromString(query.toString)
      val expected = j"""
      {
          "ids": {
              "type": ["user"],
              "values": ["foo-bar"],
              "boost": 1.0
          }
      }
      """
      actual should be(expected)
    }
  }

  "the domainQuery" should {
    "return the expected query" in {
      val query = UserQueries.domainQuery(Some(1)).get
      val actual = JsonReader.fromString(query.toString)
      val expected = j"""{"term": {"roles.domain_id": {"value": 1, "boost": 1.0}}}"""
      actual should be(expected)
    }
  }

  "the roleQuery" should {
    "return the expected query" in {
      val query = UserQueries.roleNameQuery(Some(Set("muffin", "scone"))).get
      val actual = JsonReader.fromString(query.toString)
      val expected = j"""{"terms": {"roles.role_name": ["muffin", "scone"], "boost": 1.0}}"""
      actual should be(expected)
    }
  }

  "the emailQuery" should {
    "return the expected query" in {
      val query = UserQueries.emailQuery(Some(Set("foo@baz.bar"))).get
      val actual = JsonReader.fromString(query.toString)
      val expected = j"""{"terms": {"email.raw": ["foo@baz.bar"], "boost": 1.0}}"""
      actual should be(expected)
    }
  }

  "the screenNameQuery" should {
    "return the expected query" in {
      val query = UserQueries.screenNameQuery(Some(Set("nombre"))).get
      val actual = JsonReader.fromString(query.toString)
      val expected = j"""{"terms": {"screen_name.raw": ["nombre"], "boost": 1.0}}"""
      actual should be(expected)
    }
  }

  "the flagQuery" should {
    "return the expected query" in {
      val query = UserQueries.flagQuery(Some(Set("admin"))).get
      val actual = JsonReader.fromString(query.toString)
      val expected = j"""{"terms" : {"flags" : ["admin"], "boost": 1.0}}"""
      actual should be(expected)
    }
  }

  "the authQuery" should {
    "throw an UnauthorizedError if the authorized user can view users but is attempting to do so on a domain they aren't authenticated on" in  {
      an[UnauthorizedError] should be thrownBy {
        UserQueries.authQuery(userWithAllTheRights(0), Some(domains(4)))
      }
    }

    "throw an UnauthorizedError if the authorized user has no role and lacks the manage_user rights" in  {
      an[UnauthorizedError] should be thrownBy {
        UserQueries.authQuery(userWithNoRoleAndNoRights(0), None)
      }
    }

    "return None for super admins" in {
      val query = UserQueries.authQuery(superAdminUser(0), None)
      query should be(None)
    }

    "return None for super admins even if searching for users on a domain that isn't their authenticating domain" in {
      val query = UserQueries.authQuery(superAdminUser(8), Some(domains(0)))
      query should be(None)
    }

    "return None for those with the manage_users right who aren't snooping around other's domains" in {
      val queryForUserWithAllRights = UserQueries.authQuery(userWithAllTheRights(0), None)
      val queryForUserWithAllViewRights = UserQueries.authQuery(userWithAllTheViewRights(0), None)
      val queryForUserWithManageUserRight = UserQueries.authQuery(userWithOnlyManageUsersRight(0), None)

      queryForUserWithAllRights should be(None)
      queryForUserWithAllViewRights should be(None)
      queryForUserWithManageUserRight should be(None)
    }

    "return a nested domainId query for roled users that lack the manage_user rights and who aren't querying a specific domain" in {
      val domId = 0
      val queryForUserWithStoriesRights = UserQueries.authQuery(userWithAllTheStoriesRights(domId), None)
      val queryForUserWithNonStoriesRights = UserQueries.authQuery(userWithAllTheNonStoriesRights(domId), None)
      val queryForUserWithRoleButNoRights = UserQueries.authQuery(userWithRoleButNoRights(domId), None)

      val expected = JsonReader.fromString(UserQueries.nestedRoleQuery(None, Some(domId)).get.toString)
      val actualForUserWithStoriesRights = JsonReader.fromString(queryForUserWithStoriesRights.get.toString)
      val actualForUserWithNonStoriesRights = JsonReader.fromString(queryForUserWithNonStoriesRights.get.toString)
      val actualForUserWithRoleButNoRights = JsonReader.fromString(queryForUserWithRoleButNoRights.get.toString)

      actualForUserWithStoriesRights should be(expected)
      actualForUserWithNonStoriesRights should be(expected)
      actualForUserWithRoleButNoRights should be(expected)
    }

    "return a nested domainId query for roled users that lack the manage_user rights and who are querying a specific domain, but it's their domain" in {
      val domId = 1
      val queryForUserWithStoriesRights = UserQueries.authQuery(userWithAllTheStoriesRights(domId), Some(domains(domId)))
      val queryForUserWithNonStoriesRights = UserQueries.authQuery(userWithAllTheNonStoriesRights(domId), Some(domains(domId)))
      val queryForUserWithRoleButNoRights = UserQueries.authQuery(userWithRoleButNoRights(domId), Some(domains(domId)))

      val expected = JsonReader.fromString(UserQueries.nestedRoleQuery(None, Some(domId)).get.toString)
      val actualForUserWithStoriesRights = JsonReader.fromString(queryForUserWithStoriesRights.get.toString)
      val actualForUserWithNonStoriesRights = JsonReader.fromString(queryForUserWithNonStoriesRights.get.toString)
      val actualForUserWithRoleButNoRights = JsonReader.fromString(queryForUserWithRoleButNoRights.get.toString)

      actualForUserWithStoriesRights should be(expected)
      actualForUserWithNonStoriesRights should be(expected)
      actualForUserWithRoleButNoRights should be(expected)
    }
  }

  "the compositeQuery" should {
    "return the expected query" in {
      val params = UserSearchParamSet(
        emails = Some(Set("admin@gmail.com")),
        screenNames = Some(Set("Ad men")),
        flags = Some(Set("admin")),
        roleNames = Some(Set("admin")),
        roleIds = Some(Set(2)))

      val user = AuthedUser("", domains(0), roleName = None, rights = None, flags = Some(List("admin")))
      val compositeQuery = UserQueries.compositeQuery(params, Some(domains(2)), user)
      val actual = JsonReader.fromString(compositeQuery.toString)
      val expected =j"""
        {
          "bool": {
            "must": [
              {"terms": {"email.raw": ["admin@gmail.com"], "boost": 1.0}},
              {"terms": {"screen_name.raw": ["ad men"], "boost": 1.0}},
              {"terms": {"flags": ["admin"], "boost": 1.0}},
              {
                "nested": {
                  "query": {
                    "bool": {
                      "must": [
                        {"term": {"roles.domain_id": {"value": 2, "boost": 1.0}}},
                        {"terms": {"roles.role_name": ["admin"], "boost": 1.0}}
                      ],
                      "disable_coord": false,
                      "adjust_pure_negative": true,
                      "boost": 1.0
                    }
                  },
                  "path": "roles",
                  "ignore_unmapped": false,
                  "score_mode": "avg",
                  "boost": 1.0
                }
              },
              {
                 "nested": {
                   "query": {
                     "bool": {
                       "must": [
                         {"term": {"roles.domain_id": {"value": 2, "boost": 1.0}}},
                         {"terms": {"roles.role_id": [2], "boost": 1.0}}
                       ],
                       "disable_coord": false,
                       "adjust_pure_negative": true,
                       "boost": 1.0
                     }
                   },
                   "path": "roles",
                   "ignore_unmapped": false,
                   "score_mode": "avg",
                   "boost": 1.0
                 }
               }
            ],
            "disable_coord": false,
            "adjust_pure_negative": true,
            "boost": 1.0
          }
        }
      """

      actual should be(expected)
    }
  }
}
