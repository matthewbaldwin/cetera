package com.socrata.cetera.search

import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.JsonReader
import org.scalatest.{ShouldMatchers, WordSpec}

import com.socrata.cetera.TestESDomains
import com.socrata.cetera.auth.User
import com.socrata.cetera.errors.UnauthorizedError
import com.socrata.cetera.handlers.{SearchParamSet, UserSearchParamSet}
import com.socrata.cetera.types.{ApprovalStatus, Domain, DomainSet, SimpleQuery}

class UserQueriesSpec extends WordSpec with ShouldMatchers with TestESDomains {

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
      val query = UserQueries.roleQuery(Some(Set("muffin", "scone"))).get
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
    "throw an UnauthorizedError if no user is given" in  {
      an[UnauthorizedError] should be thrownBy {
        UserQueries.authQuery(None, None)
      }
    }

    "throw an UnauthorizedError if user's domain doesn't match given domain id" in  {
      val user = User("mooks", Some(domains(6)), Some("administrator"))
      an[UnauthorizedError] should be thrownBy {
        UserQueries.authQuery(Some(user), Some(domains(4)))
      }
    }

    "throw an UnauthorizedError if user hasn't a role to view users" in  {
      val user = User("mooks", Some(domains(6)), Some(""))
      an[UnauthorizedError] should be thrownBy {
        UserQueries.authQuery(Some(user), None)
      }
    }

    "return None for super admins" in {
      val user = User("mooks", flags = Some(List("admin")))
      val query = UserQueries.authQuery(Some(user), None)
      query should be(None)
    }

    "return None for super admins even if searching for users on a domain that isn't their authenticating domain" in {
      val user = User("mooks", Some(domains(8)), flags = Some(List("admin")))
      val query = UserQueries.authQuery(Some(user), Some(domains(0)))
      query should be(None)
    }

    "return None for admins who aren't snooping around other's domains" in {
      val user = User("mooks", Some(domains(1)), Some("administrator"))
      val query = UserQueries.authQuery(Some(user), None)
      query should be(None)
    }

    "return a nested domainId query for non-admin roled users who aren't querying a specific domain" in {
      val user = User("mooks", Some(domains(1)), Some("i-have-a-role-really"))
      val expected = JsonReader.fromString(UserQueries.nestedRolesQuery(None, Some(1)).get.toString)
      val query = UserQueries.authQuery(Some(user), None)
      val actual = JsonReader.fromString(query.get.toString)
      actual should be(expected)
    }

    "return a nested domainId query for non-admin roled users who are querying a specific domain, but it's their domain" in {
      val user = User("mooks", Some(domains(1)), Some("i-have-a-role-really"))
      val expected = JsonReader.fromString(UserQueries.nestedRolesQuery(None, Some(1)).get.toString)
      val query = UserQueries.authQuery(Some(user), Some(domains(1)))
      val actual = JsonReader.fromString(query.get.toString)
      actual should be(expected)
    }
  }

  "the compositeQuery" should {
    "return the expected query" in {
      val params = UserSearchParamSet(
        emails = Some(Set("admin@gmail.com")),
        screenNames = Some(Set("Ad men")),
        flags = Some(Set("admin")),
        roles = Some(Set("admin")))

      val user = User("", None, roleName = None, rights = None, flags = Some(List("admin")))
      val compositeQuery = UserQueries.compositeQuery(params, Some(domains(2)), Some(user))
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
