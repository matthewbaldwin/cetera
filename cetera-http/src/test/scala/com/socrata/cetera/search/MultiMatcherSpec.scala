package com.socrata.cetera.search

import com.rojoma.json.v3.io.JsonReader
import org.elasticsearch.index.query.MultiMatchQueryBuilder.Type.{CROSS_FIELDS, PHRASE}
import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, WordSpec}

import com.socrata.cetera.{TestESDomains, TestESUsers}
import com.socrata.cetera.auth.{AuthedUser, User}
import com.socrata.cetera.handlers.ScoringParamSet

class MultiMatcherSpec extends WordSpec with ShouldMatchers with BeforeAndAfterAll with TestESUsers {
  val q = "some query string"

  val queryDefaults = """
    "disable_coord": false,
    "adjust_pure_negative": true,
    "boost": 1.0
  """

  val multiMatchDefaults = """
    "operator": "OR",
    "slop": 0,
    "prefix_length": 0,
    "max_expansions": 50,
    "lenient": false,
    "zero_terms_query": "NONE",
    "boost": 1.0
  """

  def matchPhraseQuery(testCondition: String, user: Option[AuthedUser], expectedPhraseQuery: String): Unit = {
    s"create a phrase query $testCondition" in {
      val actual = JsonReader.fromString(MultiMatchers.buildQuery(q, PHRASE, ScoringParamSet(), user).toString)
      val expected = JsonReader.fromString(expectedPhraseQuery)
      actual should be(expected)
    }
  }

  def matchCrossFieldsQuery(testCondition: String, user: Option[AuthedUser], expectedPhraseQuery: String): Unit = {
    s"create a cross fields query $testCondition" in {
      val actual = JsonReader.fromString(MultiMatchers.buildQuery(q, CROSS_FIELDS, ScoringParamSet(), user).toString)
      val expected = JsonReader.fromString(expectedPhraseQuery.replaceAll("phrase", "cross_fields"))
      actual should be(expected)
    }
  }

  "buildQuery when a super admin is passed " should {
    val expectedQuery =
      s"""
      {
        "multi_match": {
          "query": "some query string",
          "fields": [
          "fts_analyzed^1.0",
          "fts_raw^1.0",
          "private_fts_analyzed^1.0",
          "private_fts_raw^1.0"
          ],
          "type": "phrase",
          $multiMatchDefaults
        }
      }
      """
    val testCondition = "over all fields public and private"
    matchPhraseQuery(testCondition, Some(superAdminUser(0)), expectedQuery)
    matchCrossFieldsQuery(testCondition, Some(superAdminUser(0)), expectedQuery)
  }

  "buildQuery when a user with edit rights to both stories and non-stories is passed " should {
    val user = userWithAllTheRights(0)
    val filter = JsonReader.fromString(DocumentQuery().privateMetadataUserRestrictionsQuery(user).toString)
    val expectedQuery =
      s"""
      {
          "bool": {
              "should": [
                  {
                      "multi_match": {
                          "query": "some query string",
                          "fields": [
                              "fts_analyzed^1.0",
                              "fts_raw^1.0"
                          ],
                          "type": "phrase",
                          $multiMatchDefaults
                      }
                  },
                  {
                      "bool": {
                          "must": [
                              {
                                  "multi_match": {
                                      "query": "some query string",
                                      "fields": [
                                          "private_fts_analyzed^1.0",
                                          "private_fts_raw^1.0"
                                      ],
                                      "type": "phrase",
                                      $multiMatchDefaults
                                  }
                              }
                          ],
                          "filter": [
                              $filter
                          ],
                          $queryDefaults
                      }
                  }
              ],
              $queryDefaults
          }
      }
      """
    val testCondition = "over all public fields and private fields of docs they own/share and of docs on their domain"
    matchPhraseQuery(testCondition, Some(user), expectedQuery)
    matchCrossFieldsQuery(testCondition, Some(user), expectedQuery)
  }

  "buildQuery when a user with edit rights to only stories is passed " should {
    val user = userWithAllTheStoriesRights(0)
    val filter = JsonReader.fromString(DocumentQuery().privateMetadataUserRestrictionsQuery(user).toString)
    val expectedQuery =
      s"""
      {
          "bool": {
              "should": [
                  {
                      "multi_match": {
                          "query": "some query string",
                          "fields": [
                              "fts_analyzed^1.0",
                              "fts_raw^1.0"
                          ],
                          "type": "phrase",
                          $multiMatchDefaults
                      }
                  },
                  {
                      "bool": {
                          "must": [
                              {
                                  "multi_match": {
                                      "query": "some query string",
                                      "fields": [
                                          "private_fts_analyzed^1.0",
                                          "private_fts_raw^1.0"
                                      ],
                                      "type": "phrase",
                                      $multiMatchDefaults
                                  }
                              }
                          ],
                          "filter": [
                              $filter
                          ],
                          $queryDefaults
                      }
                  }
              ],
              $queryDefaults
          }
      }
      """
    val testCondition = "over all public fields and private fields of docs they own/share and of stories on their domain"
    matchPhraseQuery(testCondition, Some(user), expectedQuery)
    matchCrossFieldsQuery(testCondition, Some(user), expectedQuery)
  }

  "buildQuery when a user with edit rights to only non-stories is passed " should {
    val user = userWithAllTheNonStoriesRights(0)
    val filter = JsonReader.fromString(DocumentQuery().privateMetadataUserRestrictionsQuery(user).toString)
    val expectedQuery =
      s"""
      {
          "bool": {
              "should": [
                  {
                      "multi_match": {
                          "query": "some query string",
                          "fields": [
                              "fts_analyzed^1.0",
                              "fts_raw^1.0"
                          ],
                          "type": "phrase",
                          $multiMatchDefaults
                      }
                  },
                  {
                      "bool": {
                          "must": [
                              {
                                  "multi_match": {
                                      "query": "some query string",
                                      "fields": [
                                          "private_fts_analyzed^1.0",
                                          "private_fts_raw^1.0"
                                      ],
                                      "type": "phrase",
                                      $multiMatchDefaults
                                  }
                              }
                          ],
                          "filter": [
                              $filter
                          ],
                          $queryDefaults
                      }
                  }
              ],
              $queryDefaults
          }
      }
      """
    val testCondition = "over all public fields and private fields of docs they own/share and of non-stories on their domain"
    matchPhraseQuery(testCondition, Some(user), expectedQuery)
    matchCrossFieldsQuery(testCondition, Some(user), expectedQuery)
  }

  "buildQuery when user with no edit rights is passed " should {
    val expectedQuery =
      s"""
      {
          "bool": {
              "should": [
                  {
                      "multi_match": {
                          "query": "some query string",
                          "fields": ["fts_analyzed^1.0", "fts_raw^1.0"],
                          "type": "phrase",
                          $multiMatchDefaults
                      }
                  },
                  {
                      "bool": {
                          "must": [
                              {
                                  "multi_match": {
                                      "query": "some query string",
                                      "fields": ["private_fts_analyzed^1.0", "private_fts_raw^1.0"],
                                      "type": "phrase",
                                      $multiMatchDefaults
                                  }
                              }
                          ],
                          "filter": [
                              {
                                  "bool": {
                                      "should": [
                                          {"term": {"owner.id": {"value": "user-fxf", "boost": 1.0}}},
                                          {"term": {"shared_to": {"value": "user-fxf", "boost": 1.0}}}
                                      ],
                                      $queryDefaults
                                  }
                              }
                          ],
                          $queryDefaults
                      }
                  }
              ],
              $queryDefaults
          }
      }
      """
    val testCondition = "over all public fields and private fields of docs they own/share"
    matchPhraseQuery(s"$testCondition for a user with all viewing rights", Some(userWithAllTheViewRights(0)), expectedQuery)
    matchCrossFieldsQuery(s"$testCondition for a user with all viewing rights", Some(userWithAllTheViewRights(0)), expectedQuery)

    matchPhraseQuery(s"$testCondition for a user with only the manage_users right", Some(userWithOnlyManageUsersRight(0)), expectedQuery)
    matchCrossFieldsQuery(s"$testCondition for a user with only the manage_users right", Some(userWithOnlyManageUsersRight(0)), expectedQuery)

    matchPhraseQuery(s"$testCondition for a user with a role but no rights", Some(userWithRoleButNoRights(0)), expectedQuery)
    matchCrossFieldsQuery(s"$testCondition for a user with a role but no rights", Some(userWithRoleButNoRights(0)), expectedQuery)

    matchPhraseQuery(s"$testCondition for a user with no role and no rights", Some(userWithNoRoleAndNoRights(0)), expectedQuery)
    matchCrossFieldsQuery(s"$testCondition for a user with no role and no rights", Some(userWithNoRoleAndNoRights(0)), expectedQuery)
  }

  "buildQuery when no user is passed " should {
    val expectedQuery =
      s"""
      {
        "multi_match" :
          {
            "query" : "some query string",
            "fields" : [ "fts_analyzed^1.0", "fts_raw^1.0" ],
            "type" : "phrase",
            $multiMatchDefaults
          }
      }
      """
    val testCondition = "over all public fields"
    matchPhraseQuery(testCondition, user = None, expectedQuery)
    matchCrossFieldsQuery(testCondition, user = None, expectedQuery)
  }
}
