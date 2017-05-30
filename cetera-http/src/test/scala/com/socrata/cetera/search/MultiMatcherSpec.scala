package com.socrata.cetera.search

import com.rojoma.json.v3.io.JsonReader
import org.elasticsearch.index.query.MultiMatchQueryBuilder.Type.{CROSS_FIELDS, PHRASE}
import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, WordSpec}

import com.socrata.cetera.TestESDomains
import com.socrata.cetera.auth.User
import com.socrata.cetera.handlers.ScoringParamSet

class MultiMatcherSpec extends WordSpec with ShouldMatchers with BeforeAndAfterAll with TestESDomains {
  "buildQuery" should {
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

    "create a cross fields query over public fields if no user is passed" in {
      val actual = JsonReader.fromString(MultiMatchers.buildQuery(q, CROSS_FIELDS, ScoringParamSet(), None).toString)
      val expected = JsonReader.fromString(s"""
      {
        "multi_match" :
          {
            "query" : "some query string",
            "fields" : [ "fts_analyzed^1.0", "fts_raw^1.0" ],
            "type" : "cross_fields",
            $multiMatchDefaults
          }
      }
      """)
      actual should be(expected)
    }

    "create a phrase query over public fields if no user is passed" in {
      val actual = JsonReader.fromString(MultiMatchers.buildQuery(q, PHRASE, ScoringParamSet(), None).toString)
      val expected = JsonReader.fromString(s"""
      {
        "multi_match" :
          {
            "query" : "some query string",
            "fields" : [ "fts_analyzed^1.0", "fts_raw^1.0" ],
            "type" : "phrase",
            $multiMatchDefaults
          }
      }
      """)
      actual should be(expected)
    }

    "create a cross fields query over public fields and owned/shared items in private fields if the user is roleless" in {
      val user = User("annabelle", authenticatingDomain = Some(domains(0)))
      val actual = JsonReader.fromString(MultiMatchers.buildQuery(q, CROSS_FIELDS, ScoringParamSet(), Some(user)).toString)
      val expected = JsonReader.fromString(s"""
      {
          "bool": {
              "should": [
                  {
                      "multi_match": {
                          "query": "some query string",
                          "fields": ["fts_analyzed^1.0", "fts_raw^1.0"],
                          "type": "cross_fields",
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
                                      "type": "cross_fields",
                                      $multiMatchDefaults
                                  }
                              }
                          ],
                          "filter": [
                              {
                                  "bool": {
                                      "should": [
                                          {"term": {"owner.id": {"value": "annabelle", "boost": 1.0}}},
                                          {"term": {"shared_to": {"value": "annabelle", "boost": 1.0}}}
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
      """)
      actual should be(expected)
    }

    "create a phrase query over public fields and owned/shared items in private fields if the user is roleless" in {
      val user = User("annabelle", authenticatingDomain = Some(domains(0)))
      val actual = JsonReader.fromString(MultiMatchers.buildQuery(q, PHRASE, ScoringParamSet(), Some(user)).toString)
      val expected = JsonReader.fromString(s"""
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
                                          {"term": {"owner.id": {"value": "annabelle", "boost": 1.0}}},
                                          {"term": {"shared_to": {"value": "annabelle", "boost": 1.0}}}
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
      """)
      actual should be(expected)
    }

    "create a cross fields query over all fields for a super admin" in {
      val user = User("annabelle", flags = Some(List("admin")))
      val actual = JsonReader.fromString(
        MultiMatchers.buildQuery(q, CROSS_FIELDS, ScoringParamSet(), Some(user)).toString)
      val expected = JsonReader.fromString(s"""
      {
          "multi_match": {
              "query": "some query string",
              "fields": [
                  "fts_analyzed^1.0",
                  "fts_raw^1.0",
                  "private_fts_analyzed^1.0",
                  "private_fts_raw^1.0"
              ],
              "type": "cross_fields",
              $multiMatchDefaults
          }
      }
      """)
      actual should be(expected)
    }

    "create a phrase query over all fields for a super admin" in {
      val user = User("annabelle", flags = Some(List("admin")))
      val actual = JsonReader.fromString(
        MultiMatchers.buildQuery(q, PHRASE, ScoringParamSet(), Some(user)).toString)
      val expected = JsonReader.fromString(s"""
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
      """)
      actual should be(expected)
    }

    "create a cross fields query over public fields and all domain/owned/shared items in private fields if the user has an enabling role" in {
      val user = User("annabelle", authenticatingDomain = Some(domains(0)), roleName = Some("administrator"))
      val actual = JsonReader.fromString(
        MultiMatchers.buildQuery(q, CROSS_FIELDS, ScoringParamSet(), Some(user)).toString)
      val expected = JsonReader.fromString(s"""
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
                          "type": "cross_fields",
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
                                      "type": "cross_fields",
                                      $multiMatchDefaults
                                  }
                              }
                          ],
                          "filter": [
                              {
                                  "bool": {
                                      "should": [
                                          {"term": {"owner.id": {"value": "annabelle", "boost": 1.0}}},
                                          {"term": {"shared_to": {"value": "annabelle", "boost": 1.0}}},
                                          {"terms": {"socrata_id.domain_id": [0], "boost": 1.0}}
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
      """)
      actual should be(expected)
    }

    "create a phrase query over public fields and all domain/owned/shared items in private fields if the user has an enabling role" in {
      val user = User("annabelle", authenticatingDomain = Some(domains(0)), roleName = Some("administrator"))
      val actual = JsonReader.fromString(
        MultiMatchers.buildQuery(q, PHRASE, ScoringParamSet(), Some(user)).toString)
      val expected = JsonReader.fromString(s"""
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
                              {
                                  "bool": {
                                      "should": [
                                          {"term": {"owner.id": {"value": "annabelle", "boost": 1.0}}},
                                          {"term": {"shared_to": {"value": "annabelle", "boost": 1.0}}},
                                          {"terms": {"socrata_id.domain_id": [0], "boost": 1.0}}
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
      """)
      actual should be(expected)
    }
  }
}
