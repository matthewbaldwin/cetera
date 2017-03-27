package com.socrata.cetera.search

import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.interpolation._
import org.elasticsearch.index.query.QueryBuilders
import org.scalatest.{ShouldMatchers, WordSpec}

import com.socrata.cetera.types.{Datatype, DatalensDatatype, DatasetDatatype, FilterDatatype}

class BoostsSpec extends WordSpec with ShouldMatchers {
  "datatypeBoosts" should {
    "return an empty List of FilterFunctions given an empty Map of datatype boosts" in {
      Boosts.datatypeBoostFunctions(Map.empty[Datatype, Float]) shouldBe empty
    }

    "return a non-empty List of FilterFunctions given a non-empty Map of datatype boosts" in {
      val datatypeBoosts = Map[Datatype, Float](
        DatasetDatatype -> 1.23f,
        DatalensDatatype -> 2.34f,
        FilterDatatype -> 0.98f
      )

      Boosts.datatypeBoostFunctions(datatypeBoosts).length shouldEqual(3)
    }

    "add datatype boosts to the query" in {      
      val datatypeBoosts = Map[Datatype, Float](
        DatasetDatatype -> 1.23f,
        DatalensDatatype -> 2.34f,
        FilterDatatype -> 0.98f
      )

      val boostFunctions = Boosts.datatypeBoostFunctions(datatypeBoosts)
      val query = QueryBuilders.functionScoreQuery(QueryBuilders.matchAllQuery, boostFunctions.toArray)
      val actual = JsonReader.fromString(query.toString)

      val expected = j"""
      {
          "function_score": {
              "functions": [
                  {
                      "filter": {
                          "term": {"datatype": {"value": "dataset", "boost": 1.0}}
                      },
                      "weight": 1.23
                  },
                  {
                      "filter": {
                          "term": {"datatype": {"value": "datalens", "boost": 1.0}}
                      },
                      "weight": 2.34
                  },
                  {
                      "filter": {
                          "term": {"datatype": {"value": "filter", "boost": 1.0}}
                      },
                      "weight": 0.98
                  }
              ],
              "score_mode": "multiply",
              "max_boost": 3.4028235E+38,
              "boost": 1.0,
              "query": {"match_all": {"boost": 1.0}}
          }
      }
      """

      actual should be (expected)
    }

    "do nothing to the query if given no datatype boosts" in {      
      val boostFunctions = Boosts.datatypeBoostFunctions(Map.empty[Datatype, Float])
      val query = QueryBuilders.functionScoreQuery(QueryBuilders.matchAllQuery, boostFunctions.toArray)
      val actual = JsonReader.fromString(query.toString)

      val expected = j"""
      {
          "function_score": {
              "functions": [],
              "score_mode": "multiply",
              "max_boost": 3.4028235E+38,
              "boost": 1.0,
              "query": {"match_all": {"boost": 1.0}}
          }
      }      
      """

      actual should be (expected)
    }
  }

  "boostDomains" should {
    "return an empty List of FilterFunctions given an empty Map of domain boosts" in {
      Boosts.domainBoostFunctions(Map.empty[Int, Float]) shouldBe empty
    }

    "return a non-empty List of FilterFunctions given a non-empty Map of domain boosts" in {
      val domainBoosts = Map[Int, Float](
        0 -> 1.23f,
        7 -> 4.56f
      )

      Boosts.domainBoostFunctions(domainBoosts).length shouldEqual(2)
    }

    "add domain cname boosts to the query" in {
      val domainBoosts = Map[Int, Float](
        0 -> 1.23f,
        7 -> 4.56f
      )

      val boostFunctions = Boosts.domainBoostFunctions(domainBoosts)
      val query = QueryBuilders.functionScoreQuery(QueryBuilders.matchAllQuery, boostFunctions.toArray)
      val actual = JsonReader.fromString(query.toString)

      val expected = j"""
      {
          "function_score": {
              "functions": [
                  {
                      "filter": {
                          "term": {"socrata_id.domain_id": {"value": 0, "boost": 1.0}}
                      },
                      "weight": 1.23
                  },
                  {
                      "filter": {
                          "term": {"socrata_id.domain_id": {"value": 7, "boost": 1.0}}
                      },
                      "weight": 4.56
                  }
              ],
              "score_mode": "multiply",
              "max_boost": 3.4028235E+38,
              "boost": 1.0,
              "query": {"match_all": {"boost": 1.0}}
          }
      }
      """

      actual should be (expected)
    }

    "do nothing to the query if given no domain boosts" in {
      val boostFunctions = Boosts.domainBoostFunctions(Map.empty[Int, Float])
      val query = QueryBuilders.functionScoreQuery(QueryBuilders.matchAllQuery, boostFunctions.toArray)
      val actual = JsonReader.fromString(query.toString)

      val expected = j"""
      {
          "function_score": {
              "functions": [],
              "score_mode": "multiply",
              "max_boost": 3.4028235E+38,
              "boost": 1.0,
              "query": {"match_all": {"boost": 1.0}}
          }
      }
      """

      actual should be(expected)
    }
  }
}
