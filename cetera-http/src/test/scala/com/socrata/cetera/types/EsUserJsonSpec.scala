package com.socrata.cetera.types

import org.scalatest.{FunSuiteLike, Matchers}

import com.rojoma.json.v3.interpolation._

class UserInfoSpec extends FunSuiteLike with Matchers {
  test("ignore user email address when decoding a UserInfo from JSON") {
    val userJValue = j"""{
      "id": "abcd-1234",
      "email": "aardvark@socrata.com",
      "display_name": "Aardvark"
    }
    """

    val expected = Some(UserInfo("abcd-1234", Some("Aardvark")))
    UserInfo.fromJValue(userJValue) should be(expected)
  }
}

class EsUserJsonSpec extends FunSuiteLike with Matchers {
  test("decode user from elasticsearch json") {
    val source =
      """{
        |  "id": "abcd-1234",
        |  "email": "aardvark@socrata.com",
        |  "screen_name": "Aardvark",
        |  "roles": [
        |   { "domain_id": 0, "role_name": "janitor" }
        |  ]
        |}""".stripMargin
    val user = EsUser(source)

    user should be('defined)
    user.get.id should be("abcd-1234")
    user.get.screenName should be(Some("Aardvark"))
    user.get.email should be(Some("aardvark@socrata.com"))
    user.get.roles should be(Some(Set(Role(0, "janitor"))))
  }

  test("decode user from elasticsearch json, having no roles") {
    val source =
      """{
        |  "id": "abcd-1234",
        |  "email": "aardvark@socrata.com",
        |  "screen_name": "Aardvark",
        |  "roles": []
        |}""".stripMargin
    val user = EsUser(source)

    user should be('defined)
    user.get.id should be("abcd-1234")
    user.get.screenName should be(Some("Aardvark"))
    user.get.email should be(Some("aardvark@socrata.com"))
    user.get.roles should be(Some(Set.empty[Role]))
  }

  test("decode user from elasticsearch json, missing roles") {
    val source =
      """{
        |  "id": "abcd-1234",
        |  "email": "aardvark@socrata.com",
        |  "screen_name": "Aardvark"
        |}""".stripMargin
    val user = EsUser(source)

    user should be('defined)
    user.get.id should be("abcd-1234")
    user.get.screenName should be(Some("Aardvark"))
    user.get.email should be(Some("aardvark@socrata.com"))
    user.get.roles should be(None)
  }
}
