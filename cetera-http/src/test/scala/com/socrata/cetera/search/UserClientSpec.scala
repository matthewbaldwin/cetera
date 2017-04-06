package com.socrata.cetera.search

import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

import com.socrata.cetera.auth.User
import com.socrata.cetera.errors.UnauthorizedError
import com.socrata.cetera.handlers.{PagingParamSet, UserSearchParamSet}
import com.socrata.cetera.types.Domain
import com.socrata.cetera.{TestESData, TestESUsers}

class UserClientSpec extends FunSuiteLike with Matchers with TestESData with TestESUsers with BeforeAndAfterAll {
  val userClient = new UserClient(client, testSuiteName)

  val superAdmin = User("", None, roleName = None, rights = None, flags = Some(List("admin")))
  val customerAdmin = User("", Some(domains(0)), roleName = Some("administrator"), rights = None, flags = None)
  val customerEditor = User("", Some(domains(0)), roleName = Some("editor"), rights = None, flags = None)
  val customerPublisher = User("", Some(domains(0)), roleName = Some("publisher"), rights = None, flags = None)
  val customerViewer = User("", Some(domains(0)), roleName = Some("viewer"), rights = None, flags = None)
  val customerDesigner = User("", Some(domains(0)), roleName = Some("designer"), rights = None, flags = None)
  val customerRoleless = User("", Some(domains(0)), roleName = None, rights = None, flags = None)
  val anonymous = User("", None, roleName = None, rights = None, flags = None)
  val adminWithoutAuthenticatingDomain = User("", None, roleName = Some("administrator"), rights = None, flags = None)

  override protected def beforeAll(): Unit = bootstrapData()

  override protected def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
    super.beforeAll()
  }

  test("fetch user by non-existent id, get a None") {
    val userRes = userClient.fetch("dead-beef")
    userRes should be('empty)
  }

  test("fetch user by id") {
    val userRes = userClient.fetch("soul-eater")
    userRes should be(Some(users(1)))
  }

  test("an UnauthorizedError should be thrown if no authorized user is given") {
    intercept[UnauthorizedError] {
      userClient.search(UserSearchParamSet(), PagingParamSet(), None, None)
    }
  }

  test("an UnauthorizedError should be thrown if the authorized user can't view users") {
    intercept[UnauthorizedError] {
      userClient.search(UserSearchParamSet(), PagingParamSet(), None, Some(customerRoleless))
    }
    intercept[UnauthorizedError] {
      userClient.search(UserSearchParamSet(), PagingParamSet(), None, Some(anonymous))
    }
  }

  test("an UnauthorizedError should be thrown if the authorized user can view users but is attempting to do so on a domain they aren't authenticated on") {
    intercept[UnauthorizedError] {
      userClient.search(UserSearchParamSet(domain = Some(domains(2).domainCname)), PagingParamSet(), Some(domains(2)), Some(customerAdmin))
    }
  }

  // the rest of these tests assume the user is authed properly and isn't up to no good.
  test("search returns all by default for superadmins") {
    val (userRes, totalCount, _) = userClient.search(UserSearchParamSet(), PagingParamSet(), None, Some(superAdmin))
    userRes should contain theSameElementsAs(users)
    totalCount should be(users.length)
  }

  test("search returns all by default for admins") {
    val (userRes, totalCount, _) = userClient.search(UserSearchParamSet(), PagingParamSet(), None, Some(customerAdmin))
    userRes should contain theSameElementsAs(users)
    totalCount should be(users.length)
  }

  test("search returns only users on the user's domain if they are authed properly but are not a superadmin") {
    val expectedUsers = users.filter(u => u.roleName(0).nonEmpty)
    val (resForViewer, _, _) = userClient.search(UserSearchParamSet(), PagingParamSet(), None, Some(customerViewer))
    val (resForDesigner, _, _) = userClient.search(UserSearchParamSet(), PagingParamSet(), None, Some(customerDesigner))
    val (resForEditor, _, _) = userClient.search(UserSearchParamSet(), PagingParamSet(), None, Some(customerEditor))
    val (resForPublisher, _, _) = userClient.search(UserSearchParamSet(), PagingParamSet(), None, Some(customerPublisher))

    resForViewer should contain theSameElementsAs(expectedUsers)
    resForDesigner should contain theSameElementsAs(expectedUsers)
    resForEditor should contain theSameElementsAs(expectedUsers)
    resForPublisher should contain theSameElementsAs(expectedUsers)
  }

  test("search by singular role") {
    val params = UserSearchParamSet(roles = Some(Set("bear")))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, Some(superAdmin))
    userRes should contain theSameElementsAs (Seq(users(3), users(4)))
    totalCount should be(2)
  }

  test("search by multiple roles") {
    val params = UserSearchParamSet(roles = Some(Set("bear", "headmaster")))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, Some(superAdmin))
    userRes should contain theSameElementsAs (Seq(users(1), users(3), users(4)))
    totalCount should be(3)
  }

  test("search by domain") {
    val domain = domains(1)
    val params = UserSearchParamSet(domain = Some(domain.domainCname))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), Some(domain), Some(superAdmin))
    userRes should contain theSameElementsAs (Seq(users(3), users(4), users(5), users(6)))
    totalCount should be(4)
  }

  test("search by singular email") {
    val params = UserSearchParamSet(emails = Some(Set("funshine.bear@care.alot")))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, Some(superAdmin))
    userRes should contain theSameElementsAs (Seq(users(3)))
    totalCount should be(1)
  }

  test("search by multiple emails") {
    val params = UserSearchParamSet(emails = Some(Set("funshine.bear@care.alot", "good.luck.bear@care.alot")))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, Some(superAdmin))
    userRes should contain theSameElementsAs (Seq(users(3), users(4)))
    totalCount should be(2)
  }

  test("search by singular screen name") {
    val params = UserSearchParamSet(screenNames = Some(Set("death")))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, Some(superAdmin))
    userRes should contain theSameElementsAs (Seq(users(0)))
    totalCount should be(1)
  }

  test("search by multiple screen names") {
    val params = UserSearchParamSet(screenNames = Some(Set("death", "dark-star")))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, Some(superAdmin))
    userRes should contain theSameElementsAs (Seq(users(0), users(2)))
    totalCount should be(2)
  }

  test("search by singular flag") {
    val params = UserSearchParamSet(flags = Some(Set("symmetrical")))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, Some(superAdmin))
    userRes should contain theSameElementsAs (Seq(users(0)))
    totalCount should be(1)
  }

  test("search by multiple flags") {
    val params = UserSearchParamSet(flags = Some(Set("yellow", "green")))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, Some(superAdmin))
    userRes should contain theSameElementsAs (Seq(users(3), users(4)))
    totalCount should be(2)
  }

  test("search by all the exact match conditions") {
    val params = UserSearchParamSet(
      emails = Some(Set("good.luck.bear@care.alot")),
      screenNames = Some(Set("Goodluck Bear")),
      roles = Some(Set("bear"))
    )
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, Some(superAdmin))
    userRes should contain theSameElementsAs (Seq(users(4)))
    totalCount should be(1)
  }

  test("search by querying exact name") {
    val params = UserSearchParamSet(query = Some("death the kid"))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, Some(superAdmin))
    userRes should contain theSameElementsAs (Seq(users(0), users(1)))
    userRes.head should be(users(1))  // tis the best match
    totalCount should be(2)
  }

  test("search by quering partial name") {
    val params = UserSearchParamSet(query = Some("kid"))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, Some(superAdmin))
    userRes should contain theSameElementsAs (Seq(users(1)))
    totalCount should be(1)
  }

  test("search by querying exact email") {
    val params = UserSearchParamSet(query = Some("death.kid@deathcity.com"))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, Some(superAdmin))
    userRes should contain theSameElementsAs (Seq(users(0), users(1), users(2)))
    userRes.head should be(users(1))  // tis the best match
    totalCount should be(3)
  }

  test("search by querying email alias") {
    val params = UserSearchParamSet(query = Some("death.kid"))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, Some(superAdmin))
    userRes should contain theSameElementsAs (Seq(users(0), users(1)))
    userRes.head should be(users(1))  // tis the best match
    totalCount should be(2)
  }

  test("search by querying email domain") {
    val params = UserSearchParamSet(query = Some("care.alot"))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, Some(superAdmin))
    userRes should contain theSameElementsAs (Seq(users(3), users(4)))
    totalCount should be(2)
  }

  test("search by querying strings with ES-reserved characters") {
    val reservedChars = List("+", "-", "&&", "&", "||", "|", "!", "(", ")", "{", "}", "[", "]", "^", "\"", "~", "*", "?", ":", "\\", "/")
    // nothing should blow up
    reservedChars.map { c =>
      userClient.search(UserSearchParamSet(query = Some(s"anu$c")), PagingParamSet(), None, Some(superAdmin))
    }
  }

  test("search by all the things") {
    val params = UserSearchParamSet(
      emails = Some(Set("good.luck.bear@care.alot")),
      screenNames = Some(Set("Goodluck Bear")),
      roles = Some(Set("bear")),
      query = Some("good luck")
    )
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, Some(superAdmin))
    userRes should contain theSameElementsAs (Seq(users(4)))
    totalCount should be(1)
  }

  test("search by non-existent role, get no results") {
    val params = UserSearchParamSet(roles = Some(Set("Editor")))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, Some(superAdmin))
    userRes should be('empty)
    totalCount should be(0)
  }

  test("search by non-existent domain, get no results") {
    val badDom = Domain(80, "non", None, None, true, true, true, true, true)
    val (userRes, totalCount, _) = userClient.search(UserSearchParamSet(), PagingParamSet(), Some(badDom), Some(superAdmin))
    userRes should be('empty)
    totalCount should be(0)
  }

  test("search by non-existent email, get no results") {
    val params = UserSearchParamSet(emails = Some(Set("bright.heart.racoon@care.alot")))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, Some(superAdmin))
    userRes should be('empty)
    totalCount should be(0)
  }

  test("search by non-existent screen name, get no results") {
    val params = UserSearchParamSet(screenNames = Some(Set("muffin")))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, Some(superAdmin))
    userRes should be('empty)
    totalCount should be(0)
  }

  test("search with paging limits") {
    val limit = 2
    val params = PagingParamSet(offset = 1, limit = limit)
    val (userRes, totalCount, _) = userClient.search(UserSearchParamSet(), params, None, Some(superAdmin))
    userRes.size should be(limit)
    totalCount should be(users.length)
  }
}
