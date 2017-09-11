package com.socrata.cetera.search

import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

import com.socrata.cetera.auth.AuthedUser
import com.socrata.cetera.errors.UnauthorizedError
import com.socrata.cetera.handlers.{PagingParamSet, UserSearchParamSet}
import com.socrata.cetera.types.Domain
import com.socrata.cetera.{TestESData, TestESUsers}

class UserClientSpec extends FunSuiteLike with Matchers with TestESData with TestESUsers with BeforeAndAfterAll {
  val userClient = new UserClient(client, testSuiteName)
  val domainForRoles = domains(0)

  override protected def beforeAll(): Unit = bootstrapData()

  override protected def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
    super.beforeAll()
  }

  test("an UnauthorizedError should be thrown if the authorized user has no role and lacks the manage_user rights") {
    intercept[UnauthorizedError] {
      userClient.search(UserSearchParamSet(), PagingParamSet(), None, domainForRoles, userWithNoRoleAndNoRights(0))
    }
  }

  test("an UnauthorizedError should be thrown if the authorized user can view users but is attempting to do so on a domain they aren't authenticated on") {
    val domId = 0
    val otherDom = domains(1)
    intercept[UnauthorizedError] {
      userClient.search(UserSearchParamSet(domain = Some(otherDom.domainCname)), PagingParamSet(), Some(otherDom), otherDom, userWithAllTheRights(domId))
    }
  }

  // the rest of these tests assume the user is authed properly and isn't up to no good.
  test("search returns all by default for superadmins") {
    val (userRes, totalCount, _) = userClient.search(UserSearchParamSet(), PagingParamSet(), None, domainForRoles, superAdminUser(0))
    userRes should contain theSameElementsAs(users)
    totalCount should be(users.length)
  }

  test("search returns all by default for those with the manage_users right") {
    val (userResWithAllRights, _, _) = userClient.search(UserSearchParamSet(), PagingParamSet(), None, domainForRoles, userWithAllTheRights(0))
    val (userResWithAllViewRights, _, _) = userClient.search(UserSearchParamSet(), PagingParamSet(), None, domainForRoles, userWithAllTheViewRights(0))
    val (userResWithOnlyUserRights, _, _) = userClient.search(UserSearchParamSet(), PagingParamSet(), None, domainForRoles, userWithOnlyManageUsersRight(0))

    userResWithAllRights should contain theSameElementsAs(users)
    userResWithAllViewRights should contain theSameElementsAs(users)
    userResWithOnlyUserRights should contain theSameElementsAs(users)
  }

  test("search returns only users on the user's domain if they have a role but lack the manage_users right") {
    val domId = 0
    val expectedUsers = users.filter(u => u.roleName(domId).nonEmpty)
    val (userResWithAllStoriesRights, _, _) = userClient.search(UserSearchParamSet(), PagingParamSet(), None, domainForRoles, userWithAllTheStoriesRights(domId))
    val (userResWithAllNonStoriesRights, _, _) = userClient.search(UserSearchParamSet(), PagingParamSet(), None, domainForRoles, userWithAllTheNonStoriesRights(domId))
    val (userResWithOnlyRoleRights, _, _) = userClient.search(UserSearchParamSet(), PagingParamSet(), None, domainForRoles, userWithRoleButNoRights(domId))

    userResWithAllStoriesRights should contain theSameElementsAs(expectedUsers)
    userResWithAllNonStoriesRights should contain theSameElementsAs(expectedUsers)
    userResWithOnlyRoleRights should contain theSameElementsAs(expectedUsers)
  }

  test("search by singular roleName") {
    val params = UserSearchParamSet(roleNames = Some(Set("bear")))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, domainForRoles, superAdminUser(0))
    userRes should contain theSameElementsAs (Seq(users(3), users(4)))
    totalCount should be(2)
  }

  test("search by multiple roleNames") {
    val params = UserSearchParamSet(roleNames = Some(Set("bear", "headmaster")))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, domainForRoles, superAdminUser(0))
    userRes should contain theSameElementsAs (Seq(users(1), users(3), users(4)))
    totalCount should be(3)
  }

  test("search by singular roleId") {
    val dom = domains(1)
    val params = UserSearchParamSet(roleIds = Some(Set(4)), domain = Some(dom.domainCname))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), Some(dom), dom, superAdminUser(0))
    userRes should contain theSameElementsAs (Seq(users(3), users(4)))
    totalCount should be(2)
  }

  test("search by multiple roleId") {
    val dom = domains(1)
    val params = UserSearchParamSet(roleIds = Some(Set(4, 6)), domain = Some(dom.domainCname))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), Some(dom), dom, superAdminUser(0))
    userRes should contain theSameElementsAs (Seq(users(3), users(4), users(6)))
    totalCount should be(3)
  }

  test("search by domain") {
    val domain = domains(1)
    val params = UserSearchParamSet(domain = Some(domain.domainCname))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), Some(domain), domain, superAdminUser(domain.domainId))
    userRes should contain theSameElementsAs (Seq(users(3), users(4), users(5), users(6), users(7), users(8)))
    totalCount should be(6)
  }

  test("search by singular email") {
    val params = UserSearchParamSet(emails = Some(Set("funshine.bear@care.alot")))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, domainForRoles, superAdminUser(0))
    userRes should contain theSameElementsAs (Seq(users(3)))
    totalCount should be(1)
  }

  test("search by multiple emails") {
    val params = UserSearchParamSet(emails = Some(Set("funshine.bear@care.alot", "good.luck.bear@care.alot")))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, domainForRoles, superAdminUser(0))
    userRes should contain theSameElementsAs (Seq(users(3), users(4)))
    totalCount should be(2)
  }

  test("search by singular screen name") {
    val params = UserSearchParamSet(screenNames = Some(Set("death")))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, domainForRoles, superAdminUser(0))
    userRes should contain theSameElementsAs (Seq(users(0)))
    totalCount should be(1)
  }

  test("search by multiple screen names") {
    val params = UserSearchParamSet(screenNames = Some(Set("death", "dark-star")))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, domainForRoles, superAdminUser(0))
    userRes should contain theSameElementsAs (Seq(users(0), users(2)))
    totalCount should be(2)
  }

  test("search by singular flag") {
    val params = UserSearchParamSet(flags = Some(Set("symmetrical")))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, domainForRoles, superAdminUser(0))
    userRes should contain theSameElementsAs (Seq(users(0)))
    totalCount should be(1)
  }

  test("search by multiple flags") {
    val params = UserSearchParamSet(flags = Some(Set("yellow", "green")))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, domainForRoles, superAdminUser(0))
    userRes should contain theSameElementsAs (Seq(users(3), users(4)))
    totalCount should be(2)
  }

  test("search by all the exact match conditions") {
    val params = UserSearchParamSet(
      emails = Some(Set("good.luck.bear@care.alot")),
      screenNames = Some(Set("Goodluck Bear")),
      roleNames = Some(Set("bear"))
    )
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, domainForRoles, superAdminUser(0))
    userRes should contain theSameElementsAs (Seq(users(4)))
    totalCount should be(1)
  }

  test("search by querying exact name") {
    val params = UserSearchParamSet(query = Some("death the kid"))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, domainForRoles, superAdminUser(0))
    userRes should contain theSameElementsAs (Seq(users(0), users(1)))
    userRes.head should be(users(1))  // tis the best match
    totalCount should be(2)
  }

  test("search by quering partial name") {
    val params = UserSearchParamSet(query = Some("kid"))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, domainForRoles, superAdminUser(0))
    userRes should contain theSameElementsAs (Seq(users(1)))
    totalCount should be(1)
  }

  test("search by querying exact email") {
    val params = UserSearchParamSet(query = Some("death.kid@deathcity.com"))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, domainForRoles, superAdminUser(0))
    userRes should contain theSameElementsAs (Seq(users(0), users(1), users(2)))
    userRes.head should be(users(1))  // tis the best match
    totalCount should be(3)
  }

  test("search by querying email alias") {
    val params = UserSearchParamSet(query = Some("death.kid"))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, domainForRoles, superAdminUser(0))
    userRes should contain theSameElementsAs (Seq(users(0), users(1)))
    userRes.head should be(users(1))  // tis the best match
    totalCount should be(2)
  }

  test("search by querying email domain") {
    val params = UserSearchParamSet(query = Some("care.alot"))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, domainForRoles, superAdminUser(0))
    userRes should contain theSameElementsAs (Seq(users(3), users(4)))
    totalCount should be(2)
  }

  test("search by querying strings with ES-reserved characters") {
    val reservedChars = List("+", "-", "&&", "&", "||", "|", "!", "(", ")", "{", "}", "[", "]", "^", "\"", "~", "*", "?", ":", "\\", "/")
    // nothing should blow up
    reservedChars.map { c =>
      userClient.search(UserSearchParamSet(query = Some(s"anu$c")), PagingParamSet(), None, domainForRoles, superAdminUser(0))
    }
  }

  test("search by all the things") {
    val params = UserSearchParamSet(
      emails = Some(Set("good.luck.bear@care.alot")),
      screenNames = Some(Set("Goodluck Bear")),
      roleNames = Some(Set("bear")),
      query = Some("good luck")
    )
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, domainForRoles, superAdminUser(0))
    userRes should contain theSameElementsAs (Seq(users(4)))
    totalCount should be(1)
  }

  test("search by non-existent role, get no results") {
    val params = UserSearchParamSet(roleNames = Some(Set("Editor")))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, domainForRoles, superAdminUser(0))
    userRes should be('empty)
    totalCount should be(0)
  }

  test("search by non-existent domain, get no results") {
    val badDom = Domain(80, "non", None, None, None, true, true, true, true, true)
    val (userRes, totalCount, _) = userClient.search(UserSearchParamSet(), PagingParamSet(), Some(badDom), badDom, superAdminUser(0))
    userRes should be('empty)
    totalCount should be(0)
  }

  test("search by non-existent email, get no results") {
    val params = UserSearchParamSet(emails = Some(Set("bright.heart.racoon@care.alot")))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, domainForRoles, superAdminUser(0))
    userRes should be('empty)
    totalCount should be(0)
  }

  test("search by non-existent screen name, get no results") {
    val params = UserSearchParamSet(screenNames = Some(Set("muffin")))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None, domainForRoles, superAdminUser(0))
    userRes should be('empty)
    totalCount should be(0)
  }

  test("search with paging limits") {
    val limit = 2
    val params = PagingParamSet(offset = 1, limit = limit)
    val (userRes, totalCount, _) = userClient.search(UserSearchParamSet(), params, None, domainForRoles, superAdminUser(0))
    userRes.size should be(limit)
    totalCount should be(users.length)
  }

  test("search all with sort on screen_name ASC") {
    val params = PagingParamSet(sortOrder = Some("screen_name"))
    val (userRes, _, _) = userClient.search(UserSearchParamSet(), params, None, domainForRoles, superAdminUser(0))
    val expectedOrder = users.map(_.screenName.getOrElse("zzz")).map(_.toLowerCase).sorted
    val actualOrder = userRes.map(_.screenName.getOrElse("zzz")).map(_.toLowerCase)
    actualOrder.toList should equal(expectedOrder.toList)
  }

  test("search all with sort on screen_name DESC") {
    val params = PagingParamSet(sortOrder = Some("screen_name DESC"))
    val (userRes, _, _) = userClient.search(UserSearchParamSet(), params, None, domainForRoles, superAdminUser(0))
    val expectedOrder = users.map(_.screenName.getOrElse("aaa")).map(_.toLowerCase).sorted.reverse
    val actualOrder = userRes.map(_.screenName.getOrElse("aaa")).map(_.toLowerCase)
    actualOrder.toList should equal(expectedOrder.toList)
  }

  test("search all with sort on email ASC") {
    val params = PagingParamSet(sortOrder = Some("email"))
    val (userRes, _, _) = userClient.search(UserSearchParamSet(), params, None, domainForRoles, superAdminUser(0))
    val expectedOrder = users.map(_.email.getOrElse("zzz")).map(_.toLowerCase.replace(".", "")).sorted
    val actualOrder = userRes.map(_.email.getOrElse("zzz")).map(_.toLowerCase.replace(".", ""))
    actualOrder.toList should equal(expectedOrder.toList)
  }

  test("search all with sort on email DESC") {
    val params = PagingParamSet(sortOrder = Some("email DESC"))
    val (userRes, _, _) = userClient.search(UserSearchParamSet(), params, None, domainForRoles, superAdminUser(0))
    val expectedOrder = users.map(_.email.getOrElse("aaa")).map(_.toLowerCase.replace(".", "")).sorted.reverse
    val actualOrder = userRes.map(_.email.getOrElse("aaa")).map(_.toLowerCase.replace(".", ""))
    actualOrder.toList should equal(expectedOrder.toList)
  }

  test("search domain 1 with sort on role_name ASC") {
    val domain = domains(1)
    val params = PagingParamSet(sortOrder = Some("role_name"))
    val (userRes, _, _) = userClient.search(UserSearchParamSet(), params, Some(domain), domain, superAdminUser(0))
    val expectedOrder = users.map(_.roleName(domain.domainId)).flatten.map(_.toLowerCase).sorted
    val actualOrder = userRes.map(_.roleName(domain.domainId)).flatten.map(_.toLowerCase)
    actualOrder.toList should equal(expectedOrder.toList)
  }

  test("search domain 1 with sort on role_name DESC") {
    val domain = domains(1)
    val params = PagingParamSet(sortOrder = Some("role_name DESC"))
    val (userRes, _, _) = userClient.search(UserSearchParamSet(), params, Some(domain), domain, superAdminUser(0))
    val expectedOrder = users.map(_.roleName(domain.domainId)).flatten.map(_.toLowerCase).sorted.reverse
    val actualOrder = userRes.map(_.roleName(domain.domainId)).flatten.map(_.toLowerCase)
    actualOrder.toList should equal(expectedOrder.toList)
  }

  test("search all with sort on role_name ASC") {
    val domain = domains(1)
    val params = PagingParamSet(sortOrder = Some("role_name"))
    val (userRes, _, _) = userClient.search(UserSearchParamSet(), params, None, domain, superAdminUser(0))
    val expectedOrder = users.map(_.roleName(domain.domainId).getOrElse("zzz")).map(_.toLowerCase).sorted
    val actualOrder = userRes.map(_.roleName(domain.domainId).getOrElse("zzz")).map(_.toLowerCase)
    actualOrder.toList should equal(expectedOrder.toList)
  }

  test("search all with sort on role_name DESC") {
    val domain = domains(1)
    val params = PagingParamSet(sortOrder = Some("role_name DESC"))
    val (userRes, _, _) = userClient.search(UserSearchParamSet(), params, None, domain, superAdminUser(0))
    val expectedOrder = users.map(_.roleName(domain.domainId).getOrElse("aaa")).map(_.toLowerCase).sorted.reverse
    val actualOrder = userRes.map(_.roleName(domain.domainId).getOrElse("aaa")).map(_.toLowerCase)
    actualOrder.toList should equal(expectedOrder.toList)
  }

  test("search domain 1 with sort on last_authenticated_at ASC") {
    val domain = domains(1)
    val params = PagingParamSet(sortOrder = Some("last_authenticated_at"))
    val (userRes, _, _) = userClient.search(UserSearchParamSet(), params, Some(domain), domain, superAdminUser(0))
    val expectedOrder = users.map(_.lastAuthenticatedAt(domain.domainId)).flatten.sorted
    val actualOrder = userRes.map(_.lastAuthenticatedAt(domain.domainId)).flatten
    actualOrder.toList should equal(expectedOrder.toList)
  }

  test("search domain 1 with sort on last_authenticated_at DESC") {
    val domain = domains(1)
    val params = PagingParamSet(sortOrder = Some("last_authenticated_at DESC"))
    val (userRes, _, _) = userClient.search(UserSearchParamSet(), params, Some(domain), domain, superAdminUser(0))
    val expectedOrder = users.map(_.lastAuthenticatedAt(domain.domainId)).flatten.sorted.reverse
    val actualOrder = userRes.map(_.lastAuthenticatedAt(domain.domainId)).flatten
    actualOrder.toList should equal(expectedOrder.toList)
  }

  test("search all with sort on last_authenticated_at ASC") {
    val domain = domains(1)
    val params = PagingParamSet(sortOrder = Some("last_authenticated_at"))
    val (userRes, _, _) = userClient.search(UserSearchParamSet(), params, None, domain, superAdminUser(0))
    val expectedOrder = users.map(_.lastAuthenticatedAt(domain.domainId).getOrElse(BigInt(9999999999L))).sorted
    val actualOrder = userRes.map(_.lastAuthenticatedAt(domain.domainId).getOrElse(BigInt(9999999999L)))
    actualOrder.toList should equal(expectedOrder.toList)
  }

  test("search all with sort on last_authenticated_at DESC") {
    val domain = domains(1)
    val params = PagingParamSet(sortOrder = Some("last_authenticated_at DESC"))
    val (userRes, _, _) = userClient.search(UserSearchParamSet(), params, None, domain, superAdminUser(0))
    val expectedOrder = users.map(_.lastAuthenticatedAt(domain.domainId).getOrElse(BigInt(0))).sorted.reverse
    val actualOrder = userRes.map(_.lastAuthenticatedAt(domain.domainId).getOrElse(BigInt(0)))
    actualOrder.toList should equal(expectedOrder.toList)
  }
}
