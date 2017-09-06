package com.socrata.cetera

import scala.io.Source

import com.socrata.cetera.auth.AuthedUser
import com.socrata.cetera.auth.AuthedUser._
import com.socrata.cetera.types.{EsUser, Role}

trait TestESUsers extends TestESDomains {

  val users = {
    val userTSV = Source.fromInputStream(getClass.getResourceAsStream("/users.tsv"))
    userTSV.getLines()
    val iter = userTSV.getLines().map(_.split("\t"))
    iter.drop(1) // drop the header columns
    val originalUsers = iter.map { tsvLine =>
      EsUser(
        id = tsvLine(0),
        screenName = Option(tsvLine(1)).filter(_.nonEmpty),
        email = Option(tsvLine(2)).filter(_.nonEmpty),
        roles = Some(Set(Role(
          tsvLine(3).toInt,
          tsvLine(4),
          Option(tsvLine(5)).filter(_.nonEmpty).map(_.toInt),
          Option(tsvLine(6)).filter(_.nonEmpty).map(BigInt(_))))),
        flags = Option(List(tsvLine(7)).filter(_.nonEmpty)),
        profileImageUrlLarge = Option(tsvLine(8)).filter(_.nonEmpty),
        profileImageUrlMedium = Option(tsvLine(9)).filter(_.nonEmpty),
        profileImageUrlSmall = Option(tsvLine(10)).filter(_.nonEmpty)
      )
    }.toSeq

    // we want some users to have roles on mulitple domains
    originalUsers.map(u =>
      if (u.id == "bright-heart") {
        val moreRoles = u.roles.get ++ Some(Role(1, "honorary-bear", Some(20), Some(150415999)))
        u.copy(roles = Some(moreRoles))
      } else {
        u
      }
    )
  }

  def superAdminUser(domainId: Int) = AuthedUser("user-fxf", domains(domainId), roleName = None, rights = None, flags = Some(List("admin")))
  def userWithAllTheRights(domainId: Int) = AuthedUser("user-fxf", domains(domainId), roleName = Some("administrator"), flags = None,
    rights = Some(Set(toViewAllUsers, toViewOthersStories, toViewOthersViews, toEditOthersStories, toEditOthersViews)))
  def userWithAllTheStoriesRights(domainId: Int) = AuthedUser("user-fxf", domains(domainId), roleName = Some("publisher_stories"), flags = None,
    rights = Some(Set(toViewOthersStories, toEditOthersStories)))
  def userWithAllTheNonStoriesRights(domainId: Int) = AuthedUser("user-fxf", domains(domainId), roleName = Some("publisher"), flags = None,
    rights = Some(Set(toViewOthersViews, toEditOthersViews)))
  def userWithAllTheViewRights(domainId: Int) = AuthedUser("user-fxf", domains(domainId), roleName = Some("designer"), flags = None,
    rights = Some(Set(toViewAllUsers, toViewOthersStories, toViewOthersViews)))
  def userWithOnlyManageUsersRight(domainId: Int) = AuthedUser("user-fxf", domains(domainId), roleName = Some("administrator"), flags = None,
    rights = Some(Set(toViewAllUsers)))
  def userWithRoleButNoRights(domainId: Int) = AuthedUser("user-fxf", domains(domainId), roleName = Some("editor"), flags = None,
    rights = Some(Set.empty))
  def userWithNoRoleAndNoRights(domainId: Int) = AuthedUser("user-fxf", domains(domainId), roleName = None, flags = None, rights = None)
}
