package com.socrata.cetera.errors

case class UnauthorizedError(userId: Option[String], action: String) extends Throwable {
  override def getMessage: String = {
    userId match {
      case Some(id) => s"User $id is not authorized to $action"
      case None => s"An authorized user was not provided to $action"
    }
  }
}
