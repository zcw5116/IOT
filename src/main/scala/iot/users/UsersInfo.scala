package iot.users

/*
  * Created by slview on 17-6-18.
 */

//UserInfo.scala
// We declare field with Option[T] type to make that field nullable
class UsersInfo(mdn:Option[String], imsicdma:Option[String], imsilte:Option[String], imei:Option[String],
                vpdncompanycode:Option[String], nettype:Option[String], vpdndomain:Option[String], isvpdn:Option[String],
                subscribetimepcrf:Option[String], atrbprovince:Option[String], userprovince:Option[String]) extends Product {

  override def productElement(n: Int): Any =
    n match {
      case 0 => mdn
      case 1 => imsicdma
      case 2 => imsilte
      case 3 => imei
      case 4 => vpdncompanycode
      case 5 => nettype
      case 6 => vpdndomain
      case 7 => isvpdn
      case 8 => subscribetimepcrf
      case 9 => atrbprovince
      case 10 => userprovince
      case _ => throw new IndexOutOfBoundsException(n.toString)
    }

  override def productArity: Int = 11
  override def canEqual(that: Any): Boolean  = that.isInstanceOf[UsersInfo]
}

