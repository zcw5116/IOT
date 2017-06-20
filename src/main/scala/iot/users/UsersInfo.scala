package iot.users

/*
  * Created by slview on 17-6-18.
 */

//UserInfo.scala
// We declare field with Option[T] type to make that field nullable
class UsersInfo(mdn:Option[String], imsicdma:Option[String], imsilte:Option[String], iccid:Option[String],
                imei:Option[String], company:Option[String], vpdncompanycode:Option[String], nettype:Option[String],
                vpdndomain:Option[String], isvpdn:Option[String], subscribetimeaaa:Option[String],
                subscribetimehlr:Option[String], subscribetimehss:Option[String], subscribetimepcrf:Option[String],
                firstactivetime:Option[String], userstatus:Option[String], atrbprovince:Option[String],
                userprovince:Option[String]) extends Product {

  override def productElement(n: Int): Any =
    n match {
      case 0 => mdn
      case 1 => imsicdma
      case 2 => imsilte
      case 3 => iccid
      case 4 => imei
      case 5 => company
      case 6 => vpdncompanycode
      case 7 => nettype
      case 8 => vpdndomain
      case 9 => isvpdn
      case 10 => subscribetimeaaa
      case 11 => subscribetimehlr
      case 12 => subscribetimehss
      case 13 =>subscribetimepcrf
      case 14 =>firstactivetime
      case 15 =>userstatus
      case 16 =>atrbprovince
      case 17 =>userprovince
      case _ => throw new IndexOutOfBoundsException(n.toString)
    }

  override def productArity: Int = 18
  override def canEqual(that: Any): Boolean  = that.isInstanceOf[UsersInfo]
}

