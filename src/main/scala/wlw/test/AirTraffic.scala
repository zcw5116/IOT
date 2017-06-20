package wlw.test

/**
  * Created by slview on 17-6-17.
  */
//AirTraffic.scala
class AirTraffic(Year:Option[String], Month:Option[String], DayOfMonth:Option[String], DayOfWeek:Option[String],
                 DepTime:Option[String], CRSDepTime:Option[String], ArrTime:Option[String], CRSArrTime:Option[String],
                 UniqueCarrier:String, FlightNum:Option[String], TailNum:String, ActualElapsedTime:Option[String],
                 CRSElapsedTime:Option[String], AirTime:Option[String], ArrDelay:Option[String], DepDelay:Option[String],
                 Origin:String, Dest:String, Distance:Option[String], TaxiIn:Option[String], TaxiOut:Option[String],
                 Cancelled:Option[String], CancellationCode:String) extends Product {

  // We declare field with Option[T] type to make that field null-able.

  override def productElement(n: Int): Any =
    n match {
      case 0 => Year
      case 1 => Month
      case 2 => DayOfMonth
      case 3 => DayOfWeek
      case 4 => DepTime
      case 5 => CRSDepTime
      case 6 => ArrTime
      case 7 => CRSArrTime
      case 8 => UniqueCarrier
      case 9 => FlightNum
      case 10 => TailNum
      case 11 => ActualElapsedTime
      case 12 => CRSElapsedTime
      case 13 => AirTime
      case 14 => ArrDelay
      case 15 => DepDelay
      case 16 => Origin
      case 17 => Dest
      case 18 => Distance
      case 19 => TaxiIn
      case 20 => TaxiOut
      case 21 => Cancelled
      case 22 => CancellationCode
      case _ => throw new IndexOutOfBoundsException(n.toString)
    }

  override def productArity: Int = 23

  override def canEqual(that: Any): Boolean  = that.isInstanceOf[AirTraffic]
}