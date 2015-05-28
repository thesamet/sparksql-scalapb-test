import com.example.protos.demo.{Address, Person, Gender}
import javax.xml.bind.DatatypeConverter

object PrepareFile {
  val persons = Seq(
    Person().update(
      _.name := "Joe",
      _.age := 32,
      _.gender := Gender.MALE),
    Person().update(
      _.name := "Mark",
      _.age := 21,
      _.gender := Gender.MALE,
      _.addresses := Seq(
          Address(city = Some("San Francisco"), street=Some("3rd Street"))
      )),
    Person().update(
      _.name := "Steven",
      _.age := 35,
      _.gender := Gender.MALE,
      _.addresses := Seq(
          Address(city = Some("San Francisco"), street=Some("5th Street")),
          Address(city = Some("Sunnyvale"), street=Some("Wolfe"))
      )),
    Person().update(
      _.name := "Batya",
      _.age := 11,
      _.gender := Gender.FEMALE))

  def main(args: Array[String]) {
    persons.foreach {
      p =>
       println(DatatypeConverter.printBase64Binary(p.toByteArray))
    }
  }
}
