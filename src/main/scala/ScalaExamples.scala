object ScalaExamples {

  def main(args: Array[String]) {
    val	lines	=	List("hello	how	are	you","i	am	fine	how	are	you?")
    //nested	structure
    val	wordsArray	=	lines.map(line	=>	line.split("\\s+"))

    val flattenArray = wordsArray.flatten

    val flattenArray1 = lines.flatMap(line	=>	line.split("\\s+"))

    val tuple = ("hello", 1)



  }

}
