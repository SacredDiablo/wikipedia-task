package wikipedia

import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD



case class WikipediaArticle(title: String, text: String) {
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)
}

object WikipediaRanking {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
// SparkConf, установлен на локальную обработку
  val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Wiki") //Q1
  // Создание контекста по переданным из SparkConf параметрам
  val sc: SparkContext = new SparkContext(conf) //Q2
  // Hint: use a combination of `sc.textFile`, `WikipediaData.filePath` and `WikipediaData.parse`
  val wikiRdd: RDD[WikipediaArticle] = sc.textFile(WikipediaData.filePath).map(WikipediaData.parse) //Q3
  /** Returns the number of articles on which the language `lang` occurs.
   *  Hint1: consider using method `aggregate` on RDD[T].
   *  Hint2: consider using method `mentionsLanguage` on `WikipediaArticle`
   */
    //Счётчик для rankLangs: фильтруем rdd по языку и считаем количество элементов
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = rdd.filter(_.mentionsLanguage(lang)).count().toInt //Q4
  /* (1) Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`val langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. Don't forget to sort the
   *     languages by their occurrence, in decreasing order!
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = { //Q5
    rdd.cache() // разрешить хранить данные в памяти, использую для ускорения работы программы
    langs.map(lang => (lang, occurrencesOfLang(lang, rdd))).sortBy(_._2).reverse
    // создаем пары язык-число упоминаний, сортируем по количеству и делаем в начале наибольшее число (нужны наиболее популярные языки)
  }
  /* Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */

  /* тестируем все одним шагом
    def test(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
      val articles_Languages =
        rdd.flatMap(article => {
          langs.filter(lang => article.mentionsLanguage(lang))
            .map(lang => (lang, article))})
      articles_Languages.groupByKey.mapValues(_.size).sortBy(-_._2).collect().toList
    }
    print("test1", test(langs, wikiRdd))
    */
    //разбиваем шаг на шаги по условию задачи
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = { //Q6
   val articles_Languages =
      rdd.flatMap(article => { //разбиваем весь текст по условию
      langs.filter(lang => article.mentionsLanguage(lang))
       .map(lang => (lang, article))}) //создаем пары ключ-значение
    articles_Languages.groupByKey
  }

  /* (2) Compute the language ranking again, but now using the inverted index. Can you notice
   *     a performance improvement?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */

  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = //Q7
    index.mapValues(_.size).sortBy(-_._2).collect().toList //считаем количество, создавая пары ключ-значение и сортируя по второму элементу. Сортировка с знаком - дает тот же результат, что и команда reverse
  /* (3) Use `reduceByKey` so that the computation of the index and the ranking are combined.
   *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *     and the computation of the ranking? If so, can you think of a reason?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
    //
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = { //Q8
    rdd.flatMap(article => {
      langs.filter(article.mentionsLanguage)
        .map((_, 1)) //создаем RDD с перым элементом = имени языка, вторым - 1
    }).reduceByKey(_ + _) //подсчёт при помощи rBK
      .sortBy(_._2)
      .collect()
      .toList
      .reverse
  }
  def main(args: Array[String]) {

    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    /* Output the speed of each ranking */
    println(timing)
    print("конец")
    sc.stop()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
