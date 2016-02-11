import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by garyturovsky on 1/31/16.
  */
object PrimeFinder {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("PowerSpark")
      .setMaster("local[6]")

    val sc = new SparkContext(conf)

    val intRange = 2 to 1000              // Define a range for the primes

    val (primes, nonPrimes) = calculatePrimesAndNonPrimes(intRange, sc)

    sc.stop()

    println(s"Primes: $primes")
    println("Non-primes")

    nonPrimes.foreach(
      (t) => {
        println(s"${t._1} is divisble by: ${t._2.mkString(", ")}")
      }
    )

  }

  def calculatePrimesAndNonPrimes(intRange: Range.Inclusive, sc:SparkContext): (List[Int], List[(Int, List[Int])]) = {
    var items = sc.parallelize(intRange)
      .map((_, List[Int]()))
      .collect()

    intRange.by(1).foreach((primeCand) => {
      items = sc.parallelize(items)
        .map {
          case t@nonPrime if t._1 % primeCand == 0 => (nonPrime._1, nonPrime._2 :+ primeCand)  // If the number is divisble by primeCand, it's not a prime or primeCand itself; add to divisors
          case t => t                                                                           // Keep around any tuple that doesn't divide, too
        }.cache().collect()


    })

    val primes = sc.parallelize(items)
      .filter( (num) => num._2.size == 1 )
      .map(_._1)
      .collect()
      .toList

    val nonPrimes = sc.parallelize(items)
      .filter( (num) => num._2.size > 1 )
      .collect()
      .toList

    (primes, nonPrimes)
  }

}
