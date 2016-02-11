import org.apache.spark.{SparkContext, SparkConf}

import org.scalatest._

/**
  * Created by garyturovsky on 2/6/16.
  */
class PrimesSpec extends FlatSpec with Matchers {

  val conf = new SparkConf().setAppName("PowerSpark")
    .setMaster("local[6]")
    .set("spark.executor.memory","5g")

  val sc = new SparkContext(conf)

  "PrimeFinder" should "find primes and non-primes in the range 2 to 10" in {
    val (primes, nonPrimesAndDivisors) = PrimeFinder.calculatePrimesAndNonPrimes(2 to 10, sc)
    val nonPrimes = nonPrimesAndDivisors.toMap

    primes    should contain(2)
    primes    should contain(3)
    nonPrimes should contain(4 -> List(2,4))
    primes    should contain(5)
    nonPrimes should contain(6 -> List(2,3,6))
    primes    should contain(7)
    nonPrimes should contain(8 -> List(2,4,8))
    nonPrimes should contain(9 -> List(3,9))
    nonPrimes should contain(10 -> List(2,5,10))

  }

}
