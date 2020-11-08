package reductions

import scala.annotation._
import org.scalameter._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime")
    println(s"speedup: ${seqtime.value / fjtime.value}")
  }
}

object ParallelParenthesesBalancing extends ParallelParenthesesBalancingInterface {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    val length = chars.length
    @tailrec
    def balanceRec(i: Int, count: Int): Boolean = {
      if (i == length)
        count == 0
      else if (chars(i) == ')' && count == 0)
        false
      else {
        val newCount = chars(i) match {
          case '(' => 1
          case ')' => -1
          case _ => 0
        }
        balanceRec(i + 1, count + newCount)
      }
    }
    balanceRec(0, 0)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {
    
    require(threshold > 1)

    @tailrec
    def traverse(idx: Int, until: Int, balance: Int, sign: Int): (Int, Int) = {
      
      if (idx >= until) {
        (balance, sign)
      }
      else {
        val (newBalance, newSign) = chars(idx) match {
          case '(' => (balance + 1, if (sign == 0) +1 else sign)
          case ')' => (balance - 1, if (sign == 0) -1 else sign)
          case _ => (balance, sign)
        }
        traverse(idx+1, until, newBalance, newSign)
      }
    }

    def reduceSign(signLeft: Int, signRight: Int): Int =
      if (signLeft == 0) signRight else signLeft

    def reduce(from: Int, until: Int): (Int, Int) = {
      
      if (until <= from || until - from <= threshold) {
        traverse(from, until, 0, 0)
      } else {
        val mid = from + (until - from) / 2
        val ((balanceLeft, signLeft), (balanceRight, signRight)) = parallel(
          reduce(from, mid),
          reduce(mid, until)
        )
        (balanceLeft + balanceRight, reduceSign(signLeft, signRight))
      }
    }
    val (balance, sign) = reduce(0, chars.length)
    balance == 0 && sign >= 0
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
