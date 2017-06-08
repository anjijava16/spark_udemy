import util.control.Breaks._
//
//// Single Even
//
def single_even(e: Int): Boolean = ((e%2) == 0)

//
// Evens in a List -- is there an even number in a List
//
def evenInList(l: List[Int]): Boolean = {
  var i = 0
  for (i <- l) {
    if (i%2 == 0) return true
  }
  return false
}

//
//// Lucky Number Seven -- count sevens twice
//
def luckyNumberSeven(l: List[Int]): Int = {
  var sum = 0
  for (i <- l) {
    if (i == 7) {
      sum = sum + 14
    }
    else sum = sum + i
  }
  return sum
}

def sumList(xs: List[Int]): Int = {
  xs match {
    case x :: tail => x + sumList(tail)
    case Nil => 0
  }
}

//
// Balance -- sum of numbers on one side is equal to sum of numbers on other side
//
def balance(leftsum: Int, l : List[Int]): Boolean = {
  l match {
    case head :: tail => {
      val x = leftsum + head
      if (x == sumList(tail)) return true
      balance (x, tail)
    }
    case Nil => return false
  }
}

def palindrome(s: String): Boolean = (s == s.reverse)
