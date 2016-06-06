
import Debug.Trace
import            Control.Applicative
import qualified  Data.Vector as V
import            Test.HUnit
import            Test.Framework
import            Test.Framework.Providers.HUnit
import            Test.Framework.Providers.QuickCheck2 (testProperty)
import            Test.QuickCheck
import            Test.QuickCheck.Arbitrary

import           Blast

main :: IO ()
main = defaultMain tests

tests = [
   testProperty "testRangeProp" testRangeProp
    ]


rangeGen :: Gen Range
rangeGen = do
  a <- choose (0, 100)
  d <- choose (1, 50)
  return $ Range a (a+d)


testRangeProp =
  forAll ((,) <$> rangeGen <*> (choose (1, 10))) prop
  where
  prop (range@(Range start end), nbBuckets) =
    case trace (show $ length ranges) ranges of
      [] -> False
      (Range a b:t) | a == start ->
        case check ranges of
          Just b | b == end -> True
          _ -> False
    where
    p = chunk nbBuckets range
    ranges = V.toList $ partitionToVector p
    check [] = Nothing
    check [Range a b] | a <= b = Just b
    check (Range a b:Range c d:t) | b == c && a <= b && c <= d = check (Range c d:t)
    check _ = Nothing


