
import Debug.Trace
import            Control.Applicative
import qualified  Data.Vector as V
import            Test.HUnit
import            Test.Framework
import            Test.Framework.Providers.HUnit
import            Test.Framework.Providers.QuickCheck2 (testProperty)
import            Test.QuickCheck
import            Test.QuickCheck.Arbitrary

import            Blast
import            Blast.Syntax
import            Test.Computation as C
import            Test.Syntax as S

main :: IO ()
main = defaultMain (S.tests ++ C.tests)
