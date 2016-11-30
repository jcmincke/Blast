{-
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
-}


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
