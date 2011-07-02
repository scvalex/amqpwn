module Network.AMQP.FrammingTypes
    ( Class(..), Method(..), Field(..)
    , genClassIDs
    , listShow, fixClassName, fixMethodName
    ) where

import Data.Char
import qualified Data.List as L
import Language.Haskell.TH
import Text.Printf ( printf )

data Class = Class String Int [Method] [Field]
             -- ^ className, classID, methods, content-fields

instance Show Class where
    show (Class name id methods cfs) =
        printf "Class \"%s\" %d %s %s" name id
                   (listShow methods) (listShow cfs)

data Method = Method String Int [Field]
              -- ^methodName, methodID, fields

instance Show Method where
    show (Method name id fields) =
        printf "Method \"%s\" %d %s" name id (listShow fields)

data Field = TypeField String String   -- ^fieldName, fieldType
           | DomainField String String -- ^fieldName, domainName

instance Show Field where
    show (TypeField name value) =
        printf "TypeField \"%s\" \"%s\"" name value
    show (DomainField name value) =
        printf "DomainField \"%s\" \"%s\"" name value

genClassIDs :: (Monad m) => [Class] -> m [Dec]
genClassIDs classes = return [FunD (mkName "getGlassIDOf") clauses]
    where
      clauses = map mkClause classes
      mkClause (Class nam index _ fields) =
          Clause ([RecP (mkName ("CH" ++ (fixClassName nam))) []])
                 (NormalB (LitE (IntegerL (fromIntegral index))))
                 []

listShow :: (Show a) => [a] -> String
listShow cs = "[" ++ (L.intercalate ", " $ map show cs) ++ "]"

fixClassName :: String -> String
fixClassName s = (toUpper $ head s):(tail s)

fixMethodName :: String -> String
fixMethodName = map f
    where
      f '-' = '_'
      f x   = x
