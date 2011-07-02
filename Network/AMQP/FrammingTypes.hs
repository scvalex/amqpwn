module Network.AMQP.FrammingTypes
    ( Class(..), Method(..), Field(..), DomainMap
    , genClassIDFuns, genContentHeaderProperties
    , listShow, fixClassName, fixMethodName, translateType, fieldType
    ) where

import Data.Char
import qualified Data.List as L
import qualified Data.Map as M
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

type DomainMap = M.Map String String

genContentHeaderProperties :: (Monad m) => DomainMap -> [Class] -> m [Dec]
genContentHeaderProperties domainMap classes =
    return [DataD [] (mkName "ContentHeaderProperties") []
                  (map mkConstr classes) [mkName "Show"]]
        where
          mkConstr (Class nam index _ fields) =
              NormalC (chClassName nam) (map mkField fields)
          mkField (TypeField _ typ) =
              (NotStrict, AppT (ConT $ mkName "Maybe")
                               (ConT $ mkName $ translateType typ))
          mkField df@(DomainField _ _) =
              (NotStrict, AppT (ConT $ mkName "Maybe")
                               (ConT $ mkName $ translateType
                                     $ fieldType domainMap df))

genClassIDFuns :: (Monad m) => [Class] -> m [Dec]
genClassIDFuns classes =
    return [FunD (mkName "getGlassIDOf") (map mkClause classes)]
        where
          mkClause (Class nam index _ fields) =
              Clause ([RecP (chClassName nam) []])
                     (NormalB (LitE (IntegerL (fromIntegral index))))
                     []

chClassName :: String -> Name
chClassName name = mkName $ "CH" ++ (fixClassName name)

listShow :: (Show a) => [a] -> String
listShow cs = "[" ++ (L.intercalate ", " $ map show cs) ++ "]"

fixClassName :: String -> String
fixClassName s = (toUpper $ head s):(tail s)

fixMethodName :: String -> String
fixMethodName = map f
    where
      f '-' = '_'
      f x   = x

translateType :: String -> String
translateType "octet" = "Octet"
translateType "longstr" = "LongString"
translateType "shortstr" = "ShortString"
translateType "short" = "ShortInt"
translateType "long" = "LongInt"
translateType "bit" = "Bit"
translateType "table" = "FieldTable"
translateType "longlong" = "LongLongInt"
translateType "timestamp" = "Timestamp"
translateType x = error x

fieldType :: DomainMap -> Field -> String
fieldType domainMap (TypeField _ x) = x
fieldType domainMap (DomainField _ domain) =
    let (Just v) = M.lookup domain domainMap
    in v
