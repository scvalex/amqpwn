module Network.AMQP.FrammingTypes
    ( Class(..), Method(..), Field(..), DomainMap
    , genClassIDFuns, genContentHeaderProperties, genMethodPayload
    , listShow, fixClassName, fixMethodName, translateType, fieldType
    ) where

import Data.Char
import qualified Data.List as L
import qualified Data.Map as M
import Language.Haskell.TH
import Text.Printf ( printf )

data Class = Class String Int [Method] [Field]
             -- ^ className, classID, methods, content-fields
           deriving ( Show )

data Method = Method String Int [Field]
              -- ^methodName, methodID, fields
              deriving ( Show )

data Field = TypeField String String   -- ^fieldName, fieldType
           | DomainField String String -- ^fieldName, domainName
             deriving ( Show )

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

genMethodPayload :: (Monad m) => DomainMap -> [Class] -> m [Dec]
genMethodPayload domainMap classes =
    return [DataD [] (mkName "MethodPayload") []
                  (concatMap mkConstr classes) [mkName "Show"]]
        where
          mkConstr (Class nam _ methods _) =
              map (mkMethodConstr nam) methods
          mkMethodConstr clsNam (Method nam _ fields) =
              let fullName = mkName $ printf "%s_%s" (fixClassName clsNam)
                                                     (fixMethodName nam)
              in NormalC fullName (map mkField fields)
          mkField (TypeField _ typ) =
              (NotStrict, ConT $ mkName $ translateType typ)
          mkField df@(DomainField _ _) =
              (NotStrict, ConT $ mkName $ translateType
                              $ fieldType domainMap df)

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
