{-# LANGUAGE TemplateHaskell #-}

module Network.AMQP.FrammingTypes
    ( Class(..), Method(..), Field(..), DomainMap
    , genClassIDFuns, genContentHeaderProperties, genMethodPayload
    , genGetContentHeaderProperties
    , getPropBits
    , listShow, fixClassName, fixMethodName, translateType, fieldType
    ) where

import Control.Applicative ( (<$>) )
import Control.Monad ( replicateM )
import Data.Binary ( get )
import Data.Binary.Get ( getWord16be )
import Data.Bits
import Data.Char
import qualified Data.List as L
import qualified Data.Map as M
import Language.Haskell.TH
import Language.Haskell.TH.Syntax ( Quasi )
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
    let (Just v) = M.lookup domain domainMap in v

mkField :: DomainMap -> (Type -> Type) -> Field -> (Strict, Type)
mkField _ f (TypeField _ typ) =
    (NotStrict, f $ ConT $ mkName $ translateType typ)
mkField domainMap f df@(DomainField _ _) =
    (NotStrict, f $ ConT $ mkName $ translateType
                         $ fieldType domainMap df)

getPropBits num = getWord16be >>= \x -> return $ getPropBits' num 0  x
    where
      getPropBits' 0 offset _ = []
      getPropBits' num offset x =
          ((x .&. (2^(15-offset))) /= 0) : (getPropBits' (num-1) (offset+1) x)

condGet False = return Nothing
condGet True = get >>= \x -> return $ Just x

genContentHeaderProperties :: (Monad m) => DomainMap -> [Class] -> m [Dec]
genContentHeaderProperties domainMap classes =
    return [DataD [] (mkName "ContentHeaderProperties") []
                  (map mkConstr classes) [mkName "Show"]]
        where
          mkConstr (Class nam index _ fields) =
              NormalC (chClassName nam)
                      (map (mkField domainMap maybeF) fields)
          maybeF = AppT (ConT $ mkName "Maybe")

genClassIDFuns :: (Monad m) => [Class] -> m [Dec]
genClassIDFuns classes =
    return [FunD (mkName "getGlassIDOf") (map mkClause classes)]
        where
          mkClause (Class nam index _ fields) =
              Clause [RecP (chClassName nam) []]
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
              in NormalC fullName (map (mkField domainMap id) fields)

genGetContentHeaderProperties :: (Monad m, Quasi m) => [Class] -> m [Dec]
genGetContentHeaderProperties classes = do
  clauses <- mapM (runQ . mkClause) classes
  return [FunD (mkName "getContentHeaderProperties") clauses]
        where
          mkClause (Class nam index _ fields) =
              clause [litP  . integerL $ fromIntegral index]
                     (mkFunBody (chClassName nam) (length fields))
                     []
          mkFunBody nam nargs = do
            vs <- replicateM nargs (newName "x")
            vs' <- replicateM nargs (newName "y")
            normalB [| getPropBits $(litE . integerL $ fromIntegral nargs) >>=
                       $(mkConstr nam vs vs') |]
          mkConstr nam vs vs' = lamE [listP $ map varP vs] (condGets vs vs')
              where
                condGets [] [] = appE [|return|]
                                 (if null vs
                                  then conE nam
                                  else appAll (conE nam) (map varE vs'))
                condGets (w:ws) (w':ws') =
                    [| condGet $(varE w) >>=
                       $(lamE [varP w'] (condGets ws ws')) |]
                appAll exp vs = foldl appE exp vs
