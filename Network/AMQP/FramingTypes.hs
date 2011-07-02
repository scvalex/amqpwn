{-# LANGUAGE TemplateHaskell #-}

module Network.AMQP.FramingTypes
    ( Class(..), Method(..), Field(..), DomainMap
    , genClassIDFuns, genContentHeaderProperties, genMethodPayload
    , genGetContentHeaderProperties, genPutContentHeaderProperties
    , genMethodPayloadBinaryInstance
    , getPropBits, putPropBits, condGet, condPut
    , listShow, fixClassName, fixMethodName, translateType, fieldType
    ) where

import Control.Monad ( replicateM )
import Data.Binary ( Binary(..) )
import Data.Binary.Get ( Get, getWord16be )
import Data.Binary.Put ( Put, putWord16be )
import Data.Bits
import Data.Char
import qualified Data.List as L
import qualified Data.Map as M
import Data.Maybe ( isJust )
import Data.Word ( Word16 )
import Language.Haskell.TH
import Language.Haskell.TH.Syntax ( Quasi )
import Text.Printf ( printf )

import Network.AMQP.Types ( Bit )

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
fieldType _ (TypeField _ x) = x
fieldType domainMap (DomainField _ domain) =
    let (Just v) = M.lookup domain domainMap in v

mkField :: DomainMap -> (Type -> Type) -> Field -> (Strict, Type)
mkField _ f (TypeField _ typ) =
    (NotStrict, f $ ConT $ mkName $ translateType typ)
mkField domainMap f df@(DomainField _ _) =
    (NotStrict, f $ ConT $ mkName $ translateType
                         $ fieldType domainMap df)

getPropBits :: Integer -> Get [Bit]
getPropBits num = getWord16be >>= \x -> return $ getPropBits' num 0 x
    where
      getPropBits' :: Integer -> Integer -> Word16 -> [Bit]
      getPropBits' 0 _ _ = []
      getPropBits' n offset x =
          ((x .&. (2^(15-offset))) /= 0) : (getPropBits' (n-1) (offset+1) x)

condGet :: (Binary b) => Bool -> Get (Maybe b)
condGet False = return Nothing
condGet True = get >>= \x -> return $ Just x

-- | Packs up to 15 Bits into a Word16 (=Property Flags)
putPropBits :: [Bit] -> Put
putPropBits xs = putWord16be $ (putPropBits' 0 xs)
    where
      putPropBits' _ [] = 0
      putPropBits' offset (y:ys) =
          (shiftL (toInt y) (15-offset)) .|. (putPropBits' (offset+1) ys)
      toInt True = 1
      toInt False = 0

condPut :: (Binary b) => (Maybe b) -> Put
condPut (Just x) = put x
condPut _ = return ()

mkMethodName :: String -> String -> String
mkMethodName cNam nam= printf "%s_%s" (fixClassName cNam) (fixMethodName nam)

genContentHeaderProperties :: (Monad m) => DomainMap -> [Class] -> m [Dec]
genContentHeaderProperties domainMap classes =
    return [DataD [] (mkName "ContentHeaderProperties") []
                  (map mkConstr classes) [mkName "Show"]]
        where
          mkConstr (Class nam _ _ fields) =
              NormalC (chClassName nam)
                      (map (mkField domainMap maybeF) fields)
          maybeF = AppT (ConT $ mkName "Maybe")

genClassIDFuns :: (Monad m) => [Class] -> m [Dec]
genClassIDFuns classes =
    return [FunD (mkName "getGlassIDOf") (map mkClause classes)]
        where
          mkClause (Class nam index _ _) =
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
              let fullName = mkName $ mkMethodName clsNam nam
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
                condGets (w:ws) (w':ws') =
                    [| condGet $(varE w) >>=
                       $(lamE [varP w'] (condGets ws ws')) |]
                condGets [] [] = appE [|return|]
                                 (if null vs
                                  then conE nam
                                  else appAll (conE nam) (map varE vs'))
                appAll = foldl appE

genPutContentHeaderProperties :: (Monad m, Quasi m) => [Class] -> m [Dec]
genPutContentHeaderProperties classes = do
  clauses <- mapM (runQ . mkClause) classes
  return [FunD (mkName "putContentHeaderProperties") clauses]
      where
        mkClause (Class nam _ _ fields) = do
          vs <- replicateM (length fields) (newName "x")
          clause [conP (chClassName nam) (map varP vs)]
                 (normalB $ mkFunBody vs)
                 []
        mkFunBody [] = [| putPropBits [] |]
        mkFunBody vs = [| putPropBits $(isJusts vs) >> $(condPuts vs) |]
        isJusts vs = listE $ map (appE [|isJust|] . varE) vs
        condPuts [] = fail "bad juju"
        condPuts [v] = [|condPut $(varE v)|]
        condPuts (v:vs) = [| condPut $(varE v) >> $(condPuts vs) |]

-- FIXME: Quasi instead of Monad; default to return () when processing lists

genMethodPayloadBinaryInstance :: (Quasi m) => DomainMap -> [Class] -> m [Dec]
genMethodPayloadBinaryInstance domainMap classes = do
  p <- runQ mkPut
  return [InstanceD []
          (AppT (ConT $ mkName "Binary") (ConT $ mkName "MethodPayload"))
          [p]]
      where
        mkPut = funD (mkName "put") (concatMap mkClassClause classes)
        mkClassClause (Class nam index methods _) =
            map (mkClause nam index) methods
        mkClause clsNam clsIdx (Method nam index fields) = do
          vs <- replicateM (length fields) (newName "x")
          clause [conP (mkName $ mkMethodName clsNam nam) (map varP vs)]
                 (mkClauseBody clsIdx index vs)
                 []
        mkClauseBody clsIdx mthdIdx vs =
            normalB [| putWord16be $(litE . integerL $
                                     fromIntegral clsIdx) >>
                       putWord16be $(litE . integerL $
                                     fromIntegral mthdIdx) >>
                       $(putAll vs) |]
        putAll [] = [| return () |]
        putAll (v:vs) = [| put $(varE v) >> $(putAll vs) |]
