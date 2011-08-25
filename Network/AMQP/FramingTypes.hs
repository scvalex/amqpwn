{-# LANGUAGE TemplateHaskell #-}

module Network.AMQP.FramingTypes (
        -- * Codegen XML types
        Class(..), Method(..), Field(..), DomainMap,

        -- * Code-generation functions
        genClassIDFuns, genContentHeaderProperties, genMethodPayload,
        genGetContentHeaderProperties, genPutContentHeaderProperties,
        genMethodPayloadBinaryInstance,

        -- * Helpers for the generated code
        getPropBits, putPropBits, condGet, condPut,
    ) where

import Control.Monad ( replicateM )
import Data.Binary ( Binary(..) )
import Data.Binary.Get ( Get, getWord16be )
import Data.Binary.Put ( Put, putWord16be )
import Data.Bits
import Data.Char
import qualified Data.Map as M
import Data.Maybe ( isJust )
import Data.Word ( Word16 )
import Language.Haskell.TH
import Language.Haskell.TH.Syntax ( Quasi )
import Network.AMQP.Types.Internal ( Bit )
import Text.Printf ( printf )

-- Codegen XML types

-- | A class read from the spec XML.
data Class = Class String Int [Method] [Field]
             -- ^ className, classID, methods, content-fields
           deriving ( Show )

-- | A class method read from the spec XML.
data Method = Method String Int [Field]
              -- ^methodName, methodID, fields
              deriving ( Show )

-- | A method field read from the spec XML.
data Field = TypeField String String   -- ^fieldName, fieldType
           | DomainField String String -- ^fieldName, domainName
             deriving ( Show )

-- | Used to map AMQP types to Haskell types.
type DomainMap = M.Map String String

-- | Return the CH* datatype constructor name for the given class
-- name.
chClassName :: String -> Name
chClassName name = mkName $ "CH" ++ (fixClassName name)

-- | Convert an AMQP class name to a Haskell one by capitalizing it.
fixClassName :: String -> String
fixClassName s = (toUpper $ head s):(tail s)

-- | Convert an AMQP method name to a Haskell one by replacing
-- underscores with dashes.
fixMethodName :: String -> String
fixMethodName = map (\c -> if c == '-' then '_' else c)

-- FIXME: Don't forget to update this.

-- | Convert an AMQP type into the equivalent Haskell type defined in
-- "Network.AMQP.Types".
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

-- | Return the type of an AMQP type (itself, basically) or of a
-- domain.
fieldType :: DomainMap -> Field -> String
fieldType _ (TypeField _ x) = x
fieldType domainMap (DomainField _ domain) =
    let (Just v) = M.lookup domain domainMap in v

-- | Make Template Haskell Field of of an AMQP Field.
mkField :: DomainMap -> (Type -> Type) -> Field -> (Strict, Type)
mkField _ f (TypeField _ typ) =
    (NotStrict, f $ ConT $ mkName $ translateType typ)
mkField domainMap f df@(DomainField _ _) =
    (NotStrict, f $ ConT $ mkName $ translateType
                         $ fieldType domainMap df)

-- FIXME add 'his' todos here

-- | Get 16 bits.
getPropBits :: Integer -> Get [Bit]
getPropBits num = getWord16be >>= \x -> return $ getPropBits' num 0 x
    where
      getPropBits' :: Integer -> Integer -> Word16 -> [Bit]
      getPropBits' 0 _ _ = []
      getPropBits' n offset x =
          ((x .&. (2^(15-offset))) /= 0) : (getPropBits' (n-1) (offset+1) x)

-- | Only get if the parameter is True.
condGet :: (Binary b) => Bool -> Get (Maybe b)
condGet False = return Nothing
condGet True = get >>= \x -> return $ Just x

-- | Put 16 bits.
putPropBits :: [Bit] -> Put
putPropBits xs = putWord16be $ (putPropBits' 0 xs)
    where
      putPropBits' _ [] = 0
      putPropBits' offset (y:ys) =
          (shiftL (toInt y) (15-offset)) .|. (putPropBits' (offset+1) ys)
      toInt True = 1
      toInt False = 0

-- | Only put Justs, ignoring Nothings.
condPut :: (Binary b) => (Maybe b) -> Put
condPut (Just x) = put x
condPut _ = return ()

-- | Make a method name by concatenating the AMQP class and method
-- names.  This is actually used to name data-type constructors.
mkMethodName :: String -> String -> String
mkMethodName cNam nam = printf "%s_%s" (fixClassName cNam) (fixMethodName nam)

-- | Use this to create chains of lambdas and their ilk.
appAll :: ExpQ -> [ExpQ] -> ExpQ
appAll = foldl appE

-- | Generate the 'ContentHeaderProperties' data-type.
genContentHeaderProperties :: (Monad m) => DomainMap -> [Class] -> m [Dec]
genContentHeaderProperties domainMap classes =
    return [DataD [] (mkName "ContentHeaderProperties") []
                  (map mkConstr classes) [mkName "Show"]]
        where
          mkConstr (Class nam _ _ fields) =
              NormalC (chClassName nam)
                      (map (mkField domainMap maybeF) fields)
          maybeF = AppT (ConT $ mkName "Maybe")

-- | Generate the 'getClassIDOf' function.
genClassIDFuns :: (Monad m) => [Class] -> m [Dec]
genClassIDFuns classes =
    return [FunD (mkName "getClassIDOf") (map mkClause classes)]
        where
          mkClause (Class nam index _ _) =
              Clause [RecP (chClassName nam) []]
                     (NormalB (LitE (IntegerL (fromIntegral index))))
                     []

-- | Generate the 'MethodPayload' data-type.
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

-- | Generate the 'getContentHeaderProperties' function.
genGetContentHeaderProperties :: (Quasi m) => [Class] -> m [Dec]
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

-- | Generate the 'putContentHeaderProperties' function.
genPutContentHeaderProperties :: (Quasi m) => [Class] -> m [Dec]
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
        condPuts [] = [|return ()|]
        condPuts (v:vs) = [| condPut $(varE v) >> $(condPuts vs) |]

-- | Generate the 'MethodPayload' 'Binary' instance.
genMethodPayloadBinaryInstance :: (Quasi m) => DomainMap -> [Class] -> m [Dec]
genMethodPayloadBinaryInstance domainMap classes = do
  p <- runQ mkPut
  g <- runQ mkGet
  return [InstanceD []
          (AppT (ConT $ mkName "Binary") (ConT $ mkName "MethodPayload"))
          [p, g]]
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
        -- FIXME: coalesce consecutive bits
        putAll [] = [| return () |]
        putAll (v:vs) = [| put $(varE v) >> $(putAll vs) |]

        mkGet :: DecQ
        mkGet = funD (mkName "get") [clause [] mkGetBody []]
        mkGetBody = do
          clsId <- newName "classID"
          mthdId <- newName "methodID"
          normalB $ doE [ bindS (varP clsId) [|getWord16be|]
                        , bindS (varP mthdId) [|getWord16be|]
                        , noBindS $ caseE [|($(varE clsId), $(varE mthdId))|]
                                          (concatMap mkClassMatches classes) ]
        mkClassMatches (Class nam index methods _) =
            map (mkMatch nam index) methods
        mkMatch clsNam clsIdx (Method nam index fields) =
            match (tupP $ map (litP . integerL . fromIntegral)
                              [clsIdx, index])
                  (mkMatchBody clsNam nam fields)
                  []
        mkMatchBody clsNam mthdNam fields = do
          vs <- replicateM (length fields) (newName "x")
          normalB $ getAll vs vs
            where
              getAll vs [] = [| return $(appAll (conE . mkName $
                                              mkMethodName clsNam mthdNam)
                                             (map varE vs)) |]
              getAll vs (w:ws) = [| get >>= $(lamE [varP w] (getAll vs ws)) |]
