module Codegen.Codegen (
        main, run
    ) where

import Data.Char
import qualified Data.List as L
import qualified Data.Map as M
import Data.Maybe

import Network.AMQP.FramingTypes

import System.Environment ( getArgs )

import Text.Printf ( printf )

import Text.XML.Light
import Text.XML.Light.Input
import Text.XML.Light.Proc

main :: IO ()
main = do
  [ specFn ] <- getArgs
  putStr =<< run specFn

run :: FilePath -> IO String
run specFn = do
  spec <- readFile specFn
  let parsed = parseXML spec
  let !(Elem e) = parsed !! 2

  --map from domainName => type
  let domainMap = M.fromList . map readDomain $
                  findChildren (unqual "domain") e

  -- read classes
  let classes = map readClass $ findChildren (unqual "class") e :: [Class]

  return $ L.intercalate "\n"
         [ "-- WARNING: Auto-generated file. Do not edit."
         , ""
         , "module Network.AMQP.FramingData where"
         , ""
         , "import Network.AMQP.FramingTypes"
         , "import Data.Map ( fromList )"
         , ""
         , "classes :: [Class]"
         , printf "classes = %s" (listShow classes)
         , ""
         , "domainMap :: DomainMap"
         , printf "domainMap = %s" (show domainMap) ]

---- contentheader class ids -----
readDomain d =
    let (Just domainName) = lookupAttr (unqual "name") $ elAttribs d
        (Just typ) = lookupAttr (unqual "type") $ elAttribs d
    in (domainName, typ)

readClass c =
    let (Just className) = lookupAttr (unqual "name") $ elAttribs c
        (Just classIndex) = lookupAttr (unqual "index") $ elAttribs c
        methods = map readMethod $ findChildren (unqual "method") c
        fields = map readField $ findChildren (unqual "field") c
    in Class className (read classIndex) methods fields

readMethod m =
    let (Just methodName) = lookupAttr (unqual "name") $ elAttribs m
        (Just methodIndex) = lookupAttr (unqual "index") $ elAttribs m
        fields = map readField $ findChildren (unqual "field") m
    in Method methodName (read methodIndex) fields

readField f =
    let (Just fieldName) = lookupAttr (unqual "name") $ elAttribs f
        fieldType = lookupAttr (unqual "type") $ elAttribs f
        fieldDomain = lookupAttr (unqual "domain") $ elAttribs f
    in case (fieldType, fieldDomain) of
         (Just t, _) -> TypeField fieldName t
         (_, Just d) -> DomainField fieldName d

-- Helpers
listShow :: (Show a) => [a] -> String
listShow cs = "[" ++ (L.intercalate ", " $ map show cs) ++ "]"
