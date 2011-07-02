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
  spec <- readFile specFn
  let parsed = parseXML spec
  let !(Elem e) = parsed !! 2

  -- read domains
  let domains = findChildren (unqual "domain") e

  --map from domainName => type
  let domainMap = M.fromList $ map readDomain domains

  -- read classes
  let classes = map readClass $ findChildren (unqual "class") e :: [Class]

  -- generate binary instances for data-type
  let binaryGetInst = concat $ map ("\t"++) $
                      concatMap (writeBinaryGetInstForClass domainMap)
                      classes
  let binaryPutInst = concat $ map ("\t"++) $
                      concatMap (writeBinaryPutInstForClass domainMap)
                      classes

  putStrLn "module Network.AMQP.FramingData where\n\
           \\n\
           \import Network.AMQP.FramingTypes\n\
           \import Data.Map ( fromList )\n"

  putStrLn $ unlines [ "classes :: [Class]"
                     , printf "classes = %s" (listShow classes)
                     , "domainMap :: DomainMap"
                     , printf "domainMap = %s" (show domainMap) ]
  {-putStrLn $ unlines [ "instance Binary MethodPayload where"
                     , binaryPutInst -- put instances
                     -- get instances
                     , "\tget = do"
                     , "\t\tclassID <- getWord16be"
                     , "\t\tmethodID <- getWord16be"
                     , "\t\tcase (classID, methodID) of"
                     , binaryGetInst
                     ]-}

fixFieldName "type" = "typ"
fixFieldName s = map f s
    where
        f ' ' = '_'
        f x = x

---- binary get instance ----

writeBinaryGetInstForClass :: DomainMap -> Class -> [String]
writeBinaryGetInstForClass domainMap (Class nam index methods _) =
    map (writeBinaryGetInstForMethod domainMap nam index) methods

writeBinaryGetInstForMethod :: DomainMap -> String -> Int -> Method -> String
writeBinaryGetInstForMethod domainMap className classIndex (Method nam index fields) =
    let fullName = (fixClassName className) ++ "_"++(fixMethodName nam)
    in --binary instances
      "\t" ++ (writeBinaryGetInstance domainMap fullName classIndex index fields)

writeBinaryGetInstance :: DomainMap -> String -> Int -> Int -> [Field] -> String
writeBinaryGetInstance domainMap fullName classIndex methodIndex fields =
    "\t(" ++ (show classIndex) ++ "," ++ (show methodIndex) ++ ") -> " ++
    getDef ++ "\n"
        where
          manyLetters = map (:[]) ['a'..'z']

          fieldTypes :: [(String,String)] --(a..z, fieldType)
          fieldTypes = zip manyLetters $ map (fieldType domainMap) fields

          --consecutive BITS have to be merged into a Word8
          --TODO: more than 8 bits have to be split into several Word8
          grouped :: [ [(String, String)] ]
          grouped = L.groupBy (\(_,x) (_,y) -> x == "bit" && y == "bit")
                    fieldTypes

          --concatMap (\x -> " get >>= \\"++x++" ->") (take (length fields) manyLetters)

          showBlob xs | length xs == 1 = "get >>= \\" ++ (fst $ xs !! 0) ++
                                        " -> "
          showBlob xs = "getBits " ++ (show $ length xs) ++ " >>= \\[" ++
                        (concat $ L.intersperse "," $ map fst xs) ++ "] -> "

          getStmt = concatMap showBlob grouped

          getDef =
              let wrap = if (length fields) /= 0 then ("("++) . (++")") else id
              in  getStmt ++ " return " ++
                  wrap (fullName ++ concatMap (" "++) (take (length fields)
                                                       manyLetters))

---- binary put instance ----

writeBinaryPutInstForClass :: DomainMap -> Class -> [String]
writeBinaryPutInstForClass domainMap (Class nam index methods _) =
    map (writeBinaryPutInstForMethod domainMap nam index) methods

writeBinaryPutInstForMethod :: DomainMap -> String -> Int -> Method -> String
writeBinaryPutInstForMethod domainMap className classIndex (Method nam index fields) =
    let fullName = (fixClassName className) ++ "_" ++ (fixMethodName nam)
    in  --binary instances
      (writeBinaryPutInstance domainMap fullName classIndex index fields)

writeBinaryPutInstance :: DomainMap -> String -> Int -> Int -> [Field] -> String
writeBinaryPutInstance domainMap fullName classIndex methodIndex fields =
    putDef ++ "\n"
        where
          manyLetters = map (:[]) ['a'..'z']

          fieldTypes :: [(String,String)] --(a..z, fieldType)
          fieldTypes = zip manyLetters $ map (fieldType domainMap) fields

          --consecutive BITS have to be merged into a Word8
          --TODO: more than 8bits have to be split into several Word8
          grouped :: [ [(String, String)] ]
          grouped = L.groupBy (\(_,x) (_,y) -> x=="bit" && y=="bit") fieldTypes

          showBlob xs | length xs == 1 =  " >> put "++(fst $ xs!!0)
          showBlob xs = " >> putBits [" ++
                        (concat $ L.intersperse "," $ map fst xs) ++ "]"

          putStmt = concatMap showBlob grouped

          putDef =
              let wrap = if (length fields) /= 0 then ("("++) . (++")") else id
                  pattern = fullName ++ concatMap (' ':) (take (length fields)
                                                               manyLetters)
              in "put " ++ wrap pattern ++" = " ++
                 "putWord16be " ++ (show classIndex) ++ " >> putWord16be " ++
                 (show methodIndex) ++ putStmt

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
