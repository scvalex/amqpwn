import Data.Char
import qualified Data.List as L
import qualified Data.Map as M
import Data.Maybe

import System.Environment ( getArgs )

import Text.Printf ( printf )

import Text.XML.Light
import Text.XML.Light.Input
import Text.XML.Light.Proc


data Class = Class String Int [Method] [Field]
             -- ^ className, classID, methods, content-fields
             deriving (Show)

data Method = Method String Int [Field]
              -- ^methodName, methodID, fields
              deriving (Show)

data Field = TypeField String String   -- ^fieldName, fieldType
           | DomainField String String -- ^fieldName, domainName
             deriving (Show)

main :: IO ()
main = do
  [ specFn, tmplFn ] <- getArgs
  spec <- readFile specFn
  let parsed = parseXML spec
  let !(Elem e) = parsed !! 2

  -- read domains
  let domains = findChildren (unqual "domain") e

  --map from domainName => type
  let domainMap = M.fromList $ map readDomain domains

  -- read classes
  let classes = map readClass $ findChildren (unqual "class") e :: [Class]

  -- generate data declaration
  let dataDecl = unlines [ "data MethodPayload = \n"
                         , concat $ L.intersperse "\t|" $
                           concatMap (writeDataDeclForClass domainMap) classes
                         , "\tderiving Show" ]

  -- generate binary instances for data-type
  let binaryGetInst = concat $ map ("\t"++) $
                      concatMap (writeBinaryGetInstForClass domainMap)
                      classes
  let binaryPutInst = concat $ map ("\t"++) $
                      concatMap (writeBinaryPutInstForClass domainMap)
                      classes

  -- generate content types
  let contentHeaders = concat $ L.intersperse "\t|" $
                       map (writeContentHeaderForClass domainMap) classes
  let contentHeadersGetInst =
          concatMap (writeContentHeaderGetInstForClass domainMap) classes
  let contentHeadersPutInst =
          concatMap (writeContentHeaderPutInstForClass domainMap) classes
  let contentHeadersClassIDs =
          concatMap (writeContentHeaderClassIDsForClass domainMap) classes

  putStrLn =<< readFile tmplFn
  putStrLn $ unlines [ contentHeadersGetInst
                     , contentHeadersPutInst
                     , contentHeadersClassIDs
                     , "data ContentHeaderProperties ="
                     , "\t" ++ contentHeaders
                     , "\tderiving Show"
                     , ""
                     , "instance Binary MethodPayload where"
                     , binaryPutInst -- put instances
                     -- get instances
                     , "\tget = do"
                     , "\t\tclassID <- getWord16be"
                     , "\t\tmethodID <- getWord16be"
                     , "\t\tcase (classID, methodID) of"
                     , binaryGetInst
                     -- data declaration
                     , dataDecl
                     ]

fieldType domainMap (TypeField _ x) = x
fieldType domainMap (DomainField _ domain) =
    fromJust $ M.lookup domain domainMap

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

fixClassName s = (toUpper $ head s):(tail s)
fixMethodName s = map f s
    where
      f '-' = '_'
      f x   = x

fixFieldName "type" = "typ"
fixFieldName s = map f s
    where
        f ' ' = '_'
        f x = x

---- data declaration ----

writeDataDeclForClass :: M.Map String String -> Class -> [String]
writeDataDeclForClass domainMap (Class nam index methods _) =
    map ("\n\t"++) $ map (writeDataDeclForMethod domainMap nam) methods

writeDataDeclForMethod :: M.Map String String -> String -> Method -> String
writeDataDeclForMethod domainMap className (Method nam index fields) =
    let fullName = (fixClassName className) ++ "_"++(fixMethodName nam)
    in  --data type declaration
      (writeTypeDecl domainMap fullName fields)
      --binary instances
      --(writeBinaryInstance fullName fields)

writeTypeDecl domainMap fullName fields =
    fullName ++ "\n\t\t" ++
    (concat $ L.intersperse "\n\t\t" $ map writeF fields) ++ "\n"
        where
          writeF (TypeField nam typ) =
              (translateType typ) ++ " -- " ++ (fixFieldName nam)
          writeF f@(DomainField nam domain) =
              (translateType $ fieldType domainMap f) ++ " -- " ++
              (fixFieldName nam)

---- binary get instance ----

writeBinaryGetInstForClass :: M.Map String String -> Class -> [String]
writeBinaryGetInstForClass domainMap (Class nam index methods _) =
    map (writeBinaryGetInstForMethod domainMap nam index) methods

writeBinaryGetInstForMethod :: M.Map String String -> String -> Int -> Method
                            -> String
writeBinaryGetInstForMethod domainMap className classIndex (Method nam index fields) =
    let fullName = (fixClassName className) ++ "_"++(fixMethodName nam)
    in --binary instances
      "\t" ++ (writeBinaryGetInstance domainMap fullName classIndex index fields)

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

writeBinaryPutInstForClass :: M.Map String String -> Class -> [String]
writeBinaryPutInstForClass domainMap (Class nam index methods _) =
    map (writeBinaryPutInstForMethod domainMap nam index) methods

writeBinaryPutInstForMethod :: M.Map String String -> String -> Int -> Method 
                            -> String
writeBinaryPutInstForMethod domainMap className classIndex (Method nam index fields) =
    let fullName = (fixClassName className) ++ "_" ++ (fixMethodName nam)
    in  --binary instances
      (writeBinaryPutInstance domainMap fullName classIndex index fields)

writeBinaryPutInstance domainMap fullName classIndex methodIndex fields =
    putDef++"\n"
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

---- content header declaration ----

writeContentHeaderForClass :: M.Map String String -> Class -> String
writeContentHeaderForClass domainMap (Class nam index methods fields) =
    let fullName = "CH"++(fixClassName nam)
    in  (writeContentHeaderDecl domainMap fullName fields)

writeContentHeaderDecl domainMap fullName fields =
    fullName ++ "\n\t\t" ++ (concat $ L.intersperse "\n\t\t" $
                             map writeF fields) ++ "\n"
        where
          writeF (TypeField nam typ) =
              "(Maybe " ++ (translateType typ) ++ ") -- " ++
              (fixFieldName nam)
          writeF f@(DomainField nam domain) =
              "(Maybe " ++ (translateType $ fieldType domainMap f) ++
              ") -- " ++ (fixFieldName nam)

---- contentheader get instance ----

writeContentHeaderGetInstForClass :: M.Map String String -> Class -> String
writeContentHeaderGetInstForClass domainMap (Class nam index methods fields) =
    let fullName = "CH" ++ (fixClassName nam)
    in --binary instances
      (writeContentHeaderGetInstance domainMap fullName index fields)

writeContentHeaderGetInstance domainMap fullName classIndex fields =
    "getContentHeaderProperties " ++ (show classIndex) ++ " = "++getDef++"\n"
        where
          manyLetters = map (:[]) ['a'..'z']
          usedLetters = take (length fields) manyLetters

          showBlob x = "condGet " ++ x ++ " >>= \\" ++ x ++ "' -> "

          getStmt = concatMap showBlob usedLetters

          getDef =
              let wrap = if (length fields) /= 0 then ("("++) . (++")") else id
              in "getPropBits " ++ (show $ length fields) ++ " >>= \\[" ++
                 (concat $ L.intersperse "," usedLetters ) ++ "] -> "++
                 getStmt ++ " return " ++
                 wrap (fullName ++ " " ++
                 concatMap (++"' ") (take (length fields) usedLetters))

---- contentheader put instance ----

writeContentHeaderPutInstForClass :: M.Map String String -> Class -> String
writeContentHeaderPutInstForClass domainMap (Class nam index methods fields) =
    let fullName = "CH"++ (fixClassName nam)
    in --binary instances
      (writeContentHeaderPutInstance domainMap fullName index fields)

writeContentHeaderPutInstance domainMap fullName classIndex fields =
    "putContentHeaderProperties " ++ putDef ++ "\n"
        where
          manyLetters = map (:[]) ['a'..'z']
          usedLetters = take (length fields) manyLetters

          showBlob x =  " >> condPut "++x

          putStmt = concatMap showBlob usedLetters

          putDef =
              let wrap = if (length fields) /= 0 then ("("++) . (++")") else id
                  pattern = fullName ++
                            concatMap (' ':) (take (length fields)
                                                   manyLetters)
              in wrap pattern ++ " = " ++
                     "putPropBits " ++ "[" ++
                     (concat $ L.intersperse "," $ map ("isJust "++)
                             usedLetters) ++ "] " ++ putStmt

---- contentheader class ids -----
writeContentHeaderClassIDsForClass :: M.Map String String -> Class -> String
writeContentHeaderClassIDsForClass domainMap (Class nam index methods fields) =
    let fullName = "CH"++(fixClassName nam)
    in --binary instances
      (writeContentHeaderClassIDsInstance domainMap fullName index fields)

writeContentHeaderClassIDsInstance domainMap fullName classIndex fields =
    "getClassIDOf (" ++ fullName ++
    (concat $ replicate (length fields) " _") ++ ") = " ++
    (show classIndex) ++ "\n"

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
