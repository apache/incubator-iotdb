/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

grammar SqlBase;

singleStatement
    : statement EOF
    ;

/*
 * According to The Definitive ANTLR 4 Reference, 11. Altering the Parse with Semantic Predicates, Altering the Parse with Semantic Predicates.
 * "It s a good idea to avoid embedding predicates in the parser when possible for efficiency and clarity reasons."
 * So if unnecessary, don't use embedding predicates.
 */

statement
    : CREATE TIMESERIES fullPath alias? WITH attributeClauses #createTimeseries
    | DELETE TIMESERIES prefixPath (COMMA prefixPath)* #deleteTimeseries
    | ALTER TIMESERIES fullPath alterClause #alterTimeseries
    | INSERT INTO prefixPath insertColumnsSpec VALUES insertValuesSpec #insertStatement
    | UPDATE prefixPath setClause whereClause? #updateStatement
    | DELETE FROM prefixPath (COMMA prefixPath)* (whereClause)? #deleteStatement
    | SET STORAGE GROUP TO prefixPath #setStorageGroup
    | DELETE STORAGE GROUP prefixPath (COMMA prefixPath)* #deleteStorageGroup
    | SHOW METADATA #showMetadata // not support yet
    | DESCRIBE prefixPath #describePath // not support yet
    | CREATE INDEX ON prefixPath whereClause? indexWithClause #createIndex //not support yet
    | DROP INDEX indexName=ID ON prefixPath #dropIndex //not support yet
    | MERGE #merge
    | FLUSH prefixPath? (COMMA prefixPath)* (booleanClause)?#flush
    | FULL MERGE #fullMerge
    | CLEAR CACHE #clearcache
    | CREATE USER userName=ID password= stringLiteral#createUser
    | ALTER USER userName=(ROOT|ID) SET PASSWORD password=stringLiteral #alterUser
    | DROP USER userName=ID #dropUser
    | CREATE ROLE roleName=ID #createRole
    | DROP ROLE roleName=ID #dropRole
    | GRANT USER userName=ID PRIVILEGES privileges ON prefixPath #grantUser
    | GRANT ROLE roleName=ID PRIVILEGES privileges ON prefixPath #grantRole
    | REVOKE USER userName=ID PRIVILEGES privileges ON prefixPath #revokeUser
    | REVOKE ROLE roleName=ID PRIVILEGES privileges ON prefixPath #revokeRole
    | GRANT roleName=ID TO userName=ID #grantRoleToUser
    | REVOKE roleName = ID FROM userName = ID #revokeRoleFromUser
    | LOAD TIMESERIES (fileName=stringLiteral) prefixPath#loadStatement
    | GRANT WATERMARK_EMBEDDING TO rootOrId (COMMA rootOrId)* #grantWatermarkEmbedding
    | REVOKE WATERMARK_EMBEDDING FROM rootOrId (COMMA rootOrId)* #revokeWatermarkEmbedding
    | LIST USER #listUser
    | LIST ROLE #listRole
    | LIST PRIVILEGES USER username=rootOrId ON prefixPath #listPrivilegesUser
    | LIST PRIVILEGES ROLE roleName=ID ON prefixPath #listPrivilegesRole
    | LIST USER PRIVILEGES username =rootOrId #listUserPrivileges
    | LIST ROLE PRIVILEGES roleName = ID #listRolePrivileges
    | LIST ALL ROLE OF USER username = rootOrId #listAllRoleOfUser
    | LIST ALL USER OF ROLE roleName = ID #listAllUserOfRole
    | SET TTL TO path=prefixPath time=INT #setTTLStatement
    | UNSET TTL TO path=prefixPath #unsetTTLStatement
    | SHOW TTL ON prefixPath (COMMA prefixPath)* #showTTLStatement
    | SHOW ALL TTL #showAllTTLStatement
    | SHOW FLUSH TASK INFO #showFlushTaskInfo
    | SHOW VERSION #showVersion
    | SHOW LATEST? TIMESERIES prefixPath? showWhereClause? limitClause? #showTimeseries
    | SHOW STORAGE GROUP prefixPath? #showStorageGroup
    | SHOW CHILD PATHS prefixPath? #showChildPaths
    | SHOW DEVICES prefixPath? #showDevices
    | SHOW MERGE #showMergeStatus
    | SHOW CONFIGURATION #showConf
    | TRACING ON #tracingOn
    | TRACING OFF #tracingOff
    | COUNT TIMESERIES prefixPath? (GROUP BY LEVEL OPERATOR_EQ INT)? #countTimeseries
    | COUNT DEVICES prefixPath? #countDevices
    | COUNT STORAGE GROUP prefixPath? #countStorageGroup
    | COUNT NODES prefixPath LEVEL OPERATOR_EQ INT #countNodes
    | COUNT CLIENTS #countClients
    | LOAD CONFIGURATION (MINUS GLOBAL)? #loadConfigurationStatement
    | LOAD stringLiteral autoCreateSchema?#loadFiles
    | REMOVE stringLiteral #removeFile
    | MOVE stringLiteral stringLiteral #moveFile
    | DELETE PARTITION prefixPath INT(COMMA INT)* #deletePartition
    | CREATE SNAPSHOT FOR SCHEMA #createSnapshot
    | CREATE TEMPORARY? FUNCTION udfName=ID AS className=stringLiteral #createFunction
    | DROP FUNCTION udfName=ID #dropFunction
    | SHOW TEMPORARY? FUNCTIONS #showFunctions
    | SELECT topClause? selectElements
    fromClause
    whereClause?
    specialClause? #selectStatement
    ;

selectElements
    : aggregationCall (COMMA aggregationCall)* #aggregationElement
    | tableCall (COMMA tableCall)* #tableElement
    | lastClause #lastElement
    | asClause (COMMA asClause)* #asElement
    | functionAsClause (COMMA functionAsClause)* #functionAsElement
    ;

aggregationCall
    : builtInFunctionCall
    | udfCall
    ;

tableCall
    : suffixPath
    | udfCall
    | SINGLE_QUOTE_STRING_LITERAL
    ;

udfCall
    : udfName=ID LR_BRACKET udfSuffixPaths udfAttribute* RR_BRACKET
    ;

udfSuffixPaths
    : suffixPath (COMMA suffixPath)*
    ;

udfAttribute
    : COMMA udfAttributeKey=stringLiteral OPERATOR_EQ udfAttributeValue=stringLiteral
    ;

builtInFunctionCall
    : functionName LR_BRACKET suffixPath RR_BRACKET
    ;

functionName
    : MIN_TIME
    | MAX_TIME
    | MIN_VALUE
    | MAX_VALUE
    | COUNT
    | AVG
    | FIRST_VALUE
    | SUM
    | LAST_VALUE
    ;

functionAsClause
    : builtInFunctionCall (AS ID)?
    ;

lastClause
    : LAST suffixPath (COMMA suffixPath)*
    | LAST asClause (COMMA asClause)*
    ;

asClause
    : suffixPath (AS ID)?
    ;

alias
    : LR_BRACKET ID RR_BRACKET
    ;

alterClause
    : RENAME beforeName=ID TO currentName=ID
    | SET property (COMMA property)*
    | DROP ID (COMMA ID)*
    | ADD TAGS property (COMMA property)*
    | ADD ATTRIBUTES property (COMMA property)*
    | UPSERT aliasClause? tagClause? attributeClause?
    ;

aliasClause
    : ALIAS OPERATOR_EQ ID
    ;

attributeClauses
    : DATATYPE OPERATOR_EQ dataType (COMMA ENCODING OPERATOR_EQ encoding)?
    (COMMA (COMPRESSOR | COMPRESSION) OPERATOR_EQ compressor)?
    (COMMA property)*
    tagClause?
    attributeClause?
    ;

compressor
    : UNCOMPRESSED
    | SNAPPY
    | LZ4
    ;

attributeClause
    : ATTRIBUTES LR_BRACKET property (COMMA property)* RR_BRACKET
    ;

tagClause
    : TAGS LR_BRACKET property (COMMA property)* RR_BRACKET
    ;

setClause
    : SET setCol (COMMA setCol)*
    ;

whereClause
    : WHERE (orExpression | indexPredicateClause)
    ;

showWhereClause
    : WHERE (property | containsExpression)
    ;
containsExpression
    : name=ID OPERATOR_CONTAINS value=propertyValue
    ;

orExpression
    : andExpression (OPERATOR_OR andExpression)*
    ;

andExpression
    : predicate (OPERATOR_AND predicate)*
    ;

predicate
    : (TIME | TIMESTAMP | suffixPath | fullPath) comparisonOperator constant
    | (TIME | TIMESTAMP | suffixPath | fullPath) inClause
    | OPERATOR_NOT? LR_BRACKET orExpression RR_BRACKET
    ;

inClause
    : OPERATOR_NOT? OPERATOR_IN LR_BRACKET constant (COMMA constant)* RR_BRACKET
    ;

fromClause
    : FROM prefixPath (COMMA prefixPath)*
    ;

specialClause
    : specialLimit #specialLimitStatement
    | orderByTimeClause specialLimit? #orderByTimeStatement
    | groupByTimeClause orderByTimeClause? specialLimit? #groupByTimeStatement
    | groupByFillClause orderByTimeClause? specialLimit? #groupByFillStatement
    | fillClause slimitClause? alignByDeviceClauseOrDisableAlign? #fillStatement
    | alignByDeviceClauseOrDisableAlign #alignByDeviceStatementOrDisableAlignInSpecialClause
    | groupByLevelClause orderByTimeClause? specialLimit? #groupByLevelStatement
    ;

specialLimit
    : limitClause slimitClause? alignByDeviceClauseOrDisableAlign? #limitStatement
    | slimitClause limitClause? alignByDeviceClauseOrDisableAlign? #slimitStatement
    | alignByDeviceClauseOrDisableAlign #alignByDeviceClauseOrDisableAlignInSpecialLimit
    ;

orderByTimeClause
    : ORDER BY TIME (DESC | ASC)?
    ;

limitClause
    : LIMIT INT offsetClause?
    | offsetClause? LIMIT INT
    ;

offsetClause
    : OFFSET INT
    ;

slimitClause
    : SLIMIT INT soffsetClause?
    | soffsetClause? SLIMIT INT
    ;

soffsetClause
    : SOFFSET INT
    ;

alignByDeviceClause
    : ALIGN BY DEVICE
    | GROUP BY DEVICE
    ;

disableAlign
    : DISABLE ALIGN
    ;

alignByDeviceClauseOrDisableAlign
    : alignByDeviceClause
    | disableAlign
    ;

fillClause
    : FILL LR_BRACKET typeClause (COMMA typeClause)* RR_BRACKET
    ;

groupByTimeClause
    : GROUP BY LR_BRACKET
      timeInterval
      COMMA DURATION
      (COMMA DURATION)?
      RR_BRACKET
    | GROUP BY LR_BRACKET
            timeInterval
            COMMA DURATION
            (COMMA DURATION)?
            RR_BRACKET
            COMMA LEVEL OPERATOR_EQ INT
    ;

groupByFillClause
    : GROUP BY LR_BRACKET
      timeInterval
      COMMA DURATION
      RR_BRACKET
      FILL LR_BRACKET typeClause (COMMA typeClause)* RR_BRACKET
     ;

groupByLevelClause
    : GROUP BY LEVEL OPERATOR_EQ INT
    ;

typeClause
    : (dataType | ALL) LS_BRACKET linearClause RS_BRACKET
    | (dataType | ALL) LS_BRACKET previousClause RS_BRACKET
    | (dataType | ALL) LS_BRACKET previousUntilLastClause RS_BRACKET
    ;

linearClause
    : LINEAR (COMMA aheadDuration=DURATION COMMA behindDuration=DURATION)?
    ;

previousClause
    : PREVIOUS (COMMA DURATION)?
    ;

previousUntilLastClause
    : PREVIOUSUNTILLAST (COMMA DURATION)?
    ;

indexWithClause
    : WITH INDEX OPERATOR_EQ indexName=ID (COMMA property)*
    ;

topClause
    : TOP INT
    ;

indexPredicateClause
    : (suffixPath | fullPath) LIKE sequenceClause
    | (suffixPath | fullPath) CONTAIN sequenceClause WITH TOLERANCE constant (CONCAT sequenceClause WITH TOLERANCE constant)*
    ;


sequenceClause
    : LR_BRACKET constant (COMMA constant)* RR_BRACKET
    ;

comparisonOperator
    : type = OPERATOR_GT
    | type = OPERATOR_GTE
    | type = OPERATOR_LT
    | type = OPERATOR_LTE
    | type = OPERATOR_EQ
    | type = OPERATOR_NEQ
    ;

insertColumnsSpec
    : LR_BRACKET (TIMESTAMP|TIME) (COMMA nodeNameWithoutStar)+ RR_BRACKET
    ;

insertValuesSpec
    : LR_BRACKET dateFormat (COMMA constant)+ RR_BRACKET
    | LR_BRACKET INT (COMMA constant)+ RR_BRACKET
    ;

setCol
    : suffixPath OPERATOR_EQ constant
    ;

privileges
    : stringLiteral (COMMA stringLiteral)*
    ;

rootOrId
    : ROOT
    | ID
    ;

timeInterval
    : LS_BRACKET startTime=timeValue COMMA endTime=timeValue RR_BRACKET
    | LR_BRACKET startTime=timeValue COMMA endTime=timeValue RS_BRACKET
    ;

timeValue
    : dateFormat
    | dateExpression
    | INT
    ;

propertyValue
    : INT
    | ID
    | stringLiteral
    | constant
    ;

fullPath
    : ROOT (DOT nodeNameWithoutStar)*
    ;

prefixPath
    : ROOT (DOT nodeName)*
    ;

suffixPath
    : nodeName (DOT nodeName)*
    ;

nodeName
    : ID
    | STAR
    | DOUBLE_QUOTE_STRING_LITERAL
    | ID STAR
    | DURATION
    | encoding
    | dataType
    | dateExpression
    | MINUS? EXPONENT
    | MINUS? INT
    | booleanClause
    | CREATE
    | INSERT
    | UPDATE
    | DELETE
    | SELECT
    | SHOW
    | GRANT
    | INTO
    | SET
    | WHERE
    | FROM
    | TO
    | BY
    | DEVICE
    | CONFIGURATION
    | DESCRIBE
    | SLIMIT
    | LIMIT
    | UNLINK
    | OFFSET
    | SOFFSET
    | FILL
    | LINEAR
    | PREVIOUS
    | PREVIOUSUNTILLAST
    | METADATA
    | TIMESERIES
    | TIMESTAMP
    | PROPERTY
    | WITH
    | DATATYPE
    | COMPRESSOR
    | STORAGE
    | GROUP
    | LABEL
    | ADD
    | UPSERT
    | VALUES
    | NOW
    | LINK
    | INDEX
    | USING
    | ON
    | DROP
    | MERGE
    | LIST
    | USER
    | PRIVILEGES
    | ROLE
    | ALL
    | OF
    | ALTER
    | PASSWORD
    | REVOKE
    | LOAD
    | WATERMARK_EMBEDDING
    | UNSET
    | TTL
    | FLUSH
    | TASK
    | INFO
    | VERSION
    | REMOVE
    | MOVE
    | CHILD
    | PATHS
    | DEVICES
    | COUNT
    | NODES
    | LEVEL
    | MIN_TIME
    | MAX_TIME
    | MIN_VALUE
    | MAX_VALUE
    | AVG
    | FIRST_VALUE
    | SUM
    | LAST_VALUE
    | LAST
    | DISABLE
    | ALIGN
    | COMPRESSION
    | TIME
    | ATTRIBUTES
    | TAGS
    | RENAME
    | FULL
    | CLEAR
    | CACHE
    | SNAPSHOT
    | FOR
    | SCHEMA
    | TRACING
    | OFF
    | (ID | OPERATOR_IN)? LS_BRACKET ID? RS_BRACKET ID?
    | compressor
    | GLOBAL
    | PARTITION
    | DESC
    | ASC
    | CLIENTS
    ;

nodeNameWithoutStar
    : ID
    | DOUBLE_QUOTE_STRING_LITERAL
    | DURATION
    | encoding
    | dataType
    | dateExpression
    | MINUS? EXPONENT
    | MINUS? INT
    | booleanClause
    | CREATE
    | INSERT
    | UPDATE
    | DELETE
    | SELECT
    | SHOW
    | GRANT
    | INTO
    | SET
    | WHERE
    | FROM
    | TO
    | BY
    | DEVICE
    | CONFIGURATION
    | DESCRIBE
    | SLIMIT
    | LIMIT
    | UNLINK
    | OFFSET
    | SOFFSET
    | FILL
    | LINEAR
    | PREVIOUS
    | PREVIOUSUNTILLAST
    | METADATA
    | TIMESERIES
    | TIMESTAMP
    | PROPERTY
    | WITH
    | DATATYPE
    | COMPRESSOR
    | STORAGE
    | GROUP
    | LABEL
    | ADD
    | UPSERT
    | VALUES
    | NOW
    | LINK
    | INDEX
    | USING
    | ON
    | DROP
    | MERGE
    | LIST
    | USER
    | PRIVILEGES
    | ROLE
    | ALL
    | OF
    | ALTER
    | PASSWORD
    | REVOKE
    | LOAD
    | WATERMARK_EMBEDDING
    | UNSET
    | TTL
    | FLUSH
    | TASK
    | INFO
    | VERSION
    | REMOVE
    | MOVE
    | CHILD
    | PATHS
    | DEVICES
    | COUNT
    | NODES
    | LEVEL
    | MIN_TIME
    | MAX_TIME
    | MIN_VALUE
    | MAX_VALUE
    | AVG
    | FIRST_VALUE
    | SUM
    | LAST_VALUE
    | LAST
    | DISABLE
    | ALIGN
    | COMPRESSION
    | TIME
    | ATTRIBUTES
    | TAGS
    | RENAME
    | FULL
    | CLEAR
    | CACHE
    | SNAPSHOT
    | FOR
    | SCHEMA
    | TRACING
    | OFF
    | (ID | OPERATOR_IN)? LS_BRACKET ID? RS_BRACKET ID?
    | compressor
    | GLOBAL
    | PARTITION
    | DESC
    | ASC
    | CLIENTS
    ;

dataType
    : INT32 | INT64 | FLOAT | DOUBLE | BOOLEAN | TEXT
    ;

dateFormat
    : DATETIME
    | NOW LR_BRACKET RR_BRACKET
    ;

constant
    : dateExpression
    | NaN
    | MINUS? realLiteral
    | MINUS? INT
    | stringLiteral
    | booleanClause
    ;

booleanClause
    : TRUE
    | FALSE
    ;

dateExpression
    : dateFormat ((PLUS | MINUS) DURATION)*
    ;

encoding
    : PLAIN | PLAIN_DICTIONARY | RLE | DIFF | TS_2DIFF | GORILLA | REGULAR
    ;

realLiteral
    :   INT DOT (INT | EXPONENT)?
    |   DOT  (INT|EXPONENT)
    |   EXPONENT
    ;

property
    : name=ID OPERATOR_EQ value=propertyValue
    ;

autoCreateSchema
    : booleanClause
    | booleanClause INT
    ;

//============================
// Start of the keywords list
//============================
CREATE
    : C R E A T E
    ;

INSERT
    : I N S E R T
    ;

UPDATE
    : U P D A T E
    ;

DELETE
    : D E L E T E
    ;

SELECT
    : S E L E C T
    ;

SHOW
    : S H O W
    ;

GRANT
    : G R A N T
    ;

INTO
    : I N T O
    ;

SET
    : S E T
    ;

WHERE
    : W H E R E
    ;

FROM
    : F R O M
    ;

TO
    : T O
    ;

ORDER
    : O R D E R
    ;

BY
    : B Y
    ;

DEVICE
    : D E V I C E
    ;

CONFIGURATION
    : C O N F I G U R A T I O N
    ;

DESCRIBE
    : D E S C R I B E
    ;

SLIMIT
    : S L I M I T
    ;

LIMIT
    : L I M I T
    ;

UNLINK
    : U N L I N K
    ;

OFFSET
    : O F F S E T
    ;

SOFFSET
    : S O F F S E T
    ;

FILL
    : F I L L
    ;

LINEAR
    : L I N E A R
    ;

PREVIOUS
    : P R E V I O U S
    ;

PREVIOUSUNTILLAST
    : P R E V I O U S U N T I L L A S T
    ;

METADATA
    : M E T A D A T A
    ;

TIMESERIES
    : T I M E S E R I E S
    ;

TIMESTAMP
    : T I M E S T A M P
    ;

PROPERTY
    : P R O P E R T Y
    ;

WITH
    : W I T H
    ;

ROOT
    : R O O T
    ;

DATATYPE
    : D A T A T Y P E
    ;

COMPRESSOR
    : C O M P R E S S O R
    ;

STORAGE
    : S T O R A G E
    ;

GROUP
    : G R O U P
    ;

LABEL
    : L A B E L
    ;

INT32
    : I N T '3' '2'
    ;

INT64
    : I N T '6' '4'
    ;

FLOAT
    : F L O A T
    ;

DOUBLE
    : D O U B L E
    ;

BOOLEAN
    : B O O L E A N
    ;

TEXT
    : T E X T
    ;

ENCODING
    : E N C O D I N G
    ;

PLAIN
    : P L A I N
    ;

PLAIN_DICTIONARY
    : P L A I N '_' D I C T I O N A R Y
    ;

RLE
    : R L E
    ;

DIFF
    : D I F F
    ;

TS_2DIFF
    : T S '_' '2' D I F F
    ;

GORILLA
    : G O R I L L A
    ;


REGULAR
    : R E G U L A R
    ;

BITMAP
    : B I T M A P
    ;

ADD
    : A D D
    ;

UPSERT
    : U P S E R T
    ;

ALIAS
    : A L I A S
    ;

VALUES
    : V A L U E S
    ;

NOW
    : N O W
    ;

LINK
    : L I N K
    ;

INDEX
    : I N D E X
    ;

USING
    : U S I N G
    ;

TRACING
    : T R A C I N G
    ;

ON
    : O N
    ;

OFF
    : O F F
    ;

DROP
    : D R O P
    ;

MERGE
    : M E R G E
    ;

LIST
    : L I S T
    ;

USER
    : U S E R
    ;

PRIVILEGES
    : P R I V I L E G E S
    ;

ROLE
    : R O L E
    ;

ALL
    : A L L
    ;

OF
    : O F
    ;

ALTER
    : A L T E R
    ;

PASSWORD
    : P A S S W O R D
    ;

REVOKE
    : R E V O K E
    ;

LOAD
    : L O A D
    ;

WATERMARK_EMBEDDING
    : W A T E R M A R K '_' E M B E D D I N G
    ;

UNSET
    : U N S E T
    ;

TTL
    : T T L
    ;

FLUSH
    : F L U S H
    ;

TASK
    : T A S K
    ;

INFO
    : I N F O
    ;

VERSION
    : V E R S I O N
    ;

REMOVE
    : R E M O V E
    ;
MOVE
    : M O V E
    ;

CHILD
    : C H I L D
    ;

PATHS
    : P A T H S
    ;

DEVICES
    : D E V I C E S
    ;

COUNT
    : C O U N T
    ;

NODES
    : N O D E S
    ;

LEVEL
    : L E V E L
    ;

MIN_TIME
    : M I N UNDERLINE T I M E
    ;

MAX_TIME
    : M A X UNDERLINE T I M E
    ;

MIN_VALUE
    : M I N UNDERLINE V A L U E
    ;

MAX_VALUE
    : M A X UNDERLINE V A L U E
    ;

AVG
    : A V G
    ;

FIRST_VALUE
    : F I R S T UNDERLINE V A L U E
    ;

SUM
    : S U M
    ;

LAST_VALUE
    : L A S T UNDERLINE V A L U E
    ;

LAST
    : L A S T
    ;

DISABLE
    : D I S A B L E
    ;

ALIGN
    : A L I G N
    ;

COMPRESSION
    : C O M P R E S S I O N
    ;

TIME
    : T I M E
    ;

ATTRIBUTES
    : A T T R I B U T E S
    ;

TAGS
    : T A G S
    ;

RENAME
    : R E N A M E
    ;

GLOBAL
  : G L O B A L
  | G
  ;

FULL
    : F U L L
    ;

CLEAR
    : C L E A R
    ;

CACHE
    : C A C H E
    ;

TRUE
    : T R U E
    ;

FALSE
    : F A L S E
    ;

UNCOMPRESSED
    : U N C O M P R E S S E D
    ;

SNAPPY
    : S N A P P Y
    ;

GZIP
    : G Z I P
    ;

LZO
    : L Z O
    ;

PAA
    : P A A
    ;

PLA
   : P L A
   ;

LZ4
   : L Z '4' 
   ;

LATEST
    : L A T E S T
    ;

PARTITION
    : P A R T I T I O N
    ;

SNAPSHOT
    : S N A P S H O T
    ;

FOR
    : F O R
    ;

SCHEMA
    : S C H E M A
    ;

TEMPORARY
    : T E M P O R A R Y
    ;

FUNCTION
    : F U N C T I O N
    ;

FUNCTIONS
    : F U N C T I O N S
    ;

AS
    : A S
    ;

DESC
    : D E S C
    ;
ASC
    : A S C
    ;

TOP
    : T O P
    ;

CONTAIN
    : C O N T A I N
    ;

CONCAT
    : C O N C A T
    ;

LIKE
    : L I K E
    ;

TOLERANCE
    : T O L E R A N C E
    ;

CLIENTS
    : C L I E N T S
    ;
//============================
// End of the keywords list
//============================
COMMA : ',';

STAR : '*';

OPERATOR_EQ : '=' | '==';

OPERATOR_GT : '>';

OPERATOR_GTE : '>=';

OPERATOR_LT : '<';

OPERATOR_LTE : '<=';

OPERATOR_NEQ : '!=' | '<>';

OPERATOR_IN : I N;

OPERATOR_AND
    : A N D
    | '&'
    | '&&'
    ;

OPERATOR_OR
    : O R
    | '|'
    | '||'
    ;

OPERATOR_NOT
    : N O T | '!'
    ;

OPERATOR_CONTAINS
    : C O N T A I N S
    ;

MINUS : '-';

PLUS : '+';

DOT : '.';

LR_BRACKET : '(';

RR_BRACKET : ')';

LS_BRACKET : '[';

RS_BRACKET : ']';

L_BRACKET : '{';

R_BRACKET : '}';

UNDERLINE : '_';

NaN : 'NaN';

stringLiteral
   : SINGLE_QUOTE_STRING_LITERAL
   | DOUBLE_QUOTE_STRING_LITERAL
   ;

INT : [0-9]+;

EXPONENT : INT ('e'|'E') ('+'|'-')? INT ;

DURATION
    :
    (INT+ (Y|M O|W|D|H|M|S|M S|U S|N S))+
    ;

DATETIME
    : INT ('-'|'/') INT ('-'|'/') INT
      ((T | WS)
      INT ':' INT ':' INT (DOT INT)?
      (('+' | '-') INT ':' INT)?)?
    ;

/** Allow unicode rule/token names */
ID : FIRST_NAME_CHAR NAME_CHAR*;

fragment
NAME_CHAR
    :   'A'..'Z'
    |   'a'..'z'
    |   '0'..'9'
    |   '_'
    |   '-'
    |   ':'
    |   '/'
    |   '@'
    |   '#'
    |   '$'
    |   '%'
    |   '&'
    |   '+'
    |   CN_CHAR
    ;

fragment
FIRST_NAME_CHAR
    :   'A'..'Z'
    |   'a'..'z'
    |   '0'..'9'
    |   '_'
    |   '/'
    |   '@'
    |   '#'
    |   '$'
    |   '%'
    |   '&'
    |   '+'
    |   CN_CHAR
    ;

fragment CN_CHAR
  : '\u2E80'..'\u9FFF'
  ;

DOUBLE_QUOTE_STRING_LITERAL
    : '"' ('\\' . | ~'"' )*? '"'
    ;

SINGLE_QUOTE_STRING_LITERAL
    : '\'' ('\\' . | ~'\'' )*? '\''
    ;

//Characters and write it this way for case sensitivity
fragment A
    : 'a' | 'A'
    ;

fragment B
    : 'b' | 'B'
    ;

fragment C
    : 'c' | 'C'
    ;

fragment D
    : 'd' | 'D'
    ;

fragment E
    : 'e' | 'E'
    ;

fragment F
    : 'f' | 'F'
    ;

fragment G
    : 'g' | 'G'
    ;

fragment H
    : 'h' | 'H'
    ;

fragment I
    : 'i' | 'I'
    ;

fragment J
    : 'j' | 'J'
    ;

fragment K
    : 'k' | 'K'
    ;

fragment L
    : 'l' | 'L'
    ;

fragment M
    : 'm' | 'M'
    ;

fragment N
    : 'n' | 'N'
    ;

fragment O
    : 'o' | 'O'
    ;

fragment P
    : 'p' | 'P'
    ;

fragment Q
    : 'q' | 'Q'
    ;

fragment R
    : 'r' | 'R'
    ;

fragment S
    : 's' | 'S'
    ;

fragment T
    : 't' | 'T'
    ;

fragment U
    : 'u' | 'U'
    ;

fragment V
    : 'v' | 'V'
    ;

fragment W
    : 'w' | 'W'
    ;

fragment X
    : 'x' | 'X'
    ;

fragment Y
    : 'y' | 'Y'
    ;

fragment Z
    : 'z' | 'Z'
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;