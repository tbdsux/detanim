import std/[asyncdispatch, httpclient, os, strutils, strformat, json, uri, tables, typetraits]

const BASE_ENDPOINT = "https://database.deta.sh/v1"

type
  DetaClient* = ref object of RootObj
    projectId*: string
    projectKey*: string

  # deta base
  BaseMain*[HT] = ref object of DetaClient
    client*: HT
    name*: string
  BaseClient* = BaseMain[HttpClient]
  AsyncBaseClient* = BaseMain[AsyncHttpClient]

  # deta base util functions
  BaseUtil* = ref object of RootObj
    action: string
  BaseUtilIncrement* = ref object of BaseUtil
    val: int
  BaseUtilAppend*[T] = ref object of BaseUtil
    val: T
  BaseUtilPrepend*[T] = ref object of BaseUtil
    val: T
  BaseUtilTrim* = ref object of BaseUtil

  # update table
  UpdateTable* = Table[string, any]

  # custom error
  RequiredVar* = object of CatchableError
  InvalidValue* = object of CatchableError



proc Deta*(projectKey: string = getEnv("DETA_PROJECT_KEY", "")): DetaClient {.raises: [RequiredVar, InvalidValue].} =
  ## New Deta instance.
  
  if projectKey == "":
    raise newException(RequiredVar, "No Project Key set! You can also set your project key in DETA_PROJECT_KEY environment variable.")

  let v = projectKey.split("_")
  if v.len != 2:
    raise newException(InvalidValue, "Invalid Project Key!")
    
  new(result)
  result.projectKey = projectKey
  result.projectId = v[0]


proc Base*(this: DetaClient, name: string): BaseClient =
  ## Instantiate a new Deta Base instance.
  new(result)
  result.projectKey = this.projectKey
  result.projectId = this.projectId
  result.name = name
  result.client = newHttpClient()

proc AsyncBase*(this: DetaClient, name: string): AsyncBaseClient =
  ## Instantiate a new async Deta Base instance.
  new(result)
  result.projectKey = this.projectKey
  result.projectId = this.projectId
  result.name = name
  result.client = newAsyncHttpClient()


proc request(this: BaseClient | AsyncBaseClient, url: string, httpMethod: string | HttpMethod, body: string = ""): Future[string] {.multisync.} =
  this.client.headers = newHttpHeaders({"X-API-Key": this.projectKey, "Content-Type": "application/json"})

  let r = await this.client.request(url = &"{BASE_ENDPOINT}/{this.projectId}/{this.name}" & url, httpMethod = httpMethod, body = body)
  result = await r.body


proc get*(this: BaseClient | AsyncBaseClient, key: string): Future[JsonNode] {.multisync.} =
  let r = await this.request(&"/items/{encodeUrl(key)}", HttpGet)
  
  result = parseJson(r)


proc put*(this: BaseClient | AsyncBaseClient, items: seq[JsonNode]): Future[JsonNode] {.multisync.} =
  let payload = %*{"items": items}
  let r = await this.request(&"/items", HttpPut, $payload)

  result = parseJson(r)


proc delete*(this: BaseClient | AsyncBaseClient, key: string): Future[JsonNode] {.multisync.} =
  let r = await this.request(&"/items/{key}", HttpDelete)

  result = parseJson(r)

proc insert*(this: BaseClient | AsyncBaseClient, item: JsonNode): Future[JsonNode] {.multisync.} =
  let payload = %*{
    "item": item
  }

  let r = await this.request(&"/items", HttpPost, $payload)
  result = parseJson(r)

proc update*(this: BaseClient | AsyncBaseClient, updates: JsonNode, key: string): Future[JsonNode] {.multisync.} =
  new(result)

  var 
    payloadAppend = %*{}
    payloadPrepend = %*{}
    payloadIncrement = %*{}
    payloadSet = %*{}
    payloadTrim = newSeq[string]()

  
  for i, j in updates.pairs:
    case j{"action"}.getStr():
      of "append":
        payloadAppend[i] = %j{"val"}.getStr()
      of "prepent":
        payloadPrepend[i] = %j{"val"}.getStr()
      of "increment":
        payloadIncrement[i] = %j{"val"}.getInt()
      of "trim":
        payloadTrim.add(i)
      else:
        payloadSet[i] = %j

  let payload = %*{
    "set": payloadSet,
    "append": payloadAppend,
    "prepend": payloadPrepend,
    "increment": payloadIncrement,
    "delete": payloadTrim
  }

  let r = await this.request(&"/items/{key}", HttpPatch, $payload)
  result = parseJson(r)
    
proc query*(this: BaseClient | AsyncBaseClient, query: seq[JsonNode], limit: uint = 1, last: string = ""): Future[JsonNode] {.multisync.} =
  let payload = %*{
    "query": query,
    "limit": limit,
    "last": last
  }

  let r = await this.request(&"/query", HttpPost, $payload)
  result = parseJson(r)

proc util*(this: BaseClient | AsyncBaseClient): BaseUtil = 
  new(result)

proc increment*(this: BaseUtil, value: int | float = 1): BaseUtilIncrement =
  new(result)
  result.val = value
  result.action = "increment"

proc append*[T](this: BaseUtil, value: seq[T]): BaseUtilAppend[T] = 
  new(result)
  result.val = value
  result.action = "append"

proc prepend*[T](this: BaseUtil, value: seq[T]): BaseUtilAppend[T] = 
  new(result)
  result.val = value
  result.action = "prepend"

proc trim*(this: BaseUtil): BaseUtilTrim = 
  new(result)
  result.action = "trim"