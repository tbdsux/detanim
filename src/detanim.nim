import std/[asyncdispatch, httpclient, os, strutils, strformat, json, uri, tables, strutils, options]

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

  # custom request errors
  Http409Conflict* = object of CatchableError
  Http400BadRequest* = object of CatchableError
  Http401Unauthorized* = object of CatchableError
  




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


proc request(this: BaseClient | AsyncBaseClient, url: string, httpMethod: string | HttpMethod, body: string = ""): Future[(int, Option[string])] {.multisync.} =
  this.client.headers = newHttpHeaders({"X-API-Key": this.projectKey, "Content-Type": "application/json"})

  let req = await this.client.request(url = &"{BASE_ENDPOINT}/{this.projectId}/{this.name}" & url, httpMethod = httpMethod, body = body)
  
  let
    code = req.code
    body = await req.body

  var
    c: int = 0
    b: Option[string] = some("")

  case code:
    of Http404:
      c = 404
      b = none(string)
    of Http401:
      raise newException(Http401Unauthorized, "Unauthorized")
    of Http409:
      let err = parseJson(body)
      raise newException(Http409Conflict, err{"errors"}[0].getStr())
    of Http400:
      raise newException(Http400BadRequest, "Bad Request")
    of Http200, Http201, Http202, HttpCode(207):
      c = 200
      b = some(body)
    else:
      raise newException(HttpRequestError, "Internal Error. If problem persists, please raise an issue.")

  result = (c, b)

proc get*(this: BaseClient | AsyncBaseClient, key: string): Future[Option[JsonNode]] {.multisync.} =
  # check if key is empty or blank
  if key.strip() == "":
    raise ValueError.newException("Key is empty!")

  # send request
  let (status, r) = await this.request(&"/items/{encodeUrl(key)}", HttpGet)

  if status == 400:
    return none(JsonNode)

  result = some(parseJson(r.get()))


proc put*(this: BaseClient | AsyncBaseClient, items: seq[JsonNode]): Future[JsonNode] {.multisync.} =
  let payload = %*{"items": items}
  let (_, r) = await this.request(&"/items", HttpPut, $payload)

  result = parseJson(r.get())


proc delete*(this: BaseClient | AsyncBaseClient, key: string): Future[JsonNode] {.multisync.} =
  # check if key is empty or blank
  if key.strip() == "":
    raise ValueError.newException("Key is empty!")

  # send request
  let (_, r) = await this.request(&"/items/{key}", HttpDelete)

  result = parseJson(r.get())

proc insert*(this: BaseClient | AsyncBaseClient, item: JsonNode): Future[JsonNode] {.multisync.} =
  let payload = %*{
    "item": item
  }

  # send request
  let (_, r) = await this.request(&"/items", HttpPost, $payload)
  result = parseJson(r.get())

proc update*(this: BaseClient | AsyncBaseClient, updates: JsonNode, key: string): Future[JsonNode] {.multisync.} =
  # check if key is empty or blank
  if key.strip() == "":
    raise ValueError.newException("Key is empty!")

  # send request
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

  # send request
  let (_, r) = await this.request(&"/items/{key}", HttpPatch, $payload)
  result = parseJson(r.get())
    
proc query*(this: BaseClient | AsyncBaseClient, query: seq[JsonNode], limit: uint = 1, last: string = ""): Future[JsonNode] {.multisync.} =
  let payload = %*{
    "query": query,
    "limit": limit,
    "last": last
  }

  # send request
  let (_, r) = await this.request(&"/query", HttpPost, $payload)
  result = parseJson(r.get())

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