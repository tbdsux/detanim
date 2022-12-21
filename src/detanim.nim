import std/[asyncdispatch, httpclient, os, strformat, json, uri, tables,
    strutils, options]

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
  UpdateTable* = Table[string, auto]

  # custom error
  RequiredVar* = object of CatchableError
  InvalidValue* = object of CatchableError

  # custom request errors
  Http409Conflict* = object of CatchableError
  Http400BadRequest* = object of CatchableError
  Http401Unauthorized* = object of CatchableError





proc newDeta*(projectKey: string = getEnv("DETA_PROJECT_KEY",
    "")): DetaClient {.raises: [RequiredVar, InvalidValue].} =
  ## New Deta instance.

  if projectKey == "":
    raise newException(RequiredVar, "No Project Key set! You can also set your project key in DETA_PROJECT_KEY environment variable.")

  let v = projectKey.split("_")
  if v.len != 2:
    raise newException(InvalidValue, "Invalid Project Key!")

  new(result)
  result.projectKey = projectKey
  result.projectId = v[0]


proc newBase*(this: DetaClient, name: string): BaseClient =
  ## Instantiate a new Deta Base instance.
  new(result)
  result.projectKey = this.projectKey
  result.projectId = this.projectId
  result.name = name
  result.client = newHttpClient()

proc newAsyncBase*(this: DetaClient, name: string): AsyncBaseClient =
  ## Instantiate a new async Deta Base instance.
  new(result)
  result.projectKey = this.projectKey
  result.projectId = this.projectId
  result.name = name
  result.client = newAsyncHttpClient()


proc request(this: BaseClient | AsyncBaseClient, url: string,
    httpMethod: string | HttpMethod, body: string = ""): Future[(int, Option[
    string])] {.multisync.} =
  this.client.headers = newHttpHeaders({"X-API-Key": this.projectKey,
      "Content-Type": "application/json"})

  let req = await this.client.request(url = &"{BASE_ENDPOINT}/{this.projectId}/{this.name}" &
      url, httpMethod = httpMethod, body = body)

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



proc get*(this: BaseClient | AsyncBaseClient, key: string): Future[
    (JsonNode, bool)] {.multisync.} =
  ## `get` retrieves and items from the database with it's key
  ## and returns a json object response

  # check if key is empty or blank
  assert(key.strip() != "", "Key is empty!")

  # send request
  let (status, r) = await this.request(&"/items/{encodeUrl(key)}", HttpGet)

  if status == 404:
    result = (%*{}, false)
    return

  result = (parseJson(r.get()), true)


type
  PutItems* = object
    items*: seq[JsonNode]

  PutResponse* = object
    processed*: Option[PutItems]
    failed*: Option[PutItems]

proc putMany*(this: BaseClient | AsyncBaseClient, items: seq[JsonNode]): Future[
    PutResponse] {.multisync.} =
  ## `putMany` puts multiple items in the database.

  let payload = %*{"items": items}

  # send request
  let (_, r) = await this.request(&"/items", HttpPut, $payload)

  result = to(parseJson(r.get()), PutResponse)


proc put*(this: BaseClient | AsyncBaseClient, item: JsonNode): Future[
    string] {.multisync.} =
  ## `put` is the fastest way to store an item in the database.
  ## If an item already exists under a giver key, `put` will replace this item.
  ## If key is not provided, a 12 char key string is randomly generated.

  let output = await this.putMany(@[item])

  if output.processed.isSome():
    if len(output.processed.get().items) == 1:
      result = output.processed.get().items[0]["key"].getStr()


proc put*(this: BaseClient | AsyncBaseClient, item: JsonNode,
    key: string): Future[string] {.multisync.} =
  ## `put` is the fastest way to store an item in the database.
  ## If an item already exists under a giver key, `put` will replace this item.
  ## If key is not provided, a 12 char key string is randomly generated.

  # check if key is empty or blank
  assert(key.strip() != "", "Key is empty!")

  var it = item
  it["key"] = %key

  let output = await this.putMany(@[it])

  if output.processed.isSome():
    if len(output.processed.get().items) == 1:
      result = output.processed.get().items[0]["key"].getStr()

proc delete*(this: BaseClient | AsyncBaseClient, key: string): Future[
    JsonNode] {.multisync.} =
  ## `delete` deletes an item from the database that matches the key provided.

  # check if key is empty or blank
  assert(key.strip() != "", "Key is empty!")

  # send request
  let (_, r) = await this.request(&"/items/{key}", HttpDelete)

  result = parseJson(r.get())

proc insert*(this: BaseClient | AsyncBaseClient, item: JsonNode): Future[
    JsonNode] {.multisync.} =
  ## `insert` inserts a single item into a Base, but is uniq from `put`
  ## in that will raise an error if the `key` already exists in the database.

  let payload = %*{
    "item": item
  }

  # send request
  let (_, r) = await this.request(&"/items", HttpPost, $payload)
  result = parseJson(r.get())

proc insert*(this: BaseClient | AsyncBaseClient, item: JsonNode,
    key: string): Future[JsonNode] {.multisync.} =
  ## `insert` inserts a single item into a Base, but is uniq from `put`
  ## in that will raise an error if the `key` already exists in the database.

  # check if key is empty or blank
  assert(key.strip() != "", "Key is empty!")

  var it = item
  it["key"] = %it
  let payload = %*{
    "item": it
  }

  # send request
  let (_, r) = await this.request(&"/items", HttpPost, $payload)
  result = parseJson(r.get())

proc update*(this: BaseClient | AsyncBaseClient, updates: JsonNode,
    key: string) {.multisync.} =
  ## `update` updates an existing item from the database.

  # check if key is empty or blank
  assert(key.strip() != "", "Key is empty!")

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
  discard await this.request(&"/items/{key}", HttpPatch, $payload)


proc util*(this: BaseClient | AsyncBaseClient): BaseUtil =
  new(result)

proc increment*(this: BaseUtil, value: int | float = 1): BaseUtilIncrement =
  ## Increment increments the value of an attribute (must be a number).
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


type
  FetchPaging* = object
    size*: int
    last*: Option[string]

  FetchResponse* = object
    paging*: FetchPaging
    items*: seq[JsonNode]

proc fetch*(this: BaseClient | AsyncBaseClient, query: seq[JsonNode] = @[],
    limit: uint = 1, last: string = ""): Future[FetchResponse] {.multisync.} =
  ## `fetch` retrieves a list of items matching a query. It will retrieve everything if not query if provided.
  ## A query is composed of a single query object or a list of queries and in the case of a list, the individual queries are OR'ed.

  let payload = %*{
    "query": query,
    "limit": limit,
    "last": last
  }

  # send request
  let (_, r) = await this.request(&"/query", HttpPost, $payload)

  result = to(parseJson(r.get()), FetchResponse)
