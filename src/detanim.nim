import asyncdispatch, httpclient, os, strutils, strformat, json, uri

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

