# detanim

Deta SDK for NimLang

## Install

```sh
nimble install https://github.com/TheBoringDude/detanim.git
```

## Usage

```nim
import detanim, os, json

let
    r = Deta(getEnv("API_KEY"))
    base = r.Base("sample")

echo "putting new data for key `hello`"
let x = base.put(@[%*{"key": "123", "hello": "world"}])
echo $x


echo "getting data of key `hello`"
let (data, _) = base.get("123")
echo $data
```

- Async support

  ```nim
  import detanim, json, asyncdispatch

  let
      r = newDeta()
      base = r.newAsyncBase("sample")

  type
      Data = object
          key: string
          hello: string
          number: int
          boolean: bool

  proc main() {.async.} =
      echo "putting new data with key: [123]"
      let key = await base.put(%*{"hello": "world",
              "number": 99, "boolean": false}, "123")

      echo "key: ", key


      echo "getting data with key: [123]"
      let (resp, exists) = await base.get("123")

      if not exists:
          quit("Key does not exist!")

      let data = to(resp, Data)

      echo(data.hello)


  waitFor main()
  ```

## Currently Implemented Functions

- **Base** (not fully tested)

  - get
  - put (only `putMany` style / variant)
  - insert
  - delete
  - query
  - update

- **Drive**
  - [not started]

##

&copy; 2021 | TheBoringDude | [License](./LICENSE0)
