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
