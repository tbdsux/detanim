import detanim, os, json

let 
    r = Deta(getEnv("API_KEY"))
    base = r.Base("sample")

let toUpdates = %*{
    "hello": "123", 
    "another": "one",
    "number": base.util.increment(),
    "bool": base.util.trim(),
    }

let update = base.update(toUpdates, "123")
echo $update
    
