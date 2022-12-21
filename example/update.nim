import detanim, json

let
    r = newDeta()
    base = r.newBase("sample")

let toUpdates = %*{
    "hello": "123",
    "another": "one",
    "number": base.util.increment(5),
    "boolean": base.util.trim(),
    }

base.update(toUpdates, "123")

