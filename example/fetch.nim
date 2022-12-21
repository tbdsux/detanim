import detanim, std/[sequtils, json, options]

let
    r = newDeta()
    base = r.newBase("sample")


var
    res = base.fetch(limit = 2)
    allItems = res.items

while res.paging.last.isSome():
    res = base.fetch(last = res.paging.last.get())
    allItems = concat(allItems, res.items)


for i in allItems.items:
    echo $i
