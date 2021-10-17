import detanim, os, json

let 
    r = Deta(getEnv("API_KEY"))
    base = r.Base("sample")

echo "putting new data: [hello]"
let x = base.put(@[%*{"key": "123", "hello": "world"}])
echo $x


echo "getting data: [hello]"
let hello = base.get("123")
echo $hello
    
