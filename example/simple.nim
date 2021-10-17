import detanim, os, json

let 
    r = Deta(getEnv("API_KEY"))
    base = r.Base("sample")

    x = base.get("adad")


echo $x