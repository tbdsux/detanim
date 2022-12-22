import detanim

let
    r = newDeta()
    drive = r.newDrive("sample")


let file = drive.put("new.txt", "new.txt")
echo $file

let files = drive.list()
echo $files
