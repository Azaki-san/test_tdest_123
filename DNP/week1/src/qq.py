with open("image.png", "rb") as file:
    q = file.read()
    with open("image 2.png", "rb") as file1:
        f = file1.read()
        print("-----")
        print(f[20465:21000])
        print("-----")
        print(q[20465:21000])
        for i in range(len(q)):
            if q[i] != f[i]:
                print(i)
                break