import threading


def test():
    print("hey!")
    return 23


a = threading.Thread(
    target=test
)
a.start()
a.join()
print(a.get())