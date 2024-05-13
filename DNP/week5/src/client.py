import grpc
import sys
import chord_pb2 as pb2
import chord_pb2_grpc as pb2_grpc

node_channel = ""
CHANNELS = [
    "127.0.0.1:5000",
    "127.0.0.1:5001",
    "127.0.0.1:5002",
    "127.0.0.1:5003",
    "127.0.0.1:5004",
    "127.0.0.1:5005",
]


def get_stub(channel):
    channel = grpc.insecure_channel(channel)
    return pb2_grpc.ChordStub(channel)


if __name__ == "__main__":
    while True:
        try:
            inp = input("> ")
            splits = inp.split(" ")

            if splits[0] == "connect":
                node_channel = CHANNELS[int(splits[1])]
                print(f"Connected To Node {splits[1]}")

            elif splits[0] == "get_finger_table":
                stub = get_stub(node_channel)
                out = stub.GetFingerTable(pb2.GetFingerTableMessage())
                print(out.finger_table)

            elif splits[0] == "save":
                key = splits[1]
                text = inp[6 + len(key): len(inp)]
                stub = get_stub(node_channel)

                res = stub.SaveData(pb2.SaveDataMessage(key=key, text=text))

                if not res.status:
                    print("Failure, key was not saved")

                else:
                    print(f"Success, {key} was saved in node {str(res.node_id)}")

            elif splits[0] == "remove":
                key = splits[1]
                stub = get_stub(node_channel)

                res = stub.RemoveData(pb2.RemoveDataMessage(key=key))

                if not res.status:
                    print("Failure, key was not removed")

                else:
                    print(f"Success, {key} was removed from node {str(res.node_id)}")

            elif splits[0] == "find":
                key = splits[1]
                stub = get_stub(node_channel)

                res = stub.FindData(pb2.FindDataMessage(key=key))

                if res.data == "":
                    print("Failure, data was not found")

                else:
                    print(
                        f"Success, {key} was found in node {str(res.node_id)} with data {str(res.data)}"
                    )

            elif splits[0] == "quit":
                print("Shutting Down")
                break

            else:
                print("Unrecognised command\n")

        except KeyboardInterrupt:
            print("Shutting Down")
            sys.exit(0)
