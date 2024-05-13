import grpc
import sys
import zlib
from concurrent import futures
import chord_pb2_grpc as pb2_grpc
import chord_pb2 as pb2

node_id = int(sys.argv[1])

CHORD = [2, 16, 24, 25, 26, 31]
CHANNELS = [
    "127.0.0.1:5000",
    "127.0.0.1:5001",
    "127.0.0.1:5002",
    "127.0.0.1:5003",
    "127.0.0.1:5004",
    "127.0.0.1:5005",
]

data = {}
finger_table = []

M = 5
id = -1
succ = -1
pred = -1


def populate_finger_table(node_id):
    global finger_table

    def find_successor(target):
        global CHORD
        for i in range(len(CHORD)):
            if CHORD[i] > target:
                return CHORD[i]
        return CHORD[0]

    def find_predecessor(target):
        global CHORD
        for i in range(len(CHORD) - 1, -1, -1):
            if CHORD[i] < target:
                return CHORD[i]
        return CHORD[-1]

    finger_table = [find_successor((node_id + 2 ** i) % (2 ** M)) for i in range(M)]
    print(f"Node {node_id} finger table {finger_table}")
    return


def get_stub(channel):
    channel = grpc.insecure_channel(channel)
    return pb2_grpc.ChordStub(channel)


def get_target_id(key):
    hash_value = zlib.adler32(key.encode())
    return hash_value % (2 ** M)


def save(key, text):
    target_id = get_target_id(key)
    if pred < id:
        if pred < target_id <= id:
            data[key] = text
            print(f"Node {id} saved {key}")
            return id
    elif target_id <= id or target_id > pred:
        data[key] = text
        print(f"Node {id} saved {key}")
        return id
    else:
        stub = get_stub(CHANNELS[0])  # Assuming first node to contact
        res = stub.SaveData(pb2.SaveDataMessage(key=key, text=text))
        print(f"Node {id} forwarded save request to {res.node_id}")
        return res.node_id


def remove(key):
    if key in data:
        del data[key]
        print(f"Node {id} removed {key}")
        return id
    else:
        stub = get_stub(CHANNELS[0])  # Assuming first node to contact
        res = stub.RemoveData(pb2.RemoveDataMessage(key=key))
        print(f"Node {id} forwarded remove request to {res.node_id}")
        return res.node_id


def find(key):
    if key in data:
        print(f"Node {id} found {key}")
        return pb2.FindDataResponse(node_id=id, data=data[key])
    else:
        stub = get_stub(CHANNELS[0])  # Assuming first node to contact
        res = stub.FindData(pb2.FindDataMessage(key=key))
        print(f"Node {id} forwarded find request to {res.node_id}")
        return res


class NodeHandler(pb2_grpc.ChordServicer):
    def SaveData(self, request, context):
        return pb2.SaveDataResponse(node_id=save(request.key, request.text))

    def RemoveData(self, request, context):
        return pb2.RemoveDataResponse(node_id=remove(request.key))

    def FindData(self, request, context):
        return find(request.key)

    def GetFingerTable(self, request, context):
        populate_finger_table(node_id)
        return pb2.GetFingerTableResponse(finger_table=finger_table)


if __name__ == "__main__":
    node_port = str(5000 + node_id)
    node = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_ChordServicer_to_server(NodeHandler(), node)
    node.add_insecure_port("127.0.0.1:" + node_port)
    node.start()

    populate_finger_table(node_id)  # Print finger table upon starting

    try:
        node.wait_for_termination()
    except KeyboardInterrupt:
        print("Shutting down")
