import grpc
import sys
import zlib
from concurrent import futures
import chord_pb2_grpc as pb2_grpc
import chord_pb2 as pb2

node_id = 0

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
id_ = -1
channel_id = -1
succ = -1
pred = -1


def populate_finger_table():
    global finger_table, succ, pred, id_

    def successor(ring, position):
        next_higher = None
        for element in ring:
            if element >= position:
                next_higher = element
                break

        return next_higher if next_higher is not None else ring[0]

    for i in range(M):
        finger_table.append(
            successor(CHORD, (id_ + 2 ** i) % (2 ** M)))

    print(f"Node {id_} finger table {finger_table}")
    return finger_table


def get_stub(channel):
    channel = grpc.insecure_channel(channel)
    return pb2_grpc.ChordStub(channel)


def get_target_id(key):
    hash_value = zlib.adler32(key.encode())
    return hash_value % (2 ** M)


def successor(ring, position):
    next_higher = None
    for element in ring:
        if element > position:
            next_higher = element
            break

    return next_higher if next_higher is not None else ring[0]


def get_closest_preceding_node(target):
    for i in range(M - 1, -1, -1):
        if successor(finger_table, finger_table[i]) == successor(finger_table, target):
            return finger_table[i]
    return id_


def lookup(target):
    if pred < target <= id_:
        return get_stub(CHANNELS[channel_id])
    elif id_ < target <= succ:
        return get_stub(CHANNELS[CHORD.index(succ)])
    else:
        n = get_closest_preceding_node(target)


def save(key, text):
    # Hash value of the key
    target_id = get_target_id(key)

    if id == target_id:
        data[key] = text
        return id
    else:
        stub = get_stub(CHANNELS[finger_table[0]])
        return stub.SaveData(pb2.SaveDataMessage(key=key, text=text)).node_id


def remove(key):
    target_id = get_target_id(key)
    if id == target_id and key in data:
        del data[key]
        return id
    else:
        stub = get_stub(CHANNELS[finger_table[0]])
        return stub.RemoveData(pb2.RemoveDataMessage(key=key)).node_id


def find(key):
    target_id = get_target_id(key)
    if id == target_id:
        if key in data:
            return id, data[key]
        else:
            return id, ""
    elif (pred < id <= target_id) or (pred > id and (target_id > pred or target_id <= id)):
        return id, id
    elif (id < target_id < finger_table[0]) or (finger_table[0] < id < target_id) or (
            id < target_id and finger_table[0] < id):
        stub = get_stub(CHANNELS[finger_table[0]])
        return stub.FindData(pb2.FindDataMessage(key=key)).node_id, ""
    else:
        for i in range(1, M):
            if finger_table[i] <= target_id < finger_table[i + 1]:
                stub = get_stub(CHANNELS[finger_table[i]])
                return stub.FindData(pb2.FindDataMessage(key=key)).node_id, ""


class NodeHandler(pb2_grpc.ChordServicer):
    def SaveData(self, request, context):
        node_id = save(request.key, request.text)
        return pb2.SaveDataResponse(node_id=node_id, status=True)

    def RemoveData(self, request, context):
        node_id = remove(request.key)
        return pb2.RemoveDataResponse(node_id=node_id, status=True)

    def FindData(self, request, context):
        node_id, data = find(request.key)
        return pb2.FindDataResponse(node_id=node_id, data=data)

    def GetFingerTable(self, request, context):
        return pb2.GetFingerTableResponse(finger_table=finger_table)


if __name__ == "__main__":
    node_port = str(5000 + node_id)
    node = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_ChordServicer_to_server(NodeHandler(), node)
    node.add_insecure_port("127.0.0.1:" + node_port)
    node.start()
    id_ = CHORD[node_id]
    channel_id = node_id
    succ = CHORD[(node_id + 1) if node_id < len(CHORD) else 0]
    pred = CHORD[node_id - 1]
    populate_finger_table()
    lookup(22)
    try:
        node.wait_for_termination()
    except KeyboardInterrupt:
        print("Shutting down")
