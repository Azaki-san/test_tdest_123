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
id_ = -1
channel_id = -1
suc = -1
pred = -1


def populate_finger_table():
    global finger_table, suc, pred, id_

    def get_successor(target):
        n = -1
        for ch in CHORD:
            if ch >= target:
                n = ch
                break
        if n != -1:
            return n
        return CHORD[0]

    for i in range(M):
        finger_table.append(get_successor((id_ + 2 ** i) % (2 ** M)))

    print(f"Node {id_} finger table {finger_table}")
    return finger_table


def get_stub(channel):
    channel = grpc.insecure_channel(channel)
    return pb2_grpc.ChordStub(channel)


def get_target_id(key):
    hash_value = zlib.adler32(key.encode())
    return hash_value % (2 ** M)


def successor(ring, target):
    n = -1
    for ch in ring:
        if ch > target:
            n = ch
            break
    if n != -1:
        return n
    return ring[0]


def get_closest_preceding_node(target):
    for i in range(M):
        if successor(finger_table, finger_table[i]) == successor(finger_table, target):
            return finger_table[i]
    return id_


def lookup(target):
    if pred < target <= id_ or (pred >= id_ and (-pred < target <= id_)):
        return id_
    elif id_ < target <= suc or (suc <= id_ and (-id_ < target <= suc)):
        return suc
    else:
        n = get_closest_preceding_node(target)
        return n


def save(key, text):
    target_id = get_target_id(key)
    final_id = lookup(target_id)
    if final_id == id_:
        data[key] = text
        print(f"Node {id_} says: Saved {key}")
    else:
        print(f"Node {id_} says: Save from {id_} to {final_id}")
    return final_id


def remove(key):
    target_id = get_target_id(key)
    final_id = lookup(target_id)
    if final_id == id_:
        if key in data:
            del data[key]
            print(f"Node {id_} says: Removed {key}")
        else:
            return -1
    else:
        print(f"Node {id_} says: Remove from {id_} to {final_id}")
    return final_id


def find(key):
    target_id = get_target_id(key)
    final_id = lookup(target_id)
    if final_id == id_:
        if key in data:
            print(f"Node {id_} says: Found {key}")
    else:
        print(f"Node {id_} says: Find from {id_} to {final_id}")
    return final_id


class NodeHandler(pb2_grpc.ChordServicer):
    def SaveData(self, request, context):
        nid = save(request.key, request.text)
        if nid != id_:
            ch = get_stub(CHANNELS[CHORD.index(nid)])
            return ch.SaveData(pb2.SaveDataMessage(key=request.key, text=request.text))
        return pb2.SaveDataResponse(node_id=nid, status=True)

    def RemoveData(self, request, context):
        nid = remove(request.key)
        if nid == -1:
            return pb2.RemoveDataResponse(node_id=nid, status=False)
        elif nid != id_:
            ch = get_stub(CHANNELS[CHORD.index(nid)])
            return ch.RemoveData(pb2.RemoveDataMessage(key=request.key))
        return pb2.RemoveDataResponse(node_id=nid, status=True)

    def FindData(self, request, context):
        nid = find(request.key)
        if nid != id_:
            ch = get_stub(CHANNELS[CHORD.index(nid)])
            return ch.FindData(pb2.FindDataMessage(key=request.key))
        return pb2.FindDataResponse(node_id=nid, data=data.get(request.key) if request.key in data else "")

    def GetFingerTable(self, request, context):
        return pb2.GetFingerTableResponse(finger_table=finger_table)


if __name__ == "__main__":
    CHORD.sort()
    CHANNELS.sort()
    node_port = str(5000 + node_id)
    node = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_ChordServicer_to_server(NodeHandler(), node)
    node.add_insecure_port("127.0.0.1:" + node_port)
    node.start()
    id_ = CHORD[node_id]
    channel_id = node_id
    suc = CHORD[(node_id + 1) if node_id < len(CHORD) - 1 else 0]
    pred = CHORD[node_id - 1]
    populate_finger_table()
    try:
        node.wait_for_termination()
    except KeyboardInterrupt:
        print("Shutting down")
