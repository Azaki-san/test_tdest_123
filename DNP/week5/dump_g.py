import grpc
import sys
import zlib
from concurrent import futures
import chord_pb2_grpc as pb2_grpc
import chord_pb2 as pb2

node_id = sys.argv[1]
# node_id = 0

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


def populate_finger_table(chord, nodeId):
    global finger_table, succ, pred, id
    id = nodeId

    def successor(ring, position):
        next_higher = None
        for element in ring:
            if element >= position:
                next_higher = element
                break

        return next_higher if next_higher is not None else ring[0]

    for i in range(M):
        finger_table.append(
            successor(CHORD, (chord[nodeId] + 2 ** i) % (2 ** M)))
    # def find_successor(target):
    #     for j in range(len(chord)):
    #         if chord[j] >= target:
    #             return chord[j]
    #     return chord[0]
    #
    # def find_predecessor(target):
    #     for j in range(len(chord), -1, -1):
    #         if chord[j] < target:
    #             return chord[j]
    #     return chord[-1]
    #
    # finger_table = []
    # for i in range(M):
    #     finger_id = (chord[nodeId] + 2 ** i) % (2 ** M)
    #     successor = find_successor(finger_id)
    #     finger_table.append(successor)
    # succ = find_successor(nodeId)
    # pred = find_predecessor(nodeId)
    # id = nodeId

    print(f"Node {chord[nodeId]} finger table {finger_table}")
    return finger_table


def get_stub(channel):
    channel = grpc.insecure_channel(channel)
    return pb2_grpc.ChordStub(channel)


def get_target_id(key):
    hash_value = zlib.adler32(key.encode())
    return hash_value % (2 ** M)


def save(key, text):
    return


def remove(key):
    return


def find(key):
    return


class NodeHandler(pb2_grpc.ChordServicer):
    def SaveData(self, request, context):
        key = request.key
        value = request.value
        key_hash = get_target_id(key)
        if pred < id:
            if pred < key_hash <= id:
                data[key] = value
                reply = {'status': True, 'node_id': id}
            else:
                successor_node_id = finger_table[0]  # Assuming finger_table contains the IDs of successor nodes
                successor_channel_index = successor_node_id % len(CHANNELS)  # Map successor node ID to channel index
                successor_stub = get_stub(CHANNELS[successor_channel_index])
                reply = successor_stub.SaveData(pb2.SaveDataMessage(key=key, text=value))
        else:
            if (pred < key_hash <= id) or (0 <= key_hash <= id):
                data[key] = value
                reply = {'status': True, 'node_id': id}
            else:
                successor_node_id = finger_table[0]  # Assuming finger_table contains the IDs of successor nodes
                successor_channel_index = successor_node_id % len(CHANNELS)  # Map successor node ID to channel index
                successor_stub = get_stub(CHANNELS[successor_channel_index])
                reply = successor_stub.SaveData(pb2.SaveDataMessage(key=key, text=value))
        return pb2.SaveDataResponse(**reply)

    def RemoveData(self, request, context):
        reply = {}
        return pb2.RemoveDataResponse(**reply)

    def FindData(self, request, context):
        reply = {}
        return pb2.FindDataResponse(**reply)

    def GetFingerTable(self, request, context):
        reply = {}
        return pb2.GetFingerTableResponse(**reply)


if __name__ == "__main__":

    node_port = str(5000 + int(node_id))
    node = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_ChordServicer_to_server(NodeHandler(), node)
    node.add_insecure_port("127.0.0.1:" + node_port)
    node.start()

    populate_finger_table(CHORD, int(node_id))

    try:
        node.wait_for_termination()
    except KeyboardInterrupt:
        print("Shutting down")
