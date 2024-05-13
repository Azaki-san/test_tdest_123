import argparse
import copy
import threading
from concurrent import futures
from enum import Enum
import random
import grpc
import sched
import raft_pb2 as pb2
import raft_pb2_grpc as pb2_grpc


class States(Enum):
    LEADER = 1
    CANDIDATE = 2
    FOLLOWER = 3


def reset_timeout_election():
    global ELECTION_TIMEOUT
    ELECTION_LOCK.acquire()
    try:
        queue_copy = copy.copy(SCHEDULER.queue)
        for event in queue_copy:
            SCHEDULER.cancel(event)
        ELECTION_TIMEOUT = random.uniform(2.0, 4.0)
        SCHEDULER.enter(ELECTION_TIMEOUT, 1, initiate_selection)
    finally:
        ELECTION_LOCK.release()


def initiate_selection():
    global STATE, TERM, VOTED, VOTES_COUNT
    STATE = States.CANDIDATE
    TERM += 1
    VOTED = True
    VOTES_COUNT = 1
    print("TIMEOUT Expired | Leader Died")
    print(f"STATE: Candidate | Term: {TERM}")
    print(f"Voted for NODE {NODE_ID}")
    send_request_votes()


class Counter:
    def __init__(self):
        self.lock = threading.Lock()
        self.value = 0
        self.value_all = 0

    def increment(self):
        with self.lock:
            self.value += 1

    def increment_all_nodes(self):
        with self.lock:
            self.value_all += 1

    def get_value_all(self):
        with self.lock:
            return self.value_all

    def get_value(self):
        with self.lock:
            return self.value


def send_request_votes():
    global VOTED, VOTES_COUNT

    def send_request(addr, count):
        try:
            channel = grpc.insecure_channel(addr)
            stub = pb2_grpc.RaftNodeStub(channel)
            request = pb2.RequestVoteArgs(
                candidate_id=NODE_ID,
                candidate_term=TERM
            )
            response = stub.RequestVote(request)
            channel.close()
            if response.vote_result:
                count.increment()
                return True
            return False
        except grpc.RpcError:
            return False

    ELECTION_LOCK.acquire()
    try:
        VOTES_COUNT = 1
        threads = []
        counter = Counter()
        for node_id, address in SERVERS_INFO.items():
            if node_id != NODE_ID:
                tt = threading.Thread(target=send_request, args=(address, counter))
                tt.daemon = True
                tt.start()
                threads.append(tt)
        for i in threads:
            i.join()
        VOTES_COUNT += counter.get_value()
        print("Votes aggregated")
        if VOTES_COUNT > counter.get_value_all() / 2:
            become_leader()
        else:
            global STATE
            VOTED = False
            STATE = States.FOLLOWER
            print(f"STATE: Follower | TERM: {TERM}")
            reset_timeout_election()
    finally:
        ELECTION_LOCK.release()


def become_leader():
    global STATE, VOTED, LEADER_ID
    VOTED = False
    LEADER_ID = NODE_ID
    STATE = States.LEADER
    queue_copy = copy.copy(SCHEDULER.queue)
    for event in queue_copy:
        SCHEDULER.cancel(event)
    print(f"STATE: Leader | TERM: {TERM}")
    append_entries()


def append_entries():
    global COMMITED_VALUE, UNCOMMITED_VALUE

    def send_request(addr, count):
        try:
            channel = grpc.insecure_channel(addr)
            stub = pb2_grpc.RaftNodeStub(channel)
            request = pb2.AppendEntriesArgs(
                leader_id=NODE_ID,
                leader_term=TERM,
                committed_value=COMMITED_VALUE,
                uncommitted_value=UNCOMMITED_VALUE
            )
            response = stub.AppendEntries(request)
            count.increment()
            if not response.heartbeat_result:
                print("Heartbeat Failed")
            channel.close()
        except grpc.RpcError:
            pass

    if not SUSPEND:
        SCHEDULER.enter(APPEND_ENTRIES_TIMEOUT, 1, append_entries)
        threads = []
        counter = Counter()
        for node_id, address in SERVERS_INFO.items():
            if node_id != NODE_ID:
                tt = threading.Thread(target=send_request, args=(address, counter))
                tt.daemon = True
                tt.start()
                threads.append(tt)
        for i in threads:
            i.join()
        if UNCOMMITED_VALUE != COMMITED_VALUE:
            if counter.get_value() + 1 > len(SERVERS_INFO) / 2:
                COMMITED_VALUE = UNCOMMITED_VALUE


def run_scheduler():
    SCHEDULER.run()


NODE_ID = None
SERVERS_INFO = {}
SUSPEND = None
TERM = 0
VOTED = False
VOTES_COUNT = 0
ELECTION_TIMEOUT = random.uniform(2.0, 4.0)
APPEND_ENTRIES_TIMEOUT = 0.4
UNCOMMITED_VALUE = 0
COMMITED_VALUE = 0
LEADER_ID = None
STATE = States.FOLLOWER
ELECTION_LOCK = threading.Lock()
SCHEDULER = sched.scheduler()
reset_timeout_election()
t = threading.Thread(target=run_scheduler)
t.start()


class Handler(pb2_grpc.RaftNodeServicer):
    def __init__(self):
        super().__init__()
        print(f"STATE: Follower | TERM: {TERM}")

    def AppendEntries(self, request, context):
        global TERM, LEADER_ID, COMMITED_VALUE, UNCOMMITED_VALUE, STATE, VOTED
        VOTED = False
        if SUSPEND:
            if LEADER_ID == NODE_ID:
                LEADER_ID = None
                STATE = States.FOLLOWER
            context.set_details("Server suspended")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return pb2.AppendEntriesResponse()
        if request.leader_id != NODE_ID:
            reset_timeout_election()
            if request.leader_term >= TERM:
                if TERM != request.leader_term:
                    print(f"STATE: Follower | TERM: {request.leader_term}")
                if UNCOMMITED_VALUE != request.uncommitted_value:
                    UNCOMMITED_VALUE = request.uncommitted_value
                if COMMITED_VALUE != request.committed_value:
                    COMMITED_VALUE = request.committed_value
                TERM = request.leader_term
                LEADER_ID = request.leader_id
                COMMITED_VALUE = request.committed_value
                UNCOMMITED_VALUE = request.uncommitted_value
            else:
                print("Something bad happened in TERM.")
            return pb2.AppendEntriesResponse(**{"term": TERM, "heartbeat_result": True})

    def RequestVote(self, request, context):
        global TERM, VOTED
        if SUSPEND:
            context.set_details("Server suspended")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return pb2.RequestVoteResponse()
        print(f'RPC[RequestVote] Invoked')
        print(f'\tArgs:')
        print(f'\t\tcandidate_id: {request.candidate_id}')
        print(f'\t\tcandidate_term: {request.candidate_term}')
        reset_timeout_election()
        if not VOTED:
            VOTED = True
            print("Voted for", request.candidate_id)
            return pb2.RequestVoteResponse(**{"term": TERM, "vote_result": True})
        print("Already voted")
        return pb2.RequestVoteResponse(**{"term": TERM, "vote_result": False})

    def GetLeader(self, request, context):
        if SUSPEND:
            context.set_details("Server suspended")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return pb2.GetLeaderResponse()
        print(f'RPC[GetLeader] Invoked')
        return pb2.GetLeaderResponse(**{"leader_id": LEADER_ID})

    def AddValue(self, request, context):
        global UNCOMMITED_VALUE
        if SUSPEND:
            context.set_details("Server suspended")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return pb2.AddValueResponse()
        print(f'RPC[AddValue] Invoked')
        print(f'\tArgs:')
        print(f'\t\tvalue_to_add: {request.value_to_add}')
        if LEADER_ID == NODE_ID:
            UNCOMMITED_VALUE += request.value_to_add
        else:
            try:
                print("Request redirection to LEADER...")
                channel = grpc.insecure_channel(SERVERS_INFO[LEADER_ID])
                stub = pb2_grpc.RaftNodeStub(channel)
                request = pb2.AddValueArgs(
                    value_to_add=request.value_to_add
                )
                stub.AddValue(request)
                channel.close()
            except grpc.RpcError:
                print("Redirect failed. Leader is offline.")
        return pb2.AddValueResponse()

    def GetValue(self, request, context):
        if SUSPEND:
            context.set_details("Server suspended")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return pb2.GetValueResponse()
        print(f'RPC[GetValue] Invoked')
        return pb2.GetValueResponse(**{"value": COMMITED_VALUE})

    def Suspend(self, request, context):
        global SUSPEND, t
        queue_copy = copy.copy(SCHEDULER.queue)
        for event in queue_copy:
            SCHEDULER.cancel(event)
        SUSPEND = True
        t.join()
        print(f'RPC[Suspend] Invoked')
        return pb2.SuspendResponse()

    def Resume(self, request, context):
        global SUSPEND, SCHEDULER, t
        SUSPEND = False
        SCHEDULER = sched.scheduler()
        reset_timeout_election()
        t = threading.Thread(target=run_scheduler)
        t.start()
        print(f'RPC[Resume] Invoked')
        print(f"STATE: Follower | TERM: {TERM}")
        reset_timeout_election()
        return pb2.ResumeResponse()


# ----------------------------- Do not change -----------------------------
def serve():
    print(f'NODE {NODE_ID} | {SERVERS_INFO[NODE_ID]}')
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_RaftNodeServicer_to_server(Handler(), server)
    server.add_insecure_port(SERVERS_INFO[NODE_ID])
    try:
        server.start()
        while True:
            server.wait_for_termination()
    except grpc.RpcError as e:
        print(f"Unexpected Error: {e}")
    except KeyboardInterrupt:
        server.stop(grace=10)
        print("Shutting Down...")


def init(node_id):
    global NODE_ID
    NODE_ID = node_id

    with open("config.conf") as f:
        global SERVERS_INFO
        lines = f.readlines()
        for line in lines:
            parts = line.split()
            id, address = parts[0], parts[1]
            SERVERS_INFO[int(id)] = str(address)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("node_id", type=int)
    args = parser.parse_args()

    init(args.node_id)

    serve()
# ----------------------------- Do not change -----------------------------
