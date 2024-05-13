# Week 5 - Distributed Hash Tables

> Distributed and Networking Programming - Spring 2024

## Overview

Your task for this lab is to implement a simplified version of a Chord algorithm using gRPC.

### System Architecture

Chord operates over a structured P2P overlay network in which nodes (peers) are organized in a ring.

- Each node has an integer identifier: $n \in [0, 2^{M})$, where $M$ is the identifier length in bits.
- Total number of nodes: $N \leq 2^M$.
- Each node should stay up-to-date about the current topology of the ring.
- Nodes communicate over the network using RPC (gRPC is our implementation).

### Node and .proto

- Each node is responsible for storing values for keys in the range `(predecessor_id, node_id]` except the first node which stores values for keys in the range `(predecessor_id, 2**M) U [0, node_id]`.

- Each node maintains a **finger table** (i.e., a list of identifiers for some other nodes)
  - A finger table contains $M$ entries (repetitions are allowed)
  - The value of the $i^{th}$ element in the finger table for node $n$ is defined as follows:

    $$
    FT_{n}[i] = successor((n + 2^{i}) \mod 2^M), i \in \{0..M-1\}
    $$

  - Each node implements a `find_successor` function that returns the identifier of the next online node in the ring (clockwise direction).
  - Each node implements a `find_predecessor` function that returns the identifier of the previous online node in the ring (clockwise direction).
  - Finger tables are used by the Chord algorithm to achieve a logarithmic search time. They are the reason behind Chord scalability.
  - In real world, (`find_successor` and `find_predecessor`) are recursively implemented over periodic RPC calls to handle cases in which nodes join or leave the network. In our case, the ring is known in advance so you can find successors and predecessors from each node's **finger table**. Consult [this paper](https://www.cs.swarthmore.edu/~kwebb/cs91/f14/papers/Stoica_Chord.pdf) for more details.
  - For the node to answer a request, it may contact other nodes.
  - Lookup procedure:
    Chord nodes follow these steps to find the node holding (or to hold) the value `V` for a given key `K`:
    - Suppose we asked node $N_{i}$ to locate the node $N_{f}$ holding `K`, it would check for the following:
      - If $K \in (predecessor(N_{i}), N_{i}]$, then $N_{i}$ is the successor of `K` and thus it holds it ($N_{i} = N_{f}$).
      - If $K \in (N_{i}, successor(N_{i})]$, then $N_{f}$ is the successor of $N_{i}$.
      - Else, $N_{i}$ would forward the request to node $N_{j}$ with index `j` from $N_{i}$'s finger table `FT` satisfying:

       $$
       FT[j] \leq K \lt FT[j + 1]
       $$

      - Then, $N_{j}$ would repeat the process.
- A node skeleton is provided, it is up to you to conform to it or not. But you are obliged to use the provided `zlib.adler32` function to compute IDs for keys. Outputs for a given chord ring are deterministic.

- Use Protocol Buffers (`chord.proto`) for message serialization. The `Chord` service should support four RPCs:
  - `SaveData`: Accepts the key-value to be stored, should return the node id in which the value was stored.
  - `RemoveData`: Accepts the key to be removed.
  - `FindData`: Accepts the key to be retrieved.
  - `GetFingerTable`: Returns the finger table of the current node.

### Client

- The provided client uses gRPC to:
  - Connect to a node over address and port via a serial id, the node should be ready and listening.
  - Ask the node to insert an entry into the chord.
  - Execute some operations.
    - Find operation asks a certain node to locate the value for a certain key.
    - Remove operation forwards a call to remove the data with a specific key.

- A client skeleton is provided, **it is up to you to conform to it or not**. But make sure that it functions as per the logs in the [Example Run](#example-run).

## Task

- Implement `chord.proto` with service `Chord`. Generate artifacts with:

  ```bash
  python3 -m grpc_tools.protoc \
    --proto_path=. \
    --python_out=. \
    --pyi_out=. \
    --grpc_python_out=. \
    ./chord.proto
  ```

- Implement the node as explained above. The boilerplate is given in `node.py`.
  - Parse one integer argument (`node_id`).
  - Populate its finger table, successor, and predecessor nodes.
  - Keep alive to receive and forward calls.

- Implement `client.py`, it shall take inputs as specified in the logs.
  - Connect to a node by id, execute queries on it.

- Submit a single .zip archive with the following files:
  - `chord.proto`
  - `client.py`
  - `node.py`

## Example Run

- We have prepared an example ring of 6 nodes, nodes are reachable from each other by their serial (not chord) ids which map to local ports.

```python
CHORD = [2, 16, 24, 25, 26, 31]
```

- You can use [Chordgen](https://msindwan.github.io/chordgen/) to visualize test cases and verify finger tables and lookup order.

- Note that your code will be tested on different rings, a correct implementation should always work.

- The output shows how queries are routed through the ring, the routing order is deterministic.

- In this example, we show how two key-value pairs are stored on the chord.
  - A key **Kazan** with ID of 22.
  - A key **chord_week** with ID of 28.

- Open two terminals, run the client and the bash script launching 6 nodes.
- Upon start, `run_all_nodes` will run the chord:

```plain
$ python3 client.py
> 

$ sh run_all_nodes.sh
Node 31	 finger table [2, 2, 16, 16, 16]
Node 2	 finger table [16, 16, 16, 16, 24]
Node 16	 finger table [24, 24, 24, 24, 2]
Node 24	 finger table [25, 26, 31, 2, 16]
Node 25	 finger table [26, 31, 31, 2, 16]
Node 26	 finger table [31, 31, 31, 2, 16]
```

An execution of a save query, logs appended for the nodes thorough which the request was routed:
Client save(Kazan, ID = 22) -> Node 2 -> Node 16 -> Node 24.

```plain
$ python3 client.py
> connect 0
Connected To Node 0
> save Kazan city
Success, Kazan was saved in node 24

$ sh run_all_nodes.sh
...
Node 2 says: Save from 2 to 16
Node 16 says: Save from 16 to 24
Node 24 says: Saved Kazan
```

A key lifecycle:
Client save(chord_week, ID = 28) -> Node 16 -> Node 24 -> Node 26 -> Node 31.

```plain
python3 client.py
> connect 1
Connected To Node 1
> save chord_week 5
Success, chord_week was saved in node 31
> find chord_week
Success, chord_week was found in node 31 with data 5
> remove chord_week
Success, chord_week was removed from node 31
> find chord_week
Failure, data was not found
> 

$ sh run_all_nodes.sh
...
Node 16 says: Save from 16 to 24
Node 24 says: Save from 24 to 26
Node 26 says: Save from 26 to 31
Node 31 says: Saved chord_week
Node 16 says: Find from 16 to 24
Node 24 says: Find from 24 to 26
Node 26 says: Find from 26 to 31
Node 31 says: Found chord_week
Node 16 says: Remove from 16 to 24
Node 24 says: Remove from 24 to 26
Node 26 says: Remove from 26 to 31
Node 31 says: Removed chord_week
Node 16 says: Find from 16 to 24
Node 24 says: Find from 24 to 26
Node 26 says: Find from 26 to 31

```

Requesting a node's finger table:

```bash
$ python3 client py
> connect 2
Connected To Node 2
> get_finger_table
[25, 26, 31, 2, 16]
> 
```

## Suggested Steps

1. Start by implementing `chord.proto`, messages in `client.py` give hints on how the RPCs should look like, but you can introduce different schemas if you want.
2. Start implementing `node.py` by deriving its finger table and chord ID. Then, implement service handlers.
3. Align `client.py` to match your schemas, run `run_all_nodes.sh` and check logs.

## Checklist

General requirements:

- [ ] A single file `NameSurname.zip` is submitted to Moodle with the following content:

    ```plain
    ├── chord.proto
    └── client.py
    └── node.py
  ```

- [ ] Code only imports dependencies from the [Python standard library](https://docs.python.org/3/library/index.html), `grpc` package, or generated artifacts. _(-1 points for non-compliance)_
- [ ] Source code is readable and nicely formatted (e.g. according to [PEP8](https://peps.python.org/pep-0008/)). _(-1 point for non-compliance)_
- [ ] Source code is the author's original work. _(0 points for the assignment and case submission to DOE for non-compliance, both parties will be penalized for detected plagiarism)_

Implementation requirements:

- [ ] `chord.proto` does not contain syntax errors and `protoc` successfully generates code for this proto file without warnings and errors.
- [ ] Both client's and node's code runs successfully with the `.py` files generated from the `chord.proto` file.  
- [ ] Node successfully starts and listens on the given port until interrupted by `KeyboardInterrupt`. _(-1 points for non-compliance)_
- [ ] Node handles all requests (save, remove, find, get_finger_table) as described._(-2.5 points for each malformed request)_
- [ ] Node logs all requests as in the [Example Run](#example-run).
- [ ] Logging shows the node id and finger table for each node upon launch. _(-1 points for non-compliance)_
- [ ] Logging shows which node functions were called over RPC and how the request was routed (see example output above). _(-1 points for non-compliance)_
- [ ] The system provides the correct output for **the given example** ring and lookup queries. _(-5 points for non-compliance)_
- [ ] The system provides the correct output for **a different** ring and lookup queries. _(-5 points for non-compliance)_

## Additional Notes

Chord is quite a complex protocol, you can find the original implementation [here](https://github.com/sit/dht). The following simplifications were made to adjust the complexity of the task:

- Each node is initialized with knowledge about other nodes in the ring, removing the need to implement notification procedures.
- The system topology is fixed, removing the need to implement procedures for stabilization, node joining/exiting and finger table updates.
- A client is provided to test the system. In real-world scenarios, that client is typically a node as well.

In real-world implementations, the Chord and its finger tables should update dynamically as nodes enter and exit the system. Periodical stabilization procedures are also used to ensure that nodes stay up-to-date with the current topology of the ring.
