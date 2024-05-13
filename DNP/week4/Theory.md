# Week 4 Theory

## Serialization and Deserialization

_Serialization_ is the process of converting a programming construct (e.g., a data structure or an object) into a format that can be stored, transmitted over the network, and reconstructed later. Popular serialization formats include XML, JSON and Protocol Buffers.

_Deserialization_ is the opposite: process of converting the serialized format back into the original form.

## RPC (Remote Procedure Call)

RPC allows executing remote code as if it's being stored and executed locally.
This is typically needed for communication between microservices or distributed systems nodes.

The caller (client) calls a stub function and the executor (server) runs the logic.

- Stub function essentially serializes the request (including function name and parameters) and sends it over the network waiting for the response.
- The executor function receives the request, deserializes it, runs the application logic, serializes the result and sends it back to the caller.

### RPC Advantages

- **Transparency**: RPC hides the details of the network communication from the developer.
- **Interoperability**: RPC allows communication between different programming languages and platforms using a common interface.
- **Scalability**: RPC allows scaling applications by distributing the load across multiple servers.

> Popular implementations of this concept: Sun RPC, XML-RPC, JSON-RPC, and gRPC.

## gRPC

gRPC is an open-source high performance RPC framework developed by Google.

gRPC uses Protocol Buffers as the default data serialization format.

A single `.proto` file can be used to generate implementation (bindings) for different programming languages. These bindings allow storing the data in a language-neutral format.

gRPC provides utilities for error handling, authentication, bidirectional streaming with flow control, blocking/nonblocking bindings, cancellation, and timeouts.

### gRPC-related Terminology

- `Server` — server implements the methods declared by the service and runs a gRPC server to handle client calls.
- `.proto` — a Protocol Buffers (protobuf) file format that defines the service interface and the structure of the payload messages.
- `Service` — collection of methods defined in `.proto` file that can be called remotely along with their parameters and return types.
- `Message` — ProtoBuf data structures used to send and receive data in RPCs.
- `Servicer` — the class that implements the service interface defined in the `Service`.

### Error handling

Each call in gRPC returns status code and metadata, which can be used to return errors with any additional details.

Here is a basic example how to response with an error on server and handle error on client:

`server.py`:

```python
import grpc
import example_pb2
import example_pb2_grpc

class ExampleApi(example_pb2_grpc.ExampleApiServicer)
    ...

    def GetUser(self, request, context):
        user = self.db.get(request.user_id)

        if not user:
            # Mark request as failed.
            ctx.set_code(grpc.StatusCode.NOT_FOUND)
            ctx.set_details(f"User with ID {request.user_id} is not found.")
            return example_pb2.User()

        return example_pb2.User(id=user.id, name=user.name)

...
```

`client.py`:

```python
import grpc
import example_pb2
import example_pb2_grpc

with grpc.insecure_channel(server_address) as channel:
    stub = example_pb2_grpc.ExampleApiStub(channel)
    try:
        user = stub.GetUser(example_pb2.GetUserRequest(user_id=123))
        print("User info:", user)
    except grpc.RpcError as err:
        print(f"gRPC error (code={err.code()}): {err.details()}")
```

For more details and advanced error handling you can read the official ["Error handling" guide](https://grpc.io/docs/guides/error/).
