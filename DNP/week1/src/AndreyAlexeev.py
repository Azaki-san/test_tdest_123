import argparse
import socket
from math import ceil

HOST = "0.0.0.0"
PORT = None
MNSC = None
MSS = 20480

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # Reading arguments from terminal
    parser.add_argument("port", type=int)
    parser.add_argument("max_number_simultaneously_connect", type=int)
    args = parser.parse_args()
    PORT = args.port
    MNSC = args.max_number_simultaneously_connect
    # My implementation stores connected client in dictionary
    connected_clients = {}

    with socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 30000)
        s.bind((HOST, PORT))
        print(f"{(HOST, PORT)}:    Listening...")
        while True:
            try:
                data, client_address = s.recvfrom(MSS)
                message_type = chr(data[0])
                splitted_data = data.split(b'|')
                seqno = int(splitted_data[1].decode())
                if message_type == "s":
                    print(f"{client_address}: {data.decode()}")
                    # We need to check if we have place for a new client or this client is already connected
                    # (because of delay)
                    if len(connected_clients) >= MNSC and client_address not in connected_clients:
                        s.sendto(f'n|{(seqno + 1) % 2}'.encode(), client_address)
                        continue
                    filename = splitted_data[2].decode()
                    size = int(splitted_data[3].decode())
                    # Stores chunk number, sequence number, last chunk number and filename
                    connected_clients[client_address] = {"chunk_no": 1, "seq_no": seqno,
                                                         "last_chunk_no": ceil(size / MSS), "filename": filename}
                    # Notification about already existing file
                    try:
                        open(filename, "rb")
                        print(f"File {filename} already exists. Overwriting...")
                    except Exception as e:
                        pass
                    with open(filename, 'wb') as f:
                        s.sendto(f'a|{(seqno + 1) % 2}'.encode(), client_address)
                elif message_type == "d":
                    # Check if this address connected to server
                    if client_address in connected_clients:
                        filename = connected_clients[client_address]["filename"]
                        current_chunk = connected_clients[client_address]["chunk_no"]
                        last_chunk = connected_clients[client_address]["last_chunk_no"]
                        print(f"{client_address}: {data[0:4].decode()}chunk{current_chunk}")
                        chunk = data[4:]
                        s.sendto(f'a|{(seqno + 1) % 2}'.encode(), client_address)
                        # Check seq number for correctness
                        if seqno != connected_clients[client_address]["seq_no"]:
                            with open(filename, 'ab') as f:
                                f.write(chunk)
                            # Check if we received whole file
                            if current_chunk == last_chunk:
                                print(f"{(HOST, PORT)}:    Received {filename}.")
                                del connected_clients[client_address]
                            else:
                                connected_clients[client_address]["chunk_no"] += 1
                                connected_clients[client_address]["seq_no"] = (connected_clients[client_address][
                                                                                   "seq_no"] + 1) % 2
                    else:
                        s.sendto(f'n|{(seqno + 1) % 2}'.encode(), client_address)
                else:
                    print(message_type)
            except KeyboardInterrupt:
                print(f"{(HOST, PORT)}: Shutting down...")
                exit(0)
