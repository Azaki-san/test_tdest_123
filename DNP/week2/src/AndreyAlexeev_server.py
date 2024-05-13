import socket
import threading
import random

SERVER_IP = '127.0.0.1'
SERVER_PORT = 12345


def handle_client(client_socket, client_address):
    random_numbers = [random.randint(-999999999, 999999999) for _ in range(250000)]
    numbers_string = ','.join(map(str, random_numbers))
    client_socket.send(numbers_string.encode())
    print(f"Sent a file to {client_address}")
    client_socket.close()


def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((SERVER_IP, SERVER_PORT))
    server_socket.listen()

    print(f"Listening on {SERVER_IP}:{SERVER_PORT}")

    try:
        while True:
            client_socket, client_address = server_socket.accept()
            client_thread = threading.Thread(target=handle_client, args=(client_socket, client_address,))
            client_thread.start()
    except KeyboardInterrupt:
        print("Server shutting down.")
        server_socket.close()


if __name__ == "__main__":
    main()
