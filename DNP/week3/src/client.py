import zmq

IP_ADD = '127.0.0.1'
FASHION_OUTPUT_PORT = 5556


def main():
    context = zmq.Context()
    fashion_output = context.socket(zmq.REQ)
    fashion_output.connect(f"tcp://{IP_ADD}:{FASHION_OUTPUT_PORT}")

    try:
        while True:

            query = input("Enter your query (Fashion/Weather): ")

            if query == "Fashion" or query == "Weather":
                print("Sending query:", query)
                fashion_output.send_string(query)

                # Receive response from the service
                recommendation = fashion_output.recv_string()
                print("Received response:", recommendation)
            else:
                print("Invalid query. Please enter 'Fashion' or 'Weather'.")

    except KeyboardInterrupt:
        print("Terminating client")


if __name__ == "__main__":
    main()
