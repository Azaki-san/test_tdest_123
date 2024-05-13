from datetime import datetime
import json
import time

import zmq

WEATHER_INPUT_PORT = 5555
IP_ADD = '127.0.0.1'


def main():
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect(f"tcp://{IP_ADD}:{WEATHER_INPUT_PORT}")
    socket.setsockopt_string(zmq.SUBSCRIBE, "co2")

    try:
        while True:
            message = socket.recv_string()
            print(f"Received weather data: {message}")
            decoded_msg = json.loads(message[4:])
            with open("co2_data.log", "a") as file_writer:
                file_writer.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ": Received " + message + "\n")
            if decoded_msg["co2"] > 400:
                print("Danger zone! Please do not leave the home")
    except KeyboardInterrupt:
        print("Terminating data_processor")
        socket.close()
        context.term()


if __name__ == "__main__":
    main()