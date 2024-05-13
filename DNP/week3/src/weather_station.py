import zmq
import time
import json
import random
from datetime import datetime

IP_ADD = '127.0.0.1'
DATA_PROCESSES_INPUT_PORT = 5555


def main():
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind(f"tcp://{IP_ADD}:{DATA_PROCESSES_INPUT_PORT}")

    try:
        while True:
            time.sleep(2)
            weather_data_json = {"time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                 "temperature": round(random.uniform(5, 40), 1),
                                 "humidity": round(random.uniform(40, 100), 1)}
            co2_data_json = {"time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                             "co2": round(random.uniform(300, 500), 1)}
            socket.send_string(f"weather {json.dumps(weather_data_json)}")
            print(f"Weather is sent from WS1 {weather_data_json}")
            socket.send_string(f"co2 {json.dumps(co2_data_json)}")
            print(f"CO2 is sent from WS1 {co2_data_json}")
    except KeyboardInterrupt:
        socket.close()
        context.term()
        print("Terminating weather station")


if __name__ == '__main__':
    main()
