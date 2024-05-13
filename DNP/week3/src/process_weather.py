import json
import threading
import time
from collections import deque
from datetime import datetime

import zmq

WEATHER_INPUT_PORT = 5555
FASHION_SOCKET_PORT = 5556

IP_ADD = '127.0.0.1'

latest_data = {"average-temp": 0.0, "average-hum": 0.0}
weather = deque(maxlen=17)
data_lock = threading.Lock()


def update_information(cur_time, data, state):
    with data_lock:
        while data and (datetime.fromtimestamp(
                cur_time) - datetime.strptime(data[0]["time"], "%Y-%m-%d %H:%M:%S")).total_seconds() > 30:
            del data[0]
        temperature_counter = 0
        humidity_counter = 0
        for i in data:
            temperature_counter += i["temperature"]
            humidity_counter += i["humidity"]
        latest_data["average-temp"] = temperature_counter / max(1, len(data))
        latest_data["average-hum"] = humidity_counter / max(1, len(data))

    if state == "Fashion":
        response = recommendation()
    elif state == "Weather":
        response = report()
    else:
        response = "Query Not Found"
    return response


def recommendation():
    with data_lock:
        avg_temp = latest_data['average-temp']
    if avg_temp < 10:
        result = "Today weather is cold. It's better to wear warm clothes."
    elif 10 <= avg_temp < 25:
        result = "Feel free to wear spring/autumn clothes."
    else:
        result = "Go for light clothes."
    with open("weather_data.log", "a") as file_writer:
        file_writer.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ": Response " + result + "\n")
    print(result)
    return result


def report():
    with data_lock:
        avg_temp = latest_data['average-temp']
        avg_hum = latest_data['average-hum']
    result = f"The last 30 sec average Temperature is {avg_temp:.2f} and Humidity {avg_hum:.2f}"
    print(result)
    with open("weather_data.log", "a") as file_writer:
        file_writer.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ": Response " + result + "\n")
    return result


def get_data():
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect(f"tcp://{IP_ADD}:{WEATHER_INPUT_PORT}")
    socket.setsockopt_string(zmq.SUBSCRIBE, "weather")
    try:
        while True:
            req_data = socket.recv_string()
            print("Received weather data:", req_data)
            with open("weather_data.log", "a") as file_writer:
                file_writer.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ": Received " + str(req_data) + "\n")
            with data_lock:
                weather.append(json.loads(req_data[8:]))
    except KeyboardInterrupt:
        socket.close()
        context.term()


def process_client():
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(f"tcp://{IP_ADD}:{FASHION_SOCKET_PORT}")
    try:
        while True:
            request = socket.recv_string()
            response = update_information(time.time(), weather, request)
            socket.send_string(response)
    except KeyboardInterrupt:
        socket.close()
        context.term()


def main():
    weather_thread = threading.Thread(target=get_data)
    weather_thread.daemon = True
    client_thread = threading.Thread(target=process_client)
    client_thread.daemon = True
    weather_thread.start()
    client_thread.start()

    try:
        weather_thread.join()
        client_thread.join()
    except KeyboardInterrupt:
        print("Terminating data_processor")
        exit()


if __name__ == "__main__":
    main()