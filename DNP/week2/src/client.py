import os
import socket
import time

SERVER_URL = '127.0.0.1:12345'
CLIENT_BUFFER = 1024
UNSORTED_FILES_COUNT = 100


def create_directories():
    if not os.path.exists('unsorted_files'):
        os.mkdir('unsorted_files')

    if not os.path.exists('sorted_files'):
        os.mkdir('sorted_files')


def download_unsorted_files():
    for i in range(UNSORTED_FILES_COUNT):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            ip, port = SERVER_URL.split(':')
            s.connect((ip, int(port)))
            file = b''
            while True:
                packet = s.recv(CLIENT_BUFFER)
                if not packet:
                    break
                file += packet
            with open(f'unsorted_files/{i}.txt', 'wb') as f:
                f.write(file)


def create_sorted_file():
    for unsorted_id in range(UNSORTED_FILES_COUNT):
        with open(f"unsorted_files/{unsorted_id}.txt", "r") as unsorted_file:
            unsorted_list = [int(number)
                             for number in unsorted_file.read().split(',')]

            sorted_list = sorted(unsorted_list)

            with open(f"sorted_files/{unsorted_id}.txt", "w") as sorted_file:
                sorted_file.write(','.join(map(str, sorted_list)))

    return


if __name__ == '__main__':
    create_directories()

    tdownload0 = time.monotonic()
    download_unsorted_files()
    tdownload = time.monotonic() - tdownload0
    print(f"Files download time: {tdownload}")

    tsort0 = time.monotonic()
    create_sorted_file()
    tsort = time.monotonic() - tsort0
    print(f"Sorting time: {tsort}")
