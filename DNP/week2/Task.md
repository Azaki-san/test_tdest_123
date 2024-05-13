# Week 2 - Concurrency and Parallelism

> Distributed and Networking Programming - Spring 2024

## Task

Your tasks for this lab:

1. Write a multi-threaded TCP server that communicates with a given client.
2. Improve performance of the provided client using [`threading`](https://docs.python.org/3/library/threading.html) and [`multiprocessing`](https://docs.python.org/3/library/multiprocessing.html) (you are **not allowed** to use `concurrent.futures` module ).

## Server Implementation

The server should do the following:

1. Accept a new connection from a client.
2. Spawn a new thread to handle the connection.
3. Generate a list of `250000` random numbers ranging between `-999999999` and `999999999`.
4. Create a string containing the numbers separated by commas.
5. Send the list to the connected client, then close that connection.

> **Note**: the string shouldn't contain any whitespaces.

Additional requirements:

- The server should stay listening all the time and should not terminate unless a `KeyboardInterrupt` is received.
- The server should be able to handle multiple connections simultaneously.
- The server socket should be marked for address reuse so that the OS would immediately release the bound address after server termination. You can do so by calling the `setsockopt` on the server socket before binding the address as follows:
  ```python
  server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
  server_socket.bind((SERVER_IP, SERVER_PORT))
  ```

## Client Implementation

The client does the following:
 
1. Connect to the TCP server multiple times to download 100 unsorted lists, one by one.
2. Write each unsorted list into a `.txt` file in a directory called `unsorted_files` (creating the directory if it does not exist).
3. Sort each unsorted list and writes it into a `.txt` file in a directory called `sorted_files` (creating the directory if it does not exist).
4. Use [time](https://docs.python.org/3/library/time.html) module to calculate the total time taken for unsorted lists download and sorting files.

## Suggested Steps

1. Once you understand how the client code works, start by writing the server.
2. Once the server works fine, start to optimize the runtime of the client.
   - Use `threading` to spawn multiple threads that download the required lists concurrently.
   - Use `multiprocessing` module (for example, `multiprocessing.Pool()`) to spawn multiple processes (not more than your CPU core count) to sort the lists in parallel.
3. Check the time taken in each stage and verify that the client runtime was improved.


## Example Run

```bash
$ python3 NameSurname_server.py
Listening on 0.0.0.0:12345
Sent a file to ('127.0.0.1', 48168)
Sent a file to ('127.0.0.1', 48174)
...etc

# Before optimizing client
$ python3 client.py
Files download time: 44.49141001701355
Sorting time: 14.114730834960938

# After optimizing client
$ python3 NameSurname_client.py
Files download time: 29.626671075820923
Sorting time: 7.460468053817749
```

## Checklist and Grading Criteria

Submitted solution should satisfy the requirements listed below. Failing to satisfy an item will result in partial grade deduction or an assignment failure (depending on the severity). Assume that task costs 10 points and your grade cannot be negative.

- [ ] Two files with the source code are submitted to Moodle on time and named according to the format: (`NameSurname_client.py`, `NameSurname_server.py`). _(-10 points for non-compliance)_
- [ ] Code runs successfully under the [latest stable Python interpreter](https://www.python.org/downloads/). _(-10 points for non-compliance)_
- [ ] Code only imports dependencies from the [Python standard library](https://docs.python.org/3/library/index.html), i.e. no external dependencies allowed. _(-10 points for non-compliance)_
- [ ] Code does not import `concurrent.futures`. _(-10 points for non-compliance)_
- [ ] Server opens a thread for each connection. _(-10 point for non-compliance)_
- [ ] Client `Files download time` is improved after using `threading` properly. _(-5 point for non-compliance)_
- [ ] Client `Sorting time` is improved after using `multiprocessing` properly. _(-5 point for non-compliance)_
- [ ] Server and client use the same format for informative messages as in examples. _(-1 point for non-compliance)_
- [ ] Source code is readable and nicely formatted (e.g. according to [PEP8](https://peps.python.org/pep-0008/)). _(-1 point for non-compliance)_
- [ ] Source code is the author's original work. _(-10 points and case submission to DOE for non-compliance, both parties will be penalized for detected plagiarism)_

The list may not be fully complete. We preserve the right to deduct points for any other non-listed error.
