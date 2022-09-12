#!/usr/bin/env python3

import socket


def connect_to_server():
    server_address = "/tmp/cluster-client.sock"
    socket_family = socket.AF_UNIX
    socket_type = socket.SOCK_STREAM

    sock = socket.socket(socket_family, socket_type)
    sock.connect(server_address)
    return sock

    sock.sendall("hello server".encode())
    data = sock.recv(1024)
    print(f"recv data from server '{server_address}': {data.decode()}")
    sock.close()


def main():
    sock = connect_to_server()
    print("Type 'exit' or 'quit' to quit ")
    while True:
        print("> ", end='')
        raw = input()
        cmd = raw.strip()
        if cmd == '':
            continue


if cmd == "exit" or cmd == "quit":
    break
# print('send: [%s]' % cmd)
sock.sendall(cmd.encode())
while True:
    rsp_buf = sock.recv(1024)
    rsp_str = rsp_buf.decode()
    print(rsp_str, end='')
    if rsp_str.find("command finished") >= 0:
        break

if __name__ == "__main__":
    main()
