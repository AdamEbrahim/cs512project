import select
import socket
import time
from time import sleep
import sys
import random

class Slave:
    def __init__(self, name: str, upstream_node: str):
        self.name = name

        # Maps of socket to "interface number".
        self.upstream_sock_map = {}

        self.upstream_node = upstream_node
        self.recv_buffers = {}

        self.running_drift_errors = []

    # Entry point for threads.
    def start(self, listen_port_map: dict):
        # Now connect to upstream node by looking port up in listen_port_map.
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(('127.0.0.1', listen_port_map[self.upstream_node]))
        except Exception as e:
            print(f"Node {self.name} failed to connect with upstream node.")
            return
        
        self.upstream_sock_map[s] = 0
        upstream_sock = list(self.upstream_sock_map.keys())[0]

        # Send slave name so upstream nodes can build forwarding map.
        self.send(upstream_sock, self.name)

        self.run_ptp()

    def run_ptp(self):
        upstream_sock = list(self.upstream_sock_map.keys())[0]
        iteration = 1
        while True:
            # Generate a random clock drift between -1 and 1 second for this iteration
            drift = random.uniform(-1.0, 1.0)

            # Wait for sync message.
            t2, msg = self.recv_message(upstream_sock)
            if msg != "sync":
                print(f"Error: Node {self.name} didn't receive sync when expected.")
                continue

            # Add the clock drift to t2
            t2 = t2 + drift

            t1, sync_correction = self.handle_follow_up(upstream_sock)
            if t1 == -1:
                print(f"Error: Node {self.name} didn't receive follow_up when expected.")
                continue

            # Send delay request.
            t3 = self.send_delay_req(upstream_sock)

            # Add the clock drift to t3.
            t3 = t3 + drift

            # Get delay response.
            t4, delay_correction = self.handle_delay_resp(upstream_sock)
            if t4 == -1:
                print(f"Error: Node {self.name} didn't receive delay_resp when expected.")
                continue

            # print(f"Node {self.name}: computed sync_corr - {sync_correction}   computed delay_corr - {delay_correction}")

            delay = ((t2 - t1 - sync_correction) + (t4 - t3 - delay_correction)) / 2
            offset = ((t2 - t1 - sync_correction) - (t4 - t3 - delay_correction)) / 2

            print(f"Node {self.name}: true drift - {drift}   computed drift - {offset}   computed delay - {delay}")

            self.running_drift_errors.append(abs(drift - offset))

            avg = sum(self.running_drift_errors) / len(self.running_drift_errors)

            print(f"Node {self.name} average error in drift calculation by iteration {iteration}: {avg:f}")

            iteration = iteration + 1

    
    def handle_follow_up(self, upstream_sock):
        # Message will be in format "follow_up <T1> <correction>\n"
        t, msg = self.recv_message(upstream_sock)
        msg = msg.split(" ")

        if msg[0] != "follow_up":
            return (-1, 0)

        return (float(msg[1]), float(msg[2]))

    def send_delay_req(self, upstream_sock):
        # Message will be in format "delay_req <slave_name> <correction>\n"
        t3 = self.send(upstream_sock, "delay_req " + self.name + " 0\n")
        return t3

    def handle_delay_resp(self, upstream_sock):
        # Message will be in format "delay_resp <slave_name> <t4> <correction>\n"
        t4, msg = self.recv_message(upstream_sock)
        msg = msg.split(" ")

        if msg[0] != "delay_resp":
            return (-1, 0)

        return (float(msg[2]), float(msg[3]))


    def recv(self, sock):
        try:
            msg = sock.recv(4096)
            t = time.time()
            return (t, msg.decode("utf8"))
        except Exception as e:
            print("Error during receive: " + str(e))
            sock.close()
            sys.exit(1)

    def send(self, sock, data):
        try:
            sock.sendall(data.encode('utf8'))
            t = time.time()
            return t
        except Exception as e:
            print("Error during send: " + str(e))
            sock.close()
            sys.exit(1)

    def recv_message(self, sock):
        buffer = self.recv_buffers.get(sock, "")

        while "\n" not in buffer:
            chunk = sock.recv(4096)
            if not chunk:
                print(f"Error during receive_message: connection closed for node {self.name}")
                sock.close()
                sys.exit(1)
            buffer += chunk.decode("utf8")

        line, _, remainder = buffer.partition("\n")
        self.recv_buffers[sock] = remainder
        return (time.time(), line.strip())

