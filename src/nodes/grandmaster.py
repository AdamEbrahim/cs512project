import select
import socket
import time
from time import sleep
import sys

class GrandMaster:
    def __init__(self, name: str, downstream_nodes, listen_port: int, num_slaves: int):
        self.name = name

        # Maps of socket to "interface number".
        self.downstream_sock_map = {}

        self.downstream_nodes = downstream_nodes

        self.num_slaves = num_slaves

        # Setup socket to listen on for downstream connections. Upon
        # a connection request, a socket will be created to mimic a
        # downstream interface.
        self.listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listen_sock.bind(('', listen_port))
        self.listen_sock.listen()

    # Entry point for threads.
    def start(self, listen_port_map: dict):
        # First listen for and accept any downstream connections.
        for i in range(len(self.downstream_nodes)):
            conn, addr = self.listen_sock.accept()
            self.downstream_sock_map[conn] = i

        print(f"Node {self.name} has connected with all downstream nodes!")

        # Receive preliminary message containing slaves from all downstream nodes.
        for sock, interface in self.downstream_sock_map.items():
            t, msg = self.recv(sock)

        # Short delay before starting
        sleep(1)

        self.run_ptp()

    
    def run_ptp(self):
        downstream_socks = [sock for sock, interface in self.downstream_sock_map.items()]
        while True:
            self.send_sync(downstream_socks)
            self.handle_delay_req(downstream_socks)
            sleep(10)
    
    def send_sync(self, downstream_socks):
        for sock in downstream_socks:
            t1 = self.send(sock, "sync\n")

            # Add some delay so sync and follow up dont get grouped together.
            sleep(0.25)

            # Send follow up, message will be in format "follow_up <T1> <correction>\n"
            self.send(sock, "follow_up " + str(t1) + " 0\n")

    def handle_delay_req(self, downstream_socks):
        # Collect all delay requests to get more accurate timestamping.
        num_read = 0
        delay_reqs = []

        while num_read < self.num_slaves:
            readable, writeable, exceptional = select.select(downstream_socks, [], [])
            for s in readable:
                t4, msg = self.recv(s)
                # Message will be in format "delay_req <slave_name> <correction>\n"
                msg = msg.rstrip("\n").split("\n")
                delay_reqs.extend([(m, t4) for m in msg])

                num_read = num_read + len(msg)

        self.send_delay_resp(downstream_socks, delay_reqs)

    def send_delay_resp(self, downstream_socks, delay_reqs):
        for sock in downstream_socks:
            for delay_req in delay_reqs:
                msg = delay_req[0].split(" ")
                # Message will be in format "delay_resp <slave_name> <t4> <correction>\n"
                self.send(sock, "delay_resp " + msg[1] + " " + str(delay_req[1]) + " " + msg[2] + "\n")


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
