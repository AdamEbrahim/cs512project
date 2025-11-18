import select
import socket
import time
import sys
import random
import heapq

# Switches are implemented as PTP transparent clocks.

class Switch:
    def __init__(self, name: str, upstream_node: str, downstream_nodes, listen_port: int):
        # Each node's interfaces can be slaves if they receive from upstream nodes,
        # and masters if they send to downstream nodes.

        self.name = name

        # Maps of socket to "interface number".
        self.upstream_sock_map = {}
        self.downstream_sock_map = {}

        self.forwarding_map = {}

        # Priority queue of pending forwards:
        # entries are (due_time, seq, out_sock, msg, t_ingress,
        #              needs_correction, record_sync_residence, apply_sync_correction)
        self._pending_forwards = []
        self._forward_seq = 0
        self._sync_correction_buffer = {}
        self._next_send_time = {}

        self.upstream_node = upstream_node
        self.downstream_nodes = downstream_nodes

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

        # Get message from downstream nodes containing slaves on that interface.
        for sock, interface in self.downstream_sock_map.items():
            t, msg = self.recv(sock)
            msg = msg.split(" ")
            for m in msg:
                self.forwarding_map[m] = sock

        # Now connect to upstream node by looking port up in listen_port_map.
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(('127.0.0.1', listen_port_map[self.upstream_node]))
        except Exception as e:
            print(f"Node {self.name} failed to connect with upstream node.")
            return
        
        self.upstream_sock_map[s] = len(self.downstream_nodes)
        upstream_sock = list(self.upstream_sock_map.keys())[0]

        # Send message to upstream node containing all downstream slaves of this switch.
        msg = " ".join(list(self.forwarding_map.keys()))
        self.send(upstream_sock, msg)

        self.run_ptp()


    def run_ptp(self):
        # Single upstream socket.
        upstream_sock = list(self.upstream_sock_map.keys())[0]
        # All downstream sockets.
        downstream_socks = list(self.downstream_sock_map.keys())

        # All sockets we care about for select.
        all_socks = [upstream_sock] + downstream_socks

        while True:
            # Determine timeout based on earliest pending forward (if any).
            timeout = None
            if self._pending_forwards:
                now = time.time()
                next_due = self._pending_forwards[0][0]
                timeout = max(0, next_due - now)

            readable, writeable, exceptional = select.select(all_socks, [], [], timeout)

            # First, handle any newly received messages.
            for s in readable:
                t_ingress, raw_msg = self.recv(s)

                # Support multiple messages in one recv, separated by '\n'.
                for msg in raw_msg.split("\n"):
                    msg = msg.strip()
                    if not msg:
                        continue

                    parts = msg.split(" ")
                    msg_type = parts[0]

                    # Message from upstream grandmaster side.
                    if s is upstream_sock:
                        # sync and follow_up are broadcast downstream.
                        if msg_type == "sync":
                            for ds in downstream_socks:
                                self.forward(
                                    ds,
                                    msg,
                                    t_ingress,
                                    needs_correction=False,
                                    record_sync_residence=True,
                                )
                        elif msg_type == "follow_up":
                            for ds in downstream_socks:
                                self.forward(
                                    ds,
                                    msg,
                                    t_ingress,
                                    needs_correction=False,
                                    apply_sync_correction=True,
                                )
                        # delay_resp should go only to the correct slave subtree.
                        elif msg_type == "delay_resp" and len(parts) >= 2:
                            slave_name = parts[1]
                            out_sock = self.forwarding_map.get(slave_name)
                            if out_sock is not None:
                                # delay_resp only carries the correction accumulated on the
                                # delay_req path. Forward it without adding residence time.
                                self.forward(out_sock, msg, t_ingress, needs_correction=False)
                    # Message from downstream side.
                    else:
                        # delay_req always goes upstream.
                        if msg_type == "delay_req":
                            self.forward(upstream_sock, msg, t_ingress, needs_correction=True)

            # Then, send any messages whose scheduled egress time has arrived.
            now = time.time()
            while self._pending_forwards and self._pending_forwards[0][0] <= now:
                (
                    _,
                    _,
                    out_sock,
                    msg,
                    t_ingress,
                    needs_correction,
                    record_sync_residence,
                    apply_sync_correction,
                ) = heapq.heappop(self._pending_forwards)
                self._send_with_metadata(
                    out_sock,
                    msg,
                    t_ingress,
                    needs_correction,
                    record_sync_residence,
                    apply_sync_correction,
                )


    def forward(
        self,
        out_sock,
        msg,
        t_ingress,
        needs_correction: bool,
        record_sync_residence: bool = False,
        apply_sync_correction: bool = False,
    ):
        """
        Schedule a message to be forwarded after a random residence time.
        Residence time is accounted for when the message is actually sent.
        """
        # Sample an artificial residence time (small to avoid starving downstream nodes).
        residence_delay = random.uniform(0.5, 1.0)
        ready_time = t_ingress + residence_delay

        last_time = self._next_send_time.get(out_sock, 0.0)
        due_time = max(ready_time, last_time)
        # ensure strict ordering by nudging slightly forward
        due_time += 1e-6
        self._next_send_time[out_sock] = due_time

        heapq.heappush(
            self._pending_forwards,
            (
                due_time,
                self._forward_seq,
                out_sock,
                msg,
                t_ingress,
                needs_correction,
                record_sync_residence,
                apply_sync_correction,
            ),
        )
        self._forward_seq += 1


    def _send_with_metadata(
        self,
        out_sock,
        msg,
        t_ingress,
        needs_correction: bool,
        record_sync_residence: bool,
        apply_sync_correction: bool,
    ):
        """
        Actually send the message out, updating the correction field if requested.
        """
        parts = msg.split(" ")
        msg_type = parts[0] if parts else ""

        t_egress = time.time()
        residence = t_egress - t_ingress

        if record_sync_residence:
            current = self._sync_correction_buffer.get(out_sock, 0.0)
            self._sync_correction_buffer[out_sock] = current + residence

        if needs_correction and len(parts) >= 3:
            try:
                old_corr = float(parts[-1])
            except ValueError:
                old_corr = 0.0

            new_corr = old_corr + residence
            parts[-1] = str(new_corr)
            fwd_msg = " ".join(parts) + "\n"
        elif apply_sync_correction and len(parts) >= 3:
            extra_corr = self._sync_correction_buffer.pop(out_sock, 0.0)
            try:
                base_corr = float(parts[-1])
            except ValueError:
                base_corr = 0.0

            parts[-1] = str(base_corr + extra_corr)
            fwd_msg = " ".join(parts) + "\n"
        else:
            # Messages that don't need correction added are forwarded as-is.
            fwd_msg = msg + "\n"

        self.send(out_sock, fwd_msg)


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
        
