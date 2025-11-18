import threading
from nodes import GrandMaster, Switch, Slave

def build_topology():
    return

if __name__ == '__main__':
    build_topology()

    listen_port_map = {"GM": 20000, "A": 20001, "B": 20002, "C": 20003 , "G": 20004}

    gm = GrandMaster("GM", ["A"], listen_port_map["GM"], 6)
    
    a = Switch("A", "GM", ["B", "C"], listen_port_map["A"])

    b = Switch("B", "A", ["D", "E"], listen_port_map["B"])
    c = Switch("C", "A", ["F", "G"], listen_port_map["C"])

    d = Slave("D", "B")
    e = Slave("E", "B")

    f = Slave("F", "C")
    g = Switch("G", "C", ["H", "I", "J"], listen_port_map["G"])

    h = Slave("H", "G")
    i = Slave("I", "G")
    j = Slave("J", "G")

    nodes = [gm, a, b, c, d, e, f, g, h, i, j]

    # listen_port_map = {"GM": 20000, "A": 20001}

    # gm = GrandMaster("GM", ["A"], listen_port_map["GM"], 1)

    # a = Switch("A", "GM", ["B"], listen_port_map["A"])
    
    # b = Slave("B", "A")

    # nodes = [gm, a, b]

    # Create threads for each node.
    threads = []
    for node in nodes:
        threads.append(threading.Thread(target=node.start, args=(listen_port_map,)))

    # Start all threads
    for thread in threads:
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Do logging and show stuff.