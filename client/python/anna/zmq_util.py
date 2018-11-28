import zmq

def send_request(req_obj, resp_obj, send_sock, rcv_sock):
    req_string = req_obj.SerializeToString()

    send_sock.send(req_string)
    resp_obj.ParseFromString(rcv_sock.recv())

    while req_obj.request_id != resp_obj.response_id:
        resp_obj.Clear()
        resp_obj.ParseFromString(rcv_sock.recv())

class SocketCache():
    def __init__(self, context, zmq_type):
        self.context = context
        self._cache = {}
        self.zmq_type = zmq_type

    def get(self, addr):
        if addr not in self._cache:
            sock = self.context.socket(self.zmq_type)
            sock.connect(addr)

            return sock
        else:
            return self._cache[addr]
