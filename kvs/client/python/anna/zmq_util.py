import zmq

def send_request(req_obj, send_sock):
    req_string = req_obj.SerializeToString()

    send_sock.send(req_string)

def recv_response(req_ids, rcv_sock, resp_class):
    responses = []

    while len(responses) < len(req_ids):
        resp_obj = resp_class()
        resp = rcv_sock.recv()
        resp_obj.ParseFromString(resp)

        while resp_obj.response_id not in req_ids:
            resp_obj.Clear()
            resp_obj.ParseFromString(rcv_sock.recv())

        responses.append(resp_obj)

    return responses

class SocketCache():
    def __init__(self, context, zmq_type):
        self.context = context
        self._cache = {}
        self.zmq_type = zmq_type

    def get(self, addr):
        if addr not in self._cache:
            sock = self.context.socket(self.zmq_type)
            sock.connect(addr)

            self._cache[addr] = sock

            return sock
        else:
            return self._cache[addr]
