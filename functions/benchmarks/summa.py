import cloudpickle as cp
import logging
import sys
import time

from include.functions_pb2 import *
from include.serializer import *

def run(flconn, kvs, num_requests, sckt):
    ### DEFINE AND REGISTER FUNCTIONS ###

    def summa(fluent, lblock, rblock, rid, cid, numrows, numcols):
        import cloudpickle as cp
        from anna.lattices import LWWPairLattice

        bsize = lblock.shape[0]
        result = np.zeros((bsize, bsize))

        myid = fluent.getid()
        key = '(%d, %d)' %  (rid, cid)
        fluent.put(key, LWWPairLattice(0, cp.dumps(myid)))

        proc_locs = {}
        for i in range(numrows):
            for j in range(numcols):
                key = '(%d, %d)' % (i, j)

                loc = fluent.get(key)
                while loc is None:
                    loc = fluent.get(key)

                proc_locs[(i, j)] = cp.loads(loc.reveal()[1])

        for c in range(numcols):
            for k in range(bsize):
                dest = proc_locs([rid, c])
                send_id = ('l', rid, k + (bsize * cid))

                msg = cp.dumps((send_id, lblock[:,k]))
                fluent.send(dest, msg)

        for r in range(numrows):
            for k in range(bsize):
                dest = proc_locs([r, cid])
                send_id = ('r', k + (bsize * rid), cid)

                msg = cp.dumps((send_id, rblock[k,:]))
                fluent.send(dest, msg)

        for r in bsize:
            for l in bsize:
                res = np.add(np.outer(lblock[:,l], rblock[r,:]), res)

        num_recvs = (num_rows - 1) * bsize  + (num_cols - 1) * bsize
        recv_count = 0
        left_recvs = {}
        right_recvs = {}
        while recv_count < num_recvs:
            msgs = fluent.recv()
            recv_count += (len(msgs))

            for msg in msgs:
                _, body = msg
                body = cp.loads(body)

                send_id = body[0]
                if send_id[0] == 'l':
                    col = body[1]
                    left_recvs[send_id[1:]] = col

                    for row in right_recvs.values():
                        res = np.add(np.outer(col, row), res)

                if send_id[0] == 'r':
                    row = body[1]
                    right_recvs[send_id[1:]] = row

                    for col in left_recvs.values():
                        res = np.add(np.outer(col, row), res)


        return res

    cloud_summa = flconn.register(summa, 'summa')

    if cloud_summa:
        print('Successfully registered summa function.')
    else:
        sys.exit(1)

    ### TEST REGISTERED FUNCTIONS ###
    n = 2
    inp1 = np.random.randn(n, n)
    inp2 = np.random.randn(n, n)
    nr = 2
    nc = 2
    bsize = 1

    def get_block(arr, row, col, bsize):
        row_start = row * bsize
        row_end = (row + 1) * bsize
        col_start = col * bsize
        col_end = (col + 1) * bsize

        return arr[row_start:row_end, col_start:col_end]

    rids = {}
    for r in range(nr):
        for c in range(nc):
            lblock = get_block(inp1, r, c, bsize)
            rblock = get_block(inp2, r, c, bsize)

            rids[(r, c)] = cloud_summa(lblock, rblock, r, c, nr, nc)

    result = np.zeros((n, n))
    for key in rids:
        res = rids[key].get()
        r = key[0]
        c = key[1]
        print(res)
        result[(r * bsize):((r + 1) * bsize), (c * bsize):((c + 1) * bsize)] = res

    print(result)
    print(np.matmul(inp1, inp2))

    return [], [], [], []
