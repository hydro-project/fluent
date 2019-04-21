import cloudpickle as cp
import logging
import sys
import time
import uuid

from include.functions_pb2 import *
from include.serializer import *

def run(flconn, kvs, num_requests, sckt):
    ### DEFINE AND REGISTER FUNCTIONS ###
    def summa(fluent, uid, lblock, rblock, rid, cid, numrows, numcols):
        import cloudpickle as cp
        from anna.lattices import LWWPairLattice

        bsize = lblock.shape[0]
        res = np.zeros((bsize, bsize))

        myid = fluent.getid()
        key = '%s: (%d, %d)' %  (uid, rid, cid)
        fluent.put(key, LWWPairLattice(0, cp.dumps(myid)))

        proc_locs = {}
        for i in range(numrows):
            if i == rid:
                continue
            key = '%s: (%d, %d)' % (uid, i, cid)
            loc = fluent.get(key)

            while loc is None:
                loc = fluent.get(key)

            proc_locs[(i, cid)] = cp.loads(loc.reveal()[1])

        for j in range(numcols):
            if j == cid:
                continue

            key = '%s: (%d, %d)' % (uid, rid, j)
            loc = fluent.get(key)

            while loc is None:
                loc = fluent.get(key)

            proc_locs[(rid, j)] = cp.loads(loc.reveal()[1])

        for c in range(numcols):
            if c == cid:
                continue

            for k in range(bsize):
                dest = proc_locs[rid, c]
                send_id = ('l', k + (bsize * cid))

                msg = cp.dumps((send_id, lblock[:,k]))
                fluent.send(dest, msg)

        for r in range(numrows):
            if r == rid:
                continue

            for k in range(bsize):
                dest = proc_locs[r, cid]
                send_id = ('r', k + (bsize * rid))

                msg = cp.dumps((send_id, rblock[k,:]))
                fluent.send(dest, msg)

        num_recvs = (numrows - 1) * bsize  + (numcols - 1) * bsize
        recv_count = 0
        left_recvs = {}
        right_recvs = {}

        for l in range(bsize):
            left_recvs[l + (bsize * cid)] = lblock[:,l]

        for r in range(bsize):
            right_recvs[r + (bsize * rid)] = rblock[r,:]

        while recv_count < num_recvs:
            msgs = fluent.recv()
            recv_count += (len(msgs))

            for msg in msgs:
                _, body = msg
                body = cp.loads(body)

                send_id = body[0]
                if send_id[0] == 'l':
                    col = body[1]
                    key = send_id[1]
                    left_recvs[key] = col

                    if key in right_recvs:
                        match_vec = right_recvs[key]
                        res = np.add(np.outer(col, match_vec), res)

                        del right_recvs[key]
                        del left_recvs[key]

                if send_id[0] == 'r':
                    row = body[1]
                    key = send_id[1]
                    right_recvs[key] = row

                    if key in left_recvs:
                        match_vec = left_recvs[key]
                        res = np.add(np.outer(match_vec, row), res)

                        del right_recvs[key]
                        del left_recvs[key]

        for key in left_recvs:
            left = left_recvs[key]
            right = right_recvs[key]

            res = np.add(res, np.outer(left, right))

        return res

    cloud_summa = flconn.register(summa, 'summa')

    if cloud_summa:
        print('Successfully registered summa function.')
    else:
        sys.exit(1)

    ### TEST REGISTERED FUNCTIONS ###
    n = 1000
    inp1 = np.random.randn(n, n)
    inp2 = np.random.randn(n, n)
    nt = 4
    nr = nt
    nc = nt
    bsize = int(n / nr)

    def get_block(arr, row, col, bsize):
        row_start = row * bsize
        row_end = (row + 1) * bsize
        col_start = col * bsize
        col_end = (col + 1) * bsize

        return arr[row_start:row_end, col_start:col_end]

    latencies = []
    for _ in range(num_requests):
        time.sleep(.1)
        uid = str(uuid.uuid4())
        rids = {}

        start = time.time()
        for r in range(nr):
            for c in range(nc):
                lblock = get_block(inp1, r, c, bsize)
                rblock = get_block(inp2, r, c, bsize)

                rids[(r, c)] = cloud_summa(uid, lblock, rblock, r, c, nr, nc)

        result = np.zeros((n, n))
        for key in rids:
            res = rids[key].get()
            r = key[0]
            c = key[1]
            result[(r * bsize):((r + 1) * bsize), (c * bsize):((c + 1) * bsize)] = res

        end = time.time()
        print(end - start)
        latencies.append(end -start)

        if False in np.isclose(result, np.matmul(inp1, inp2)):
            print('Failure!')

    return latencies, [], [], 0
