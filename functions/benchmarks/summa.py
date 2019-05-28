import cloudpickle as cp
import logging
import sys
import time
import uuid

from anna.lattices import LWWPairLattice
from include.functions_pb2 import *
from include.kvs_pb2 import *
from include.serializer import *
from include.shared import *

def run(flconn, kvs, num_requests, sckt):
    ### DEFINE AND REGISTER FUNCTIONS ###
    def summa(fluent, uid, lblock, rblock, rid, cid, numrows, numcols):
        import cloudpickle as cp
        from anna.lattices import LWWPairLattice
        import time
        gstart = time.time()

        bsize = lblock.shape[0]
        ssize = 100
        res = np.zeros((bsize, bsize))

        myid = fluent.getid()
        key = '%s: (%d, %d)' %  (uid, rid, cid)

        fluent.put(key, LWWPairLattice(0, cp.dumps(myid)))

        start = time.time()
        proc_locs = {}
        keyset = []
        idset = {}
        for i in range(numrows):
            if i == rid:
                continue

            key = '%s: (%d, %d)' % (uid, i, cid)
            keyset.append(key)
            idset[key] = (i, cid)

        for j in range(numcols):
            if j == cid:
                continue

            key = '%s: (%d, %d)' % (uid, rid, j)
            keyset.append(key)
            idset[key] = (rid, j)


        locs = fluent.get(keyset)
        while None in locs.values():
            locs = fluent.get(keyset)

        for key in locs:
            loc = idset[key]
            proc_locs[loc] = cp.loads(locs[key].reveal()[1])

        end = time.time()
        gtime = end - gstart

        start = time.time()
        for c in range(numcols):
            if c == cid:
                continue

            for k in range(int(bsize / ssize)):
                dest = proc_locs[(rid, c)]
                send_id = ('l', k + (bsize * cid))

                msg = cp.dumps((send_id, lblock[:,(k * ssize):((k+1) * ssize)]))
                fluent.send(dest, msg)

        for r in range(numrows):
            if r == rid:
                continue

            for k in range(int(bsize / ssize)):
                dest = proc_locs[(r, cid)]
                send_id = ('r', k + (bsize * rid))

                msg = cp.dumps((send_id, rblock[(k * ssize):((k+1) * ssize),:]))
                fluent.send(dest, msg)
        end = time.time()
        stime = end - start

        num_recvs = (((numrows - 1) * bsize) / ssize) * 2
        recv_count = 0
        left_recvs = {}
        right_recvs = {}

        start = time.time()
        for l in range(int(bsize / ssize)):
            left_recvs[l + (bsize * cid)] = lblock[:,(l * ssize):((l+1) * ssize)]

        for r in range(int(bsize / ssize)):
            right_recvs[r + (bsize * rid)] = rblock[(r * ssize):((r+1) * ssize),:]

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
                        res = np.add(np.matmul(col, match_vec), res)

                        del right_recvs[key]
                        del left_recvs[key]

                if send_id[0] == 'r':
                    row = body[1]
                    key = send_id[1]
                    right_recvs[key] = row

                    if key in left_recvs:
                        match_vec = left_recvs[key]
                        res = np.add(np.matmul(match_vec, row), res)

                        del right_recvs[key]
                        del left_recvs[key]

        for key in left_recvs:
            left = left_recvs[key]
            right = right_recvs[key]
            logging.info(left.shape)
            logging.info(right.shape)

            res = np.add(res, np.matmul(left, right))
        end = time.time()
        ctime = end - start
        return res, gtime, stime, ctime, (end - gstart)

    cloud_summa = flconn.register(summa, 'summa')

    if cloud_summa:
        print('Successfully registered summa function.')
    else:
        sys.exit(1)

    ### TEST REGISTERED FUNCTIONS ###
    n = 10000
    inp1 = np.random.randn(n, n)
    inp2 = np.random.randn(n, n)
    nt = 5
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

        left_id_map = {}
        right_id_map = {}
        for r in range(nr):
            for c in range(nc):
                lblock = get_block(inp1, r, c, bsize)
                rblock = get_block(inp2, r, c, bsize)
                id1 = str(uuid.uuid4())
                id2 = str(uuid.uuid4())

                kvs.put(id1, LWWPairLattice(0, serialize_val(lblock)))
                kvs.put(id2, LWWPairLattice(0, serialize_val(rblock)))

                left_id_map[(r, c)] = id1
                right_id_map[(r, c)] = id2

        start = time.time()
        for r in range(nr):
            for c in range(nc):
                r1 = FluentReference(left_id_map[(r, c)], LWW, True)
                r2 = FluentReference(right_id_map[(r, c)], LWW, True)

                rids[(r, c)] = cloud_summa(uid, r1, r2, r, c, nr, nc)
        end = time.time()
        print('Scheduling to %.6f seconds.' % (end - start))

        result = np.zeros((n, n))
        get_times = []
        send_times = []
        comp_times = []
        total_times = []

        for key in rids:
            lstart = time.time()
            res = rids[key].get()
            lend = time.time()
            get_times.append(res[1])
            send_times.append(res[2])
            comp_times.append(res[3])
            total_times.append(res[4])

            res = res[0]
            r = key[0]
            c = key[1]
            result[(r * bsize):((r + 1) * bsize), (c * bsize):((c + 1) * bsize)] = res

        end = time.time()
        latencies.append(end -start)

        if False in np.isclose(result, np.matmul(inp1, inp2)):
            print('Failure!')

    return latencies, [], [], 0
