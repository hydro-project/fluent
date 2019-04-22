

def run(flconn, kvs, num_requests):
    def xxx_put(fluent, k, v):
        fluent.put(k, v)
        return 'success'

    def xxx_causal_put(fluent, k, v, dep_keys):
        fluent.causal_put(k, {'0': 1}, {dep_key: {'0': 1} for dep_key in dep_keys}, v, '0')
        return 'success'

    def xxx_get(fluent, k):
        return list(fluent.get(k))

    def xxx_causal_get(fluent, k):
        vc, values = fluent.causal_get(k, '0')
        return dict(vc), list(values)

    fns = {
        'xxx_put': xxx_put,
        'xxx_causal_put': xxx_causal_put,
        'xxx_get': xxx_get,
        'xxx_causal_get': xxx_causal_get,
    }

    cfns = {
        fname: flconn.register(fn, fname)
        for fname, fn
        in fns.items()
    }

    for fname, fn in cfns.items():
        if fn:
            print("Successfully registered {}.".format(fname))

    def callfn(fname, *args):
        r = cfns[fname](*args).get()
        print("%s(%s) -> %s" % (fname, args, r))

    callfn('xxx_put', 'xxx_a', b'3')
    callfn('xxx_get', 'xxx_a')
    callfn('xxx_causal_put', 'xxx_b', b'1', [])
    callfn('xxx_causal_put', 'xxx_c', b'2', ['xxx_b'])
    callfn('xxx_causal_get', 'xxx_c')
