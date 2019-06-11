# Executing Functions in Fluent

Assuming you have a Fluent cluster running, as described [here](getting-started-aws.md), you can run functions and DAGs in Fluent.

First, we'll create two new functions:

```python3
>>> from client import FluentConnection
>>> flconn = FluentConnection(AWS_FUNCTION_ELB, MY_IP)
>>> incr = lambda fluent, a: a + 1
>>> cloud_incr = flconn.register(incr, 'incr')
>>> cloud_incr(1).get()
2
>>> square = lambda fluent, a: a * a
>>> cloud_square = flconn.register(square, 'square')
>>> cloud_square(2).get()
4
```

Now we'll chain those functions together and execute them at once:

```python3
# create a DAG with two functions, incr and square, where incr comes before square
>>> flconn.register_dag('test_dag', ['incr', 'square'], [('incr', 'square')])
True # returns False if registration fails, e.g., if one of the referenced functions does not exist
>>> flconn.call_dag('test_dag', { 'incr': 1 }).get()
4
```

* All functions take a `fluent` argument as their first parameter. See [below](#Fluent-API) for the full API for this library, which includes message passing and KVS access.
* All calls to functions and DAGs are by default asynchronous. Results are stored in the key-value store, and object IDs are returned. DAG calls can optionally specify synchronous calls by setting the `direct_response` argument to `True`.
* DAGs can have arbitrary branches and connections and have multiple sources, but there must be only one sink function in the DAG. The result of this sink function is what is returned to the caller.


## Fluent API

| API Name  | Functionality | 
|-----------|---------------|
| `get(key)`| Retrieves `key` from the KVS |
| `put(key, value)`| Puts `value` into the KVS at key `key` |
| `get_id()`| Returns the unique messaging identifier for this function |
| `send(id, msg)`| Sends message contents `msg` to the function at ID `id` |
| `recv()`| Receives any messages sent to this function |