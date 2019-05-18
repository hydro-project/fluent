import pickle
import random
import time
import uuid
import sys

import cloudpickle as cp
import redis
import numpy

from anna.lattices import *

def run(flconn, kvs, create, num_requests, count_anomalies, sckt, reply_frac=0.2):


    class FluentRedisShim:
        def __init__(self, fluent_user_library):
            self._fluent_lib = fluent_user_library

        # Checking for existence of arbitrary keys.
        def exists(self, key):
            vc, value_or_none = self._fluent_lib.get(key)
            return value_or_none is not None

        ## Single value storage.

        # Retrieving arbitrary values that were stored by set().

        def get(self, key):
            vc, value = self.get_with_vc(key)
            return value

        def get_with_vc(self, key):
            if self.exists(key):
                vc, values = self._fluent_lib.get(key)
                value = values[0]
                value = pickle.loads(value)
                return vc, value
            else:
                return None, None

        # Storing arbitary values that can be retrieved by get().
        def set(self, key, value):
            value = pickle.dumps(value)
            self._fluent_lib.put(key, value)

        # Do a set with causal dependencies.
        # Dependencies is {'key': <vc>}
        def causal_set(self, key, value, dependencies):
            value = pickle.dumps(value)

            client_id = str(int(uuid.uuid4()))
            vector_clock = {client_id: 1}
            self._fluent_lib.causal_put(key, vector_clock, dependencies, value, client_id)

        ## Counter storage.

        # incr in Redis is used for two things:
        # - returning unique IDs
        # - as a counter
        # This mocks out the former.
        def incr(self, key):
            return int(uuid.uuid4())


        ## Set storage.

        # Add an item to the set at this key.
        def sadd(self, key, value):
            value = pickle.dumps(value)
            self._fluent_lib.put(key, value)

        # Remove an item from the set at this key.
        def srem(self, key, value):
            raise NotImplementedError  # No removals in experiments rn; implement tombstones later if you need it.

        # Set contents.
        def smembers(self, key):
            if self.exists(key):
                vc, values = self._fluent_lib.get(key)
                return set((pickle.loads(val) for val in values))
            else:
                return set()

        # Set membership.
        def sismember(self, key):
            return key in self.smembers(key)

        # Set size.
        def scard(self, key):
            return len(self.smembers(key))


        ## Append-only lists.

        # Append.
        def lpush(self, key, value):
            # microseconds.
            # This value will be 16 digits long for the foreseeable future.
            ts = int(time.time() * 1000000)
            value = pickle.dumps(value)
            value = ('{}:{}'.format(ts, value)).encode()
            self._fluent_lib.put(key, value)

        # Slice.
        def lrange(self, key, begin, end):
            if self.exists(key):
                vc, values = self._fluent_lib.get(key)
                oset = ListBasedOrderedSet(values)
                values = [
                    # trim off timestamp + delimiter, and deserialize the rest.
                    pickle.loads(eval(item.decode()[17:]))
                    for item in oset.lst[begin:end]
                ]
                return values
            else:
                return []

        # Size.
        def llen(self, key):
            if self.exists(key):
                vc, values = self._fluent_lib.get(key)
                return len(list(values))
            else:
                return 0


    class RealRedisShim:
        def __init__(self, fluent_user_library):
            self.r = redis.Redis(host='3.89.194.39', port=6379, db=0)

        def exists(self, key):
            res = self.r.exists(key)
            return res

        def get(self, key):
            res = self.r.get(key)
            return res

        def set(self, key, value):
            res = self.r.set(key, value)
            return res

        def incr(self, key):
            res = self.r.incr(key)
            return res

        def sadd(self, key, value):
            res = self.r.sadd(key, value)
            return res

        def smembers(self, key):
            res = self.r.smembers(key)
            return res

        def sismember(self, key, value):
            res = self.r.sismember(key, value)
            return res

        def scard(self, key):
            res = self.r.scard(key)
            return res

        def lpush(self, key, value):
            res = self.r.lpush(key, value)
            return res

        def lrange(self, key, begin, end):
            res = self.r.lrange(key, begin, end)
            return res

        def llen(self, key):
            res = self.r.llen(key)
            return res


    class TimedRedisShim:
        REDIS_SHIM_TO_USE = FluentRedisShim
        def __init__(self, fluent_user_library):
            self.r = REDIS_SHIM_TO_USE(fluent_user_library)
            self.times = {
                'exists': [],
                'get': [],
                'set': [],
                'incr': [],
                'sadd': [],
                'smembers': [],
                'sismember': [],
                'scard': [],
                'lpush': [],
                'lrange': [],
                'llen': [],
            }

        def exists(self, key):
            start = time.time()
            res = self.r.exists(key)
            self.times['exists'].append(time.time() - start)
            return res

        def get(self, key):
            start = time.time()
            res = self.r.get(key)
            self.times['get'].append(time.time() - start)
            return res

        def set(self, key, value):
            start = time.time()
            res = self.r.set(key, value)
            self.times['set'].append(time.time() - start)
            return res

        def incr(self, key):
            start = time.time()
            res = self.r.incr(key)
            self.times['incr'].append(time.time() - start)
            return res

        def sadd(self, key, value):
            start = time.time()
            res = self.r.sadd(key, value)
            self.times['sadd'].append(time.time() - start)
            return res

        def smembers(self, key):
            start = time.time()
            res = self.r.smembers(key)
            self.times['smembers'].append(time.time() - start)
            return res

        def sismember(self, key, value):
            start = time.time()
            res = self.r.sismember(key, value)
            self.times['sismember'].append(time.time() - start)
            return res

        def scard(self, key):
            start = time.time()
            res = self.r.scard(key)
            self.times['scard'].append(time.time() - start)
            return res

        def lpush(self, key, value):
            start = time.time()
            res = self.r.lpush(key, value)
            self.times['lpush'].append(time.time() - start)
            return res

        def lrange(self, key, begin, end):
            start = time.time()
            res = self.r.lrange(key, begin, end)
            self.times['lrange'].append(time.time() - start)
            return res

        def llen(self, key):
            start = time.time()
            res = self.r.llen(key)
            self.times['llen'].append(time.time() - start)
            return res




    class Timeline:
      @staticmethod
      def page(r,page):
        _from = (page-1)*10
        _to = (page)*10
        return [Post(r, post_id).content for post_id in r.lrange('timeline',_from,_to)]

    class Model(object):
      def __init__(self, r, id):
        self.__dict__['id'] = id
        self.__dict__['r'] = r

      def __eq__(self,other):
        return self.id == other.id

      def __setattr__(self,name,value):
        if name not in self.__dict__:
          klass = self.__class__.__name__.lower()
          key = '%s:id:%s:%s' % (klass,self.id,name.lower())
          self.r.set(key,value)
        else:
          self.__dict__[name] = value

      def __getattr__(self,name):
        if name not in self.__dict__:
          klass = self.__class__.__name__.lower()
          v = self.r.get('%s:id:%s:%s' % (klass,self.id,name.lower()))
          if v:
            return v
          raise AttributeError('%s doesn\'t exist' % name)
        else:
          return self.__dict__[name]

    class User(Model):
      @staticmethod
      def find_by_user(r, user):
        _id = r.get("user:user:%s" % user)
        if _id is not None:
          return int(_id)
        else:
          return None

      @staticmethod
      def find_by_id(_id):
        if r.exists("user:id:%s:user" % _id):
          return User(int(_id))
        else:
          return None

      @staticmethod
      def create(r, user, password):
        user_id = r.incr("user:uid")
        if not r.get("user:user:%s" % user):
          r.set("user:id:%s:user" % user_id, user)
          r.set("user:user:%s" % user, user_id)

          r.set("user:id:%s:password" % user_id, password)
          r.lpush("users", user_id)
        # return User(user_id)
        # return None

      def posts(self,page=1):
        _from, _to = (page-1)*10, page*10
        posts = r.lrange("user:id:%s:posts" % self.id, _from, _to)
        if posts:
          return [Post(int(post_id)) for post_id in posts]
        return []

      @staticmethod
      def timeline(r, user, page=1):
        userid = User.find_by_user(r, user)
        timeline_len = r.llen("user:id:%s:timeline" % userid)
        _from, _to = timeline_len - page*10, timeline_len - (page-1)*10,
        timeline = r.lrange("user:id:%s:timeline" % userid, _from, _to)
        if timeline:
          # XXX waiting on causal consistency is done already,
          # but we still need to reinsert the dependee tweet.
          # We can skip this for now since the performance overhead is already incurred;
          # inserting the actual tweets is just a quick operation for application-semantics correctness.
          return [(int(post_id), Post(r, int(post_id)).content) for post_id in timeline]
        return []

      @staticmethod
      def timeline_anomalies(r, user):
        userid = User.find_by_user(r, user)
        timeline_len = r.llen("user:id:%s:timeline" % userid)
        _from, _to = 0, timeline_len
        timeline = r.lrange("user:id:%s:timeline" % userid, _from, _to)
        if timeline:

          # XXX waiting on causal consistency is always done upon retrieving post contents,
          # but this says nothing about the state of the timeline, which can have missing postids.
          # Here, we get the list of parent tweets of any replies in the timeline
          # and make sure they're in the timeline too.
          tl_set = set(int(pid) for pid in timeline)
          parents_of_replies = []
          tl_posts = []
          missing_posts = []
          for post_id in timeline:
            post = Post(r, int(post_id))
            tl_posts.append((int(post_id), post.content))
            try:
                post_parent_id = post.parent
                print(post_parent_id)
                parents_of_replies.append(int(post_parent_id))
            except AttributeError:
                pass

          # See how many anomalies we got.
          for parent_pid in parents_of_replies:
            if parent_pid not in tl_set:
              missing_posts.append((int(parent_pid), Post(r, int(parent_pid)).content))

          return tl_posts, missing_posts
        return [], []

      def mentions(self,page=1):
        _from, _to = (page-1)*10, page*10
        mentions = r.lrange("user:id:%s:mentions" % self.id, _from, _to)
        if mentions:
          return [Post(int(post_id)) for post_id in mentions]
        return []

      @staticmethod
      def add_post(r, userid, post_id):
        r.lpush("user:id:%s:posts" % userid, post_id)
        r.lpush("user:id:%s:timeline" % userid, post_id)
        r.sadd('posts:id', post_id)

      @staticmethod
      def add_timeline_post(r, userid, post_id):
        r.lpush("user:id:%s:timeline" % userid, post_id)

      def add_mention(self,post):
        r.lpush("user:id:%s:mentions" % self.id, post.id)

      @staticmethod
      def follow(r, user, target):
        userid = User.find_by_user(r, user)
        targetid = User.find_by_user(r, target)
        if userid == targetid:
          return
        else:
          r.sadd("user:id:%s:followees" % userid, targetid)
          User.add_follower(r, targetid, userid)

      def stop_following(self,user):
        r.srem("user:id:%s:followees" % self.id, user.id)
        user.remove_follower(self)

      def following(self,user):
        if r.sismember("user:id:%s:followees" % self.id, user.id):
          return True
        return False

      @staticmethod
      def followers(r, userid):
        followers = r.smembers("user:id:%s:followers" % userid)
        if followers:
          return followers
        return []

      @staticmethod
      def followees(userid):
        followees = r.smembers("user:id:%s:followees" % userid)
        if followees:
          return followers
        return []


      #added
      @property
      def tweet_count(self):
        return r.llen("user:id:%s:posts" % self.id) or 0

      @property
      def followees_count(self):
        return r.scard("user:id:%s:followees" % self.id) or 0

      @property
      def followers_count(self):
        return r.scard("user:id:%s:followers" % self.id) or 0

      @staticmethod
      def add_follower(r, userid, targetid):
        r.sadd("user:id:%s:followers" % userid, targetid)

      def remove_follower(self,user):
        r.srem("user:id:%s:followers" % self.id, user.id)

    class Post(Model):
      @staticmethod
      def create(r, user, content, parent_post_id=None):
        userid = User.find_by_user(r, user)
        post_id = r.incr("post:uid")
        post = Post(r, post_id)
        post.user_id = userid
        # #post.created_at = Time.now.to_s

        # Handle replies. If we're replying to a tweet,
        # parent_content_key is the key of that tweet's content.
        if parent_post_id is None:
            post.content = content
        else:
            klass = post.__class__.__name__.lower()

            # Convert the parent post id to its post content id.
            parent_content_key = '%s:id:%s:%s' % (klass, parent_post_id, 'content')

            # Get the parent tweet content's VC; we need it to express a dependency on it.
            vc, _ = r.get_with_vc(parent_content_key)

            # Post our tweet, with the causal dependency.
            r.causal_set('%s:id:%s:%s' % (klass,post.id,'content'), content, {parent_content_key: vc})

            # Set that this tweet has a parent (for application use, not kvs consistency).
            post.parent = parent_post_id


        User.add_post(r, userid, post_id)
        # r.lpush("timeline", post_id)  # not testing global timeline
        for follower in User.followers(r, userid):
          User.add_timeline_post(r, follower, post_id)

        # mentions = re.findall('@\w+', content)
        # for mention in mentions:
        #   u = User.find_by_user(mention[1:])
        #   if u:
        #     u.add_mention(post)




      @staticmethod
      def find_by_id(id):
        if r.sismember('posts:id', int(id)):
          return Post(id)
        return None

      @property
      def user(self):
        return User.find_by_id(r.get("post:id:%s:user_id" % self.id))




    def xxx_redis_exists(fluent, key):
        redis = FluentRedisShim(fluent)
        return str(redis.exists(key))
    def xxx_redis_get(fluent, key):
        redis = FluentRedisShim(fluent)
        return str(redis.get(key))
    def xxx_redis_set(fluent, key, value):
        redis = FluentRedisShim(fluent)
        redis.set(key, value)
        return 'success'
    def xxx_redis_incr(fluent, key):
        redis = FluentRedisShim(fluent)
        return str(redis.incr(key))
    def xxx_redis_sadd(fluent, key, value):
        redis = FluentRedisShim(fluent)
        redis.sadd(key, value)
        return 'success'
    def xxx_redis_smembers(fluent, key):
        redis = FluentRedisShim(fluent)
        return str(redis.smembers(key))
    def xxx_redis_lpush(fluent, key, value):
        redis = FluentRedisShim(fluent)
        redis.lpush(key, value)
        return 'success'
    def xxx_redis_lrange(fluent, key, begin, end):
        redis = FluentRedisShim(fluent)
        return str(redis.lrange(key, begin, end))
    def xxx_redis_llen(fluent, key):
        redis = FluentRedisShim(fluent)
        return str(redis.llen(key))




    # def xxx_global_timeline(fluent_lib, page):
    #     f_redis = FluentRedisShim(fluent_lib)
    #     return Timeline.page(f_redis, page)

    def xxx_user_create(fluent_lib, user):
        f_redis = FluentRedisShim(fluent_lib)
        # fluent_lib.put('xxx_user', SetLattice({b'hi',}))
        # fluent_lib.put('xxx_userb', OrderedSetLattice(ListBasedOrderedSet([b'hi'])))
        User.create(f_redis, user, 'password')
        return 'success'

    def xxx_user_timeline(fluent_lib, user, page):
        f_redis = FluentRedisShim(fluent_lib)
        return User.timeline(f_redis, user, page)

    def xxx_user_timeline_anomalies(fluent, user):
        f_redis = FluentRedisShim(fluent)
        return User.timeline_anomalies(f_redis, user)

    def xxx_user_profile(fluent_lib, user, page):
        pass

    def xxx_user_follow(fluent_lib, user, target):
        f_redis = FluentRedisShim(fluent_lib)
        User.follow(f_redis, user, target)
        return 'success'

    def xxx_post_create(fluent_lib, user, post):
        f_redis = FluentRedisShim(fluent_lib)
        Post.create(f_redis, user, post)
        return 'success'

    def xxx_reply_create(fluent_lib, user, post, parent_cid):
        f_redis = FluentRedisShim(fluent_lib)
        Post.create(f_redis, user, post, parent_cid)
        return 'success'

    # user, postcontent, pids -> ...
    # def post_create_with_dep(flib, user, post, deps):
    #     pass

    # dag_name = 'read-and-tweet'
    # ['user_timeline_pids', 'post_create_with_dep']


    fns = {
        'xxx_redis_exists': xxx_redis_exists,
        'xxx_redis_get': xxx_redis_get,
        'xxx_redis_set': xxx_redis_set,
        'xxx_redis_incr': xxx_redis_incr,
        'xxx_redis_sadd': xxx_redis_sadd,
        'xxx_redis_smembers': xxx_redis_smembers,
        'xxx_redis_lpush': xxx_redis_lpush,
        'xxx_redis_lrange': xxx_redis_lrange,
        'xxx_redis_llen': xxx_redis_llen,
        # 'xxx_global_timeline': xxx_global_timeline,
        'xxx_user_create': xxx_user_create,
        'xxx_user_timeline': xxx_user_timeline,
        'xxx_user_timeline_anomalies': xxx_user_timeline_anomalies,
        'xxx_user_profile': xxx_user_profile,
        'xxx_user_follow': xxx_user_follow,
        'xxx_post_create': xxx_post_create,
        'xxx_reply_create': xxx_reply_create,
    }

    cfns = {}  # populate separately in create/noncreate modes

    def callfn(fname, *args):
        r = cfns[fname](*args).get()
        print("%s(%s) -> %s" % (fname, args, r))
        return r

    # Experiment parameters.
    # ######################
    num_users = 1000
    max_degree = 50
    num_pretweets = num_requests
    num_ops = num_requests  # 80% reads, 20% writes
    usernames = [str(i + 1) for i in range(num_users)]
    count_anomalies = count_anomalies

    # -> str
    def get_random_user():
        return str(int(random.random() * num_users) + 1)

    def get_zipf_user():
        a = 1.5  # "realistic social network distribution" from johann
        res = numpy.random.zipf(1.5)
        while res > num_users:
            res = numpy.random.zipf(1.5)
        return str(res)

    def get_n_zipf_users(n):
        users = set()
        while len(users) < n:
            users.add(get_zipf_user())
        return users

    # Posts a reply to the most recent tweet in the user's timeline, if available.
    # Does nothing if there are no tweets in the timeline to reply to!
    def post_random_reply(username):
        res = cfns['xxx_user_timeline'](username, 1).get()
        if len(res) == 0: return False
        parent_tweet_cid = res[-1][0]
        res = cfns['xxx_reply_create'](
            username, "{} says: @{}, agreed!".format(username, parent_tweet_cid), parent_tweet_cid).get()
        return True



    if create:
        if num_ops == 0:
            # DEFINE AND REGISTER FUNCTIONS
            cfns = {
                fname: flconn.register(f, fname)
                for fname, f
                in fns.items()
            }

            for fname, cf in cfns.items():
                if cf:
                    print ("Successfully registered {}.".format(fname))

            print ("Making %s users..." % num_users)
            # Make all the users.
            for username in usernames:
                res = cfns['xxx_user_create'](username).get()
                if res != 'success':
                    print("xxx_user_create(%s) -> %s" % (username, str(res)))
                    sys.exit(1)

            # Make all the user connections.
            # Every user calls follow max_degree times.
            # The people they follow are zipfian-distributed.

            print ("Doing %s follows per user..." % max_degree)
            for username in usernames:
                targets = get_n_zipf_users(max_degree)
                for target in targets:
                    res = cfns['xxx_user_follow'](username, target).get()
                    if res != 'success':
                        print("xxx_user_follow(%s, %s) -> %s" % (username, target, str(res)))
                        sys.exit(1)

        else:
            # PREPOPULATE TWEETS
            cfns = {
                fname: flconn.get(fname)
                for fname, f
                in fns.items()
            }
            wtimes = []
            epoch_wtimes = []
            log_start = time.time()
            # Prepopulating tweets, so our read and write times are more realistic.
            print ("Prepopulating %s tweets total..." % num_pretweets)
            for i in range(num_pretweets):
                username = get_random_user()
                post = "{} says: I love fluent!".format(username)
                # Let's make some of these tweets be replies to other tweets.
                t = random.random()
                start = time.time()
                tweeted = False
                if t < reply_frac: # Reply attempt factor.
                    tweeted = post_random_reply(username)
                if not tweeted:
                    cfns['xxx_post_create'](username, post).get()
                elapsed = time.time() - start
                wtimes.append(elapsed)
                epoch_wtimes.append(elapsed)
                if time.time() - log_start > 5:
                    print("%s tweets populated." % i)
                    # if sckt:
                    #     sckt.send(cp.dumps(epoch_wtimes))
                    epoch_wtimes.clear()
                    log_start = time.time()


    else:
        cfns = {
            fname: flconn.get(fname)
            for fname, f
            in fns.items()
        }

        # Execute workload.
        rtimes = []
        wtimes = []
        start = time.time()
        tl_lengths = []
        anomaly_counts = []
        print ("Executing %s ops workload..." % num_ops)
        for numop in range(num_ops):
            t = random.random()
            # Pick a user at uniform.
            username = get_random_user()
            # 80% reads.
            if t < 0.8:
                r_start = time.time()
                if count_anomalies:
                    res, anomalies = cfns['xxx_user_timeline_anomalies'](username).get()
                    anomaly_counts.append(len(anomalies))
                    tl_lengths.append(len(res))
                else:
                    res = cfns['xxx_user_timeline'](username, 1).get()
                rtimes.append(time.time() - r_start)

            # 20% writes.
            else:
                w_start = time.time()
                tweeted = False
                reply_roll = random.random()
                if reply_roll < reply_frac:
                    tweeted = post_random_reply(username)
                if not tweeted:
                    post = "{} says: I LOVE fluent!".format(username)
                    res = cfns['xxx_post_create'](username, post).get()
                wtimes.append(time.time() - w_start)

        end = time.time()
        elapsed = end - start


        # Sanity check: print timeline of most and least popular user.
        res = cfns['xxx_user_timeline']('1', 1).get()
        print("xxx_user_timeline('1', 1) -> %s" % (str(res)))
        res = cfns['xxx_user_timeline'](str(num_users), 1).get()
        print("xxx_user_timeline(%s, 1) -> %s" % (str(num_users), str(res)))

        if anomaly_counts:
            avg_anomaly = sum(x for x in anomaly_counts) / len(anomaly_counts)
            max_anomaly = max(anomaly_counts)
            min_anomaly = min(anomaly_counts)
            avg_tl_length = sum(x for x in tl_lengths) / len(tl_lengths)
            print("Anomalies (min/avg/max): %s/%s/%s" % (min_anomaly, avg_anomaly, max_anomaly))
            print("Average tl length: ", avg_tl_length)

        if sckt:
            sckt.send(cp.dumps((rtimes, wtimes)))

        return [elapsed], rtimes, wtimes, 0


    # # Redis shim tests (not retwis related).
    # callfn('xxx_redis_exists', 'xxx_foo')
    # callfn('xxx_redis_set', 'xxx_foo', b'3')
    # callfn('xxx_redis_get', 'xxx_foo')
    # callfn('xxx_redis_incr', 'xxx_cntr')
    # callfn('xxx_redis_sadd', 'xxx_sxt', b'4')
    # callfn('xxx_redis_smembers', 'xxx_sxt')
    # callfn('xxx_redis_lpush', 'xxx_lxt', b'5')
    # callfn('xxx_redis_lrange', 'xxx_lxt', 0, 10)
    # callfn('xxx_redis_llen', 'xxx_lxt')
    # callfn('xxx_redis_lpush', 'xxx_lxt', b'6')
    # callfn('xxx_redis_lpush', 'xxx_lxt', b'4')
    # callfn('xxx_redis_lrange', 'xxx_lxt', 0, 10)


    # # Retwis tests (not benchmark related).
    # callfn('xxx_user_create', 'xxx_alice')
    # callfn('xxx_user_create', 'xxx_bob')
    # callfn('xxx_user_create', 'xxx_obama')
    # callfn('xxx_user_follow', 'xxx_alice', 'xxx_obama')
    # callfn('xxx_user_follow', 'xxx_alice', 'xxx_bob')
    # callfn('xxx_user_follow', 'xxx_bob', 'xxx_obama')
    # # callfn('xxx_post_create', 'xxx_obama', 'hello world!')
    # # callfn('xxx_post_create', 'xxx_obama', 'hello world!')
    # # callfn('xxx_post_create', 'xxx_obama', 'hello world!')
    # # callfn('xxx_post_create', 'xxx_obama', 'hello world!')
    # callfn('xxx_post_create', 'xxx_obama', 'hello world!')
    # # print("sleeping for gossip interval...")
    # # time.sleep(20)
    # r = callfn('xxx_user_timeline', 'xxx_bob', 1)
    # # obamas_tweet = r[-1]
    # # obamas_tweet_cid = obamas_tweet[0]
    # # callfn('xxx_reply_create', 'xxx_bob', 'hi obama!', obamas_tweet_cid)
    # callfn('xxx_user_timeline_anomalies', 'xxx_alice')
    # callfn('xxx_user_timeline', 'xxx_alice', 1)
    # # return






