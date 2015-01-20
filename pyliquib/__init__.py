import logging
import threading
import time
import uuid

from constants import *
from pyliquib.inlinejs import InlineJsChangeSet
from pyliquib.pycallback import PyCallbackChangeSet

logger = logging.getLogger('pyliquib')


def run(db, change_sets):
    __run(db, change_sets)


def pycall(id, author, callback, comment='', always=False):
    return PyCallbackChangeSet(id, author, callback, comment, always)


def js(id, author, js, comment='', always=False):
    return InlineJsChangeSet(id, author, js, comment, always)


def __run(db, change_sets, lock=threading.RLock()):
    with lock:
        try:
            if not __acquire_liquib_lock(db):
                raise ValueError("Couldn't acquire database lock. Please check the %s collection" % LIQUIB_LOCK)

            if len(change_sets) != len(set(map(lambda cs: cs.id, change_sets))):
                import collections

                ids = map(lambda x: x.id, change_sets)
                duplicates = [x for x, count in collections.Counter(ids).items() if count > 1]
                raise ValueError("Duplicate id: %s" % duplicates)

            __ensure_id_index(db)

            processed_ids = set()
            for cs in change_sets:
                cs.execute(db)
                processed_ids.add(cs.id)
        finally:
            __release_liquib_lock(db)


def __ensure_id_index(db):
    db[LIQUIB_LOG].ensure_index('id', 1, unique=True)


def __ensure_lock_index(db):
    lock_collection = db[LIQUIB_LOCK]
    lock_collection.remove({'concurrent': {'$ne': 1}})
    lock_collection.ensure_index('concurrent', 1, unique=True, expireAfterSeconds=20)


def __acquire_liquib_lock(db, sleep_time_secs=5):
    logger.info('Acquiring lock...')
    __ensure_lock_index(db)
    lock_collection = db[LIQUIB_LOCK]

    my_uuid = str(uuid.uuid4())

    for i in range(1, 6):
        lock_collection.insert({'concurrent': 1, 'uuid': my_uuid}, w=99, j=True, fsync=True)

        lock = lock_collection.find_one({'concurrent': 1})

        if lock['uuid'] == my_uuid:
            return True
        else:
            print 'attempt %d' % i
            logger.info('Database is locked. Waiting for %d seconds.', sleep_time_secs)
            time.sleep(sleep_time_secs)

    return False


def __release_liquib_lock(db):
    logger.info('Releasing lock...')
    db[LIQUIB_LOCK].update({}, {"$set": {'lock_i': 0}})
