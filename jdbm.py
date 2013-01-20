'''
jdbm
=======
Journaling dbm

Allowed backends
----------------

- 'memory' (mostly for tests)
- 'tc' (tokyo cabinet)

Usage
-----

    >>> from jdbm import jdbm
    >>> db = jdbm('tc', filename='var/readme-test.tc',
    ...     journal_filename='var/readme-test.journal.tc')

Standard operations:

    >>> db.put('a', 'foo')
    >>> db.put('b', 'bar')
    >>> db.get('a')
    'foo'
    >>> set(k for k in db)
    set(['a', 'b'])
    >>> db.exists('a')
    True
    >>> db.delete('a')
    >>> db.length()
    1

Restore db from journal:

    >>> db.clear(journaling=False)
    >>> db.length()
    0
    >>> db.restore_from_journal()
    >>> db.length()
    1
'''
import os
import gzip
try:
    import simplejson as json
except ImportError:
    import json


__version__ = '1.0'


BACKENDS = {
    'memory': 'MemoryBackend',
    'tc': 'TokyoCabinetBackend',
}


def jdbm(backend, **kwargs):
    if backend in BACKENDS:
        backend = globals().get(BACKENDS[backend])
    return backend(**kwargs)


class BaseBackend(object):
    '''
    Backend should privide next methods:
        put
        get
        __iter__
        delete
        exists
        length
    '''
    def __init__(self, filename=None, journal_filename=None,
            makedirs=True, **kwargs):
        if not any([filename, journal_filename]):
            raise AttributeError('journal_filename or filename is required')
        self.journal_filename = journal_filename or filename + '.journal.gz'
        if makedirs:
            dirname = os.path.dirname(os.path.abspath(self.journal_filename))
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            self.journal_open()

    def journal_open(self, mode='a+'):
        if getattr(self, 'journal', None):
            self.journal.close()
        self.journal = gzip.open(self.journal_filename, mode)

    def journal_log_put(self, k, v):
        entry = json.dumps(['+', k, v])
        self.journal.write(entry + '\n')

    def journal_log_del(self, k):
        entry = json.dumps(['-', k])
        self.journal.write(entry + '\n')

    def journal_clear(self):
        self.journal_open('w')

    def clear(self, journaling=True):
        '''Remove all db items'''
        for k in list(self):
            self.delete(k, journaling=journaling)

    def restore_from_journal(self):
        self.clear(journaling=False)
        self.journal_open('r')
        for i, dump in enumerate(self.journal):
            i += 1
            data = json.loads(dump)
            action = data.pop(0)
            if action == '+':
                self.put(*data, journaling=False)
            elif action == '-':
                self.delete(*data, journaling=False)
            else:
                raise Exception('Journal error in line %i' % (i))
        self.journal_open()


class DictStyleBackend(BaseBackend):
    '''
    Backend for any dict-style dmbs
    '''
    def __init__(self, **kwargs):
        super(DictStyleBackend, self).__init__(**kwargs)
        self.db = {}  # rewrite on the real backend

    def put(self, k, v, journaling=True):
        self.db[k] = v
        if journaling:
            self.journal_log_put(k, v)

    def get(self, k, default=None):
        try:
            return self.db[k]
        except KeyError:
            return default

    def delete(self, k, journaling=True):
        del self.db[k]
        if journaling:
            self.journal_log_del(k)

    def __iter__(self):
        for k in self.db:
            yield k

    def exists(self, k):
        return k in self.db

    def length(self):
        return len(self.db)


class MemoryBackend(DictStyleBackend):
    '''
    DB in memory for tests
    '''
    def __init__(self, **kwargs):
        super(MemoryBackend, self).__init__(**kwargs)
        self.db = {}


class TokyoCabinetBackend(DictStyleBackend):
    '''
    DB in memory for tests
    '''
    def __init__(self, filename, compress=True, **kwargs):
        super(TokyoCabinetBackend, self).__init__(filename=filename, **kwargs)
        self.db = tc_open(filename)


def tc_open(path, mode='a+', compress=True):
    import tc

    db = tc.HDB()
    if compress:
        db.tune(-1, -1, -1, tc.HDBTDEFLATE)
    db.open(path,
        {
            'r': tc.HDBOREADER,
            'w': tc.HDBOWRITER | tc.HDBOCREAT | tc.HDBOTRUNC,
            'a': tc.HDBOWRITER,
            'a+': tc.HDBOWRITER | tc.HDBOCREAT,
        }[mode]
    )
    return db


def run_tests():
    import sys
    import doctest

    doctest.testmod()

    for backend in ['memory', 'tc']:
        print '=== Testing backend: ', backend

        db = jdbm(backend, filename='var/%s-backend-test' % backend)
        db.journal_clear()
        db.put('a', '111')

        db.clear(journaling=False)
        assert db.length() == 0

        db.restore_from_journal()
        assert db.length() == 1

        assert db.get('a') == '111'
        assert db.get('b') == None

        assert db.exists('a')
        assert not db.exists('b')

        db.delete('a')
        assert not db.exists('a')


        # journal test:
        db.journal.close()
        data = gzip.open(db.journal_filename).read()
        assert data == '["+", "a", "111"]\n["-", "a"]\n'

        db.journal_clear()
        db.journal.close()
        data = gzip.open(db.journal_filename).read()
        assert data == ''

        print 'test passed'



if __name__ == '__main__':
    run_tests()
