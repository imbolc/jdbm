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