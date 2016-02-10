# Borrowed from https://github.com/kennethreitz/dj-database-url/blob/master/dj_database_url.py

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

from .sqlite.liteaxolotlstore import LiteAxolotlStore
from .postgres.pgaxolotlstore import PostgresAxolotlStore


# Register database schemes in URLs.
urlparse.uses_netloc.append('postgres')
urlparse.uses_netloc.append('postgresql')
urlparse.uses_netloc.append('pgsql')
urlparse.uses_netloc.append('sqlite')


def get_store_from_url(url, username=''):
    url = urlparse.urlparse(url)

    # Split query strings from path.
    path = url.path[1:]
    if '?' in path and not url.query:
        path, query = path.split('?', 2)
    else:
        path, query = path, url.query
    query = urlparse.parse_qs(query)

    # If we are using sqlite and we have no path, then assume we
    # want an in-memory database (this is the behaviour of sqlalchemy)
    if url.scheme == 'sqlite' and path == '':
        path = ':memory:'

    # Handle postgres percent-encoded paths.
    hostname = url.hostname or ''
    if '%2f' in hostname.lower():
        hostname = hostname.replace('%2f', '/').replace('%2F', '/')

    config = {
        'dbname': urlparse.unquote(path or ''),
        'user': urlparse.unquote(url.username or ''),
        'password': urlparse.unquote(url.password or ''),
        'host': hostname,
        'port': url.port or ''
    }
    if url.scheme in ['postgres', 'postgresql', 'pgsql']:
        store = PostgresAxolotlStore('dbname={dbname} user={user} password='
            '{password} host={host} port={port}'.format(**config),
            table_prefix='yowsup_{}'.format(username))
    elif url.scheme == 'sqlite':
        store = LiteAxolotlStore(config['dbname'])

    return store
