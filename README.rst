======
keeper
======

Keeper is a filesystem-based, content-addressable value store for Python 3.

Installation
============

  $ pip install keeper


Using Keeper
============

Keeper is constructed with a Storage object. This example uses FileStorage::

    from keeper import FileStorage, Keeper

Pass a directory path to a FileStorage instance, and then pass the Storage instance to the Keeper::

   with FileStorage("/some/directory/") as storage:
        with Keeper(storage) as k:
           ...

Add string or bytes objects, and get a key in return::

       key = self.keeper.add("The quick brown fox jumped over the lazy dog")

Use the key to retrieve the string::

       value = keeper[key].as_string()


Add another string with some metadata::

       key = keeper.add(text, mime="text/plain", filename="foo.txt", author="Joe Bloggs")

Retrieve metadata::

       print(keeper[key].meta.mime)


Add a binary stream you can write to::

       with keeper.add_stream() as stream:
            stream.write(b'A large number of bytes')

After the stream has closed, retrieve the key::

       key = stream.key


Write cacheing
--------------

On slow filesystems, such as USB flash drives, it's possible to buffer writes in RAM, using a
storage intermediary called WriteCacheStorage. Use it like this::

    from keeper import FileStorage, WriteCacheStorage, Keeper

    with FileStorage("/some/directory/") as file_storage:
        with WriteCacheStorage(file_storage) as cached_storage:
            with Keeper(cached_storage) as k:
                ...


Closing the WriteCacheStorage instance will block until all pending writes have been committed to
the underlying FileStorage.


Deployment
==========

To build and deploy a package to PyPI::

  $ bumpversion patch
  $ python setup.py sdist bdist_wheel
  $ twine upload dist/*


