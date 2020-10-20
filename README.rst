======
keeper
======

Keeper is a filesystem-based, content-addressable value store for Python 3.

Installation
============

  $ pip install keeper


Using Keeper
============

Pass a directory path to a Keeper instance::

   from keeper import Keeper

   with Keeper("/some/directory/") as k:
       ...

Add string or bytes objects, and get a key in return::

       key = self.keeper.add("The quick brown fox jumped over the lazy dog")

Use the key to retrieve the string::

       value = keeper[key].as_string()


Add another string with some metadata::

       key = keeper.add(text, mime="text/plain", filename="foo.txt", author="Joe Bloggs")

Retrieve metadata::

       print(keeper[key].meta.mime)


Add a stream you can write to::

       with keeper.add_stream() as stream:
            stream.write(b'A large number of bytes')

After the stream has closed, retrieve the key::

       key = stream.key

