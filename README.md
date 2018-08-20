# Liszt

Liszt is an append-only key-list database.

## Insertion

For the sake of reliability, insertion is performed locally.

```haskell
commitFile :: FilePath -> Transaction a -> IO a
insert :: Serialise a => Key -> a -> Transaction ()

> commitFile "foo.liszt" $ insert "message" ("hello, world" :: Text)
```

## Query

`lisztd` starts a server. The first argument is the root directory to find liszt
files.

```
$ lisztd .
```

You can use the command line utility to watch a stream. `-b 0` follows a stream
from offset 0. `-f "%p\n"` prints payloads with newlines.

```
$ liszt foo.liszt message -b 0 -f "%p\n"
hello, world
```

# Representation

A liszt file consists of four types of contents: key, payload, node, and node schema.

Keys and payloads are bytestrings with arbitrary length.
