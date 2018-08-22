# Liszt

Liszt is an append-only key-list database.

## Insertion

For the sake of reliability, insertion is performed locally.
Insertions can be batched up to one commit in order to reduce overheads.

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

Liszt employs a 2-3 tree (special case of B tree) of skew binary random access lists as its on-disk format.

A liszt file consists of six types of contents: keys, payloads, tags, nodes, trees, and a footer.

`Key`s, `Tag`s, and `Payload`s are bytestrings with arbitrary length.

A pointer to a content with type `t` is denoted by `P t`.
It's a pair of the byte offset and the length of the content, encoded in unsigned VLQ.

A node is one of the following:

* Empty node: `0x00`
* 1-leaf: `0x01 (P Key) Spine`
* 2-leaf: `0x02 (P Key) Spine (P Key) Spine`
* 2-node: `0x12 (P Node) (P Key) Spine (P Node)`
* 3-node: `0x13 (P Node) (P Key) Spine (P Node) (P Key) Spine (P Node)`

Spine is a list of pairs of:

* Rank: the number of payloads in a tree
* Pointer to a tree

Spine prefixed by the length of the list.

* `Spine`: `Int [Int (P Tree)]`

Tree is either

* Tip: `0x80 Int Tag (P Payload)`
* Bin: `0x81 Int Tag (P Payload) (P Node) (P Node)`

where the Int represents the length of the `Tag`.

A footer is a root node padded to 256 bytes long, and the padding must end with the following byte sequence:

```
8df4 c865 fa3d 300a f3f8 9962 e049 e379 489c e4cd 2b52 a630 5584 004c 4953 5a54
```

(last 5 bytes is LISZT)

Committing is performed in the following steps. This append-only scheme grants robustness to some extent.

* Read the root node from the file.
* Append payloads.
* Append a new footer.

When an exception occurs in the middle, it will put the original footer to prevent corruption
(TODO: find the last footer in case of corrupted file).
