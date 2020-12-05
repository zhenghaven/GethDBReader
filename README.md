# GethDBReader

Requesting large number of blocks at once from Geth client
could cause significant overhead to a program.
This Python library helps reading blocks directly from Geth's
internal databases, including Level DB and Freezer DB.

## Requirements

This library has been run and tested under the following requirements:

- Python 3.8
- Python 3rd-party modules (installed with `python3 -m pip install`):
	- rlp
	- web3
	- readerwriterlock
	- python-snappy
	- leveldb

## API Quickstart

Older blocks are stored in the append-only database - FreezerDB,
while recent blocks are stored in the key-value database - LevelDB.

If the block number of the block needed isn't in the FreezerDB,
it's probably stored in the LevelDB.

FreezerDB's directory is typically the one named `ancient` under the LevelDB's
directory.

### GethDB

This is the single interface that helps looking for the block from both
DB automatically.

```python
#! /usr/bin/python3

from GethDBReader import GethDB

# Construct the DB by giving the path to the DB
db = GethDB.GethDB('<Path to the LevelDB>', '<Path to the FreezerDB>(optional)')

# Get Eth header by giving its block number
print(db.GetHeaderByNum(0))
```

If a more specific access to one of the DB is needed, the following modules can
be used.

### GethLevelDB

```python
#! /usr/bin/python3

from GethDBReader import GethLevelDB

# Construct the DB by giving the path to the DB
db = GethLevelDB.GethLevelDB('<Path to the LevelDB>')

# Get Eth header by giving its block number
print(db.GetHeaderByNum(0))
```

### GethFreezerDB

```python
#! /usr/bin/python3

from GethDBReader import GethFreezerDB

# Construct the DB by giving the path to the DB
db = GethFreezerDB.GethFreezerDB('<Path to the FreezerDB>')

# Get Eth header by giving its block number
print(db.GetHeaderByNum(0))
```
