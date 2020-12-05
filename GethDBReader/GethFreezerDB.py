"""
Implementation for GethFreezerDB class that provides unified APIs for requesting
block headers from the FreezerDB.

Copyright (C) 2020  Haofan Zheng

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""

import sys

from web3 import Web3 #pip install web3

from . import FreezerTable
from . import EthHeader

TABLE_HASHES  = 'hashes'
TABLE_HEADERS = 'headers'

TABLE_SCHEMA = {
	TABLE_HEADERS : [False],
	TABLE_HASHES : [True],
}

class GethFreezerDB:
	"""
	A manager used to access content of Geth's FreezerDB.
	Currently, in only supports read operations on block headers.
	"""

	def __init__(self, path):
		"""
		Constructs the manager, and opens all necessary tables.

		Args:
			path: A path, which specifies the path to the directory
			      that stores all the FreezerDB files.
		"""

		self.path = path
		self.tables = {}

		for tName, schema in TABLE_SCHEMA.items():
			self.tables[tName] = FreezerTable.NewTable(self.path, tName, None, None, None, schema[0])

	def VerifyHeaderHashes(self):
		"""
		Verifies the hashes stored in the hash table with the headers stored in
		the header table, to make sure they are match.
		*NOTE*: Only call this function when necessary. It will iterate through
		all entries in the FreezerDB, to it can take a long time.
		"""

		if self.tables[TABLE_HASHES].items != self.tables[TABLE_HEADERS].items:
			print('ERROR:', 'The number of items in "{}" table doesn\'t match the "{}" table'.format(TABLE_HASHES, TABLE_HEADERS))

		failed = 0

		for i in range(0, self.tables[TABLE_HASHES].items):

			if i % 10000 == 0:
				sys.stdout.write('.')
				sys.stdout.flush()

			headerHash = self.tables[TABLE_HASHES].Retrieve(i)
			header = self.tables[TABLE_HEADERS].Retrieve(i)
			if bytes(Web3.keccak(header)) != headerHash:
				print()
				print('WARNING:', 'Block number {}\'s header hash doesn\'t match the one stored in DB.'.format(i))
				failed += 1

			i += 1

			if failed > 100:
				print()
				print('ERROR:', 'Stop! Too much fails.')
				break

		print()
		print('INFO:', 'Verified {} blocks.'.format(i))
		print('INFO:', 'Where {} blocks failed.'.format(failed))

	def GetBlockCount(self):
		"""
		Get the number of blocks stored in the FreezerDB.

		Returns:
			The number of blocks stored in the FreezerDB.
		"""

		return self.tables[TABLE_HASHES].GetItemsCount()

	def GetHeaderHash(self, num):
		"""
		Get the hash of the block header specified by the block number.

		Args:
			num: A integer, which specifies the block number.

		Returns:
			The hash in bytes.
		"""

		return self.tables[TABLE_HASHES].Retrieve(num)

	def GetHeaderByNum(self, num):
		"""
		Get the header specified by the block number.

		Args:
			num: A integer, which specifies the block number.

		Returns:
			The header stored in :class:`EthHeader` object.
		"""

		return EthHeader.FromBytes(self.tables[TABLE_HEADERS].Retrieve(num))
