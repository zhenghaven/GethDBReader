"""
Implementation for GethLevelDB class that provides unified APIs for requesting
block headers from the LevelDB.

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

import os
import sys

import leveldb # pip install leveldb

from . import EthHeader

BLOCK_NUM_LEN = 8
BLOCK_KEY_PREFIX = bytearray(b'h');
BLOCK_NUM_SUFFIX = bytearray(b'n');

def ParseBlockNumKey(b):
	"""
	Internally used function.
	"""

	numBytes = b[1:9]
	num = int.from_bytes(numBytes, byteorder='big')
	return [num, '0x' + numBytes.hex()]

def ParseBlockHeaderKey(b):
	"""
	Internally used function.
	"""

	numBytes = b[1:9]
	hashBytes = b[9:41]
	num = int.from_bytes(numBytes, byteorder='big')
	return [num, '0x' + numBytes.hex(), '0x' + hashBytes.hex()]

def WriteBlockNumKey(f, i, b):
	"""
	Internally used function.
	"""

	parsed = ParseBlockNumKey(b)

	f.write(str(i))
	f.write(',')
	f.write('0x' + b.hex())
	f.write(',')
	f.write(str(parsed[0]))
	f.write(',')
	f.write(parsed[1])
	f.write('\n')

def WriteBlockHeaderKey(f, i, b):
	"""
	Internally used function.
	"""

	parsed = ParseBlockHeaderKey(b)

	f.write(str(i))
	f.write(',')
	f.write('0x' + b.hex())
	f.write(',')
	f.write(str(parsed[0]))
	f.write(',')
	f.write(parsed[1])
	f.write(',')
	f.write(parsed[2])
	f.write('\n')

class GethLevelDB:
	"""
	A manager used to access content of Geth's LevelDB.
	Currently, in only supports read operations on block headers.
	"""

	def __init__(self, path):
		"""
		Constructs the manager, and opens all necessary tables.

		Args:
			path: A string, which specifies the path to the directory that stores all the LevelDB files.
		"""

		self.path = path

		self.db = leveldb.LevelDB(path)

	def FindAndWriteAllHashes(self, blockNumOutPath, blockHeaderOutPath):
		"""
		Iterate through all entries in the LevelDB, to find what block numbers
		and block headers are existing in the LevelDB, and output the results to
		CSV files.

		Args:
			blockNumOutPath   : The output path for the CSV file that stores result for block numbers.
			blockHeaderOutPath: The output path for the CSV file that stores result for block headers.
		"""

		print('INFO: Searching for all existing block numbers and block headers, and output to CSV files.')

		blockNumFile = None
		BlockHeaderFile = None

		try:
			blockNumFile    = open(self.csvBlockNumPath, 'w')
			blockNumFile.write('Index')
			blockNumFile.write(',')
			blockNumFile.write('Key')
			blockNumFile.write(',')
			blockNumFile.write('BlockNum')
			blockNumFile.write(',')
			blockNumFile.write('BlockNumHex')
			blockNumFile.write('\n')
			blockNumFile.flush()
		except:
			print('INFO: Searching for block numbers is disabled.')
			blockNumFile = None

		try:
			BlockHeaderFile = open(self.csvBlockHeaPath, 'w')
			BlockHeaderFile.write('Index')
			BlockHeaderFile.write(',')
			BlockHeaderFile.write('Key')
			BlockHeaderFile.write(',')
			BlockHeaderFile.write('BlockNum')
			BlockHeaderFile.write(',')
			BlockHeaderFile.write('BlockNumHex')
			BlockHeaderFile.write(',')
			BlockHeaderFile.write('HashHex')
			BlockHeaderFile.write('\n')
			BlockHeaderFile.flush()
		except:
			print('INFO: Searching for block headers is disabled.')
			BlockHeaderFile = None

		if blockNumFile is None and BlockHeaderFile is None:
			print('WARNING: Nothing to generate. Exit.')
			return

		try:
			i = 0
			found = False

			for key in self.db.RangeIter(include_value = False):
				# print(i, '\t', key, '\t', len(key))
				i = i + 1
				# if i > 15:
				# 	break
				if i % 100000 == 0:
					if found:
						sys.stdout.write('*')
						found = False
					else:
						sys.stdout.write('.')
					sys.stdout.flush()

				if ((blockNumFile is not None) and
					len(key) == 10 and
					key[0] == BLOCK_KEY_PREFIX[0] and
					key[9] == BLOCK_NUM_SUFFIX[0]):

					found = True
					WriteBlockNumKey(blockNumFile, i, key)

				elif ((BlockHeaderFile is not None) and
					len(key) == 41 and
					key[0] == BLOCK_KEY_PREFIX[0]):

					found = True
					WriteBlockHeaderKey(BlockHeaderFile, i, key)

			print('INFO: Total number of keys in DB = ', i)
			print()
		finally:
			blockNumFile.close()
			BlockHeaderFile.close()

	def GetHeaderHash(self, num):
		"""
		Get the hash of the block header specified by the block number.

		Args:
			num: A integer, which specifies the block number.

		Returns:
			The hash in bytes.
		"""

		numByte = bytearray(num.to_bytes(BLOCK_NUM_LEN, byteorder='big'))
		numKey = BLOCK_KEY_PREFIX + numByte + BLOCK_NUM_SUFFIX

		return bytes(self.db.Get(numKey))

	def GetHeaderByNumHash(self, num, h):
		"""
		Get the header specified by the block number and hash.
		This function is usually used internally.
		:function:`GetHeaderByNum` will find the hash and then call this function.

		Args:
			num: A integer, which specifies the block number.
			h  : Bytes, which is the hash of the block header.

		Returns:
			The header stored in :class:`EthHeader` object.
		"""

		numByte = bytearray(num.to_bytes(BLOCK_NUM_LEN, byteorder='big'))
		headerKey = BLOCK_KEY_PREFIX + numByte + h

		return EthHeader.FromBytes(self.db.Get(headerKey))

	def GetHeaderByNum(self, num):
		"""
		Get the header specified by the block number.

		Args:
			num: A integer, which specifies the block number.

		Returns:
			The header stored in :class:`EthHeader` object.
		"""

		h = self.GetHeaderHash(num)

		return self.GetHeaderByNumHash(num, h)
