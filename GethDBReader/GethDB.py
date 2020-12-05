"""
Implementation for GethDB class that provides single APIs that automatically
looking for data from both the LevelDB and the FreezerDB.

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

from . import GethLevelDB
from . import GethFreezerDB

class GethDB:
	"""
	A manager used to automatically
	looking for data from both the LevelDB and the FreezerDB.
	"""

	def __init__(self, leveldbPath, freezerdbPath = None):
		"""
		Construct the GethDB object.

		Args:
			leveldbPath   : path to the LevelDB.
			freezerdbPath : path to the FreezerDB. Default to `None`. If `None`
			                is given, it will use the default path, which is the
							`ancient` directory under the `leveldbPath`.
		"""

		self.leveldbPath = os.path.abspath(leveldbPath)

		if freezerdbPath is None:
			self.freezerdbPath = os.path.join(self.leveldbPath, 'ancient')
		else:
			self.freezerdbPath = os.path.abspath(freezerdbPath)

		self.leveldb   = GethLevelDB.GethLevelDB(self.leveldbPath)
		self.freezerdb = GethFreezerDB.GethFreezerDB(self.freezerdbPath)

	def AncientBlockCount(self):
		"""
		Get the number of blocks stored in the ancient database.

		Returns:
			The number of blocks stored in the ancient database.
		"""

		return self.freezerdb.GetBlockCount()

	def GetHeaderHash(self, num):
		"""
		Get the hash of the block header specified by the block number.

		Args:
			num: A integer, which specifies the block number.

		Returns:
			The hash in bytes.
		"""

		try:
			return self.freezerdb.GetHeaderHash(num)
		except KeyError:
			try:
				return self.leveldb.GetHeaderHash(num)
			except KeyError:
				raise KeyError('Both FreezerDB and LevelDB haven\'t stored the specified block.') from None

	def GetHeaderByNum(self, num):
		"""
		Get the header specified by the block number.

		Args:
			num: A integer, which specifies the block number.

		Returns:
			The header stored in :class:`EthHeader` object.
		"""

		try:
			return self.freezerdb.GetHeaderByNum(num)
		except KeyError:
			try:
				return self.leveldb.GetHeaderByNum(num)
			except KeyError:
				raise KeyError('Both FreezerDB and LevelDB haven\'t stored the specified block.') from None
