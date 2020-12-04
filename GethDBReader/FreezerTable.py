"""
Implementation for FreezerTable class that access and read data from
the FreezerDB.

This code is re-written in Python by Haofan Zheng, and its original source is
authored by go-ethereum Authors and available at
https://github.com/ethereum/go-ethereum/blob/master/core/rawdb/freezer_table.go

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
import logging

from readerwriterlock import rwlock # pip install readerwriterlock
import snappy # pip install python-snappy

INDEX_ENTRY_SIZE = 6

class IndexEntry:
	"""
	A class holds the data for each entry in the index file.
	"""

	def __init__(self):
		"""
		Constructor that initializes values to zeros
		"""

		self.filenum = 0
		self.offset  = 0

	def UnmarshalBinary(self, b):
		"""
		Unmarshal the bytes for one entry from the index file, and store the
		value into this instance.

		Args:
			b: the bytes for one entry.
		"""

		self.filenum = int.from_bytes(b[:2] , byteorder='big')
		self.offset  = int.from_bytes(b[2:6], byteorder='big')

	def MarshallBinary(self):
		"""
		Marshall the value for one entry stored in this instance, and return
		the bytes that can then be written to the index file.

		Returns:
			The bytes that can then be written to the index file.
		"""

		return self.filenum.to_bytes(2, byteorder='big') + self.offset.to_bytes(4, byteorder='big')

def OpenFreezerFileForAppend(filename):
	"""
	A function used internally to open the Freezer table files for append.

	args:
		filename: The name of the file / path of the file to open

	Returns:
		The file object, opened by the python built-in ``open`` function.
	"""

	file = open(filename, 'a+b', opener=
		(lambda fName, modeNotUsed : os.open(fName, os.O_RDWR|os.O_CREAT, mode=0o644))) # |os.O_BINARY

	stat = os.fstat(file.fileno())
	file.seek(stat.st_size)

	return file

def OpenFreezerFileForReadOnly(filename):
	"""
	A function used internally to open the Freezer table files for read only.

	args:
		filename: The name of the file / path of the file to open

	Returns:
		The file object, opened by the python built-in ``open`` function.
	"""

	return open(filename, 'rb')

def OpenFreezerFileTruncated(filename):
	"""
	A function used internally to open and truncate the Freezer table files.

	args:
		filename: The name of the file / path of the file to open

	Returns:
		The file object, opened by the python built-in ``open`` function.
	"""

	return open(filename, 'w+b', opener=
		(lambda fName, modeNotUsed : os.open(fName, os.O_RDWR|os.O_CREAT|os.O_TRUNC, mode=0o644))) # |os.O_BINARY

class FreezerTable:
	"""
	Class that manages one FreezerDB *table*.
	"""

	def __init__(self,

		noCompression,
		maxFileSize,
		name,
		path,

		files,
		index,
		# readMeter,
		# writeMeter,
		# sizeGauge,

		logger,

		items = None,

		head = None,
		headId = None,
		tailId = None,
		itemOffset = None,

		headBytes = None
		):

		"""
		Constructs a :class:`FreezerTable` object. Normally, this is called
		internally.

		Args:
			noCompression: A boolean, indicates whether or not the table data is compressed.
			maxFileSize  : A Integer, indicates the maximum size of a table blob file.
			name         : A string,  specifies the name of the table.
			path         : A string,  specifies the path to the folder that stores the Freezer table files.
			files        : A dict,    maps from the file number to the opened file.
			index        : A file,    opened index file.
			logger       : A logger,  got from python built-in ``logging`` module.
			items        : Internal state, defaults to `None`.
			head         : Internal state, defaults to `None`.
			headId       : Internal state, defaults to `None`.
			tailId       : Internal state, defaults to `None`.
			itemOffset   : Internal state, defaults to `None`.
			headBytes    : Internal state, defaults to `None`.
		"""

		self.items = items

		self.noCompression = noCompression
		self.maxFileSize = maxFileSize
		self.name = name
		self.path = path

		self.head = head
		self.files = files
		self.headId = headId
		self.tailId = tailId
		self.index = index

		self.itemOffset = itemOffset

		self.headBytes = headBytes
		# self.readMeter = readMeter
		# self.writeMeter = writeMeter
		# self.sizeGauge = sizeGauge

		self.logger = logger
		self.lock = rwlock.RWLockRead()

	def OpenFile(self, num, opener) :

		if num not in self.files:

			if self.noCompression:
				name = '{name}.{idx:04d}.rdat'.format(name=self.name, idx=num)
			else:
				name = '{name}.{idx:04d}.cdat'.format(name=self.name, idx=num)

			file = opener(os.path.join(self.path, name))

			self.files[num] = file

			return file
		else:
			return self.files[num]

	def Close(self) :

		with self.lock.gen_wlock():
			self.index.close()
			self.index = None

			exps = []
			for num, file in self.files.items():
				try:
					file.close()
				except Exception as e:
					exps.append(e)
			self.files = {}

			self.head = None

			if len(exps) > 0:
				raise exps[0]

	def __del__(self):
		self.Close()

	# releaseFile closes a file, and removes it from the open file cache.
	# Assumes that the caller holds the write lock
	def ReleaseFile(self, num):
		file = self.files.pop(num, None)
		if file is not None:
			file.close()

	# releaseFilesAfter closes all open files with a higher number, and optionally also deletes the files
	def ReleaseFilesAfter(self, num, remove):
		toDel = []

		for fnum, file in self.files.items():
			if fnum > num:
				file.close()
				if remove:
					os.remove(os.path.realpath(file.name))
				toDel.append(fnum)

		for fnum in toDel:
			del self.files[fnum]

	# preopen opens all files that the freezer will need. This method should be called from an init-context,
	# since it assumes that it doesn't have to bother with locking
	# The rationale for doing preopen is to not have to do it from within Retrieve, thus not needing to ever
	# obtain a write-lock within Retrieve.
	def Preopen(self) :
		self.ReleaseFilesAfter(0, False)

		for i in range(self.tailId, self.headId):
			self.OpenFile(i, OpenFreezerFileForReadOnly)

		self.head = self.OpenFile(self.headId, OpenFreezerFileForAppend)

	# repair cross checks the head and the index file and truncates them to
	# be in sync with each other after a potential crash / data loss.
	def Repair(self):
		buffer = bytearray(INDEX_ENTRY_SIZE)

		stat = os.fstat(self.index.fileno())

		if stat.st_size == 0 :
			self.index.write(buffer)

		overflow = stat.st_size % INDEX_ENTRY_SIZE
		if overflow != 0 :
			TruncateFreezerFile(self.index, stat.st_size - overflow)

		stat = os.fstat(self.index.fileno())

		offsetsSize = stat.st_size

		firstIndex = IndexEntry()
		lastIndex  = IndexEntry()

		self.index.seek(0)
		buffer = self.index.read(INDEX_ENTRY_SIZE)
		firstIndex.UnmarshalBinary(buffer)

		self.tailId = firstIndex.filenum
		self.itemOffset = firstIndex.offset

		self.index.seek(offsetsSize - INDEX_ENTRY_SIZE)
		buffer = self.index.read(INDEX_ENTRY_SIZE)
		lastIndex.UnmarshalBinary(buffer)
		self.head = self.OpenFile(lastIndex.filenum, OpenFreezerFileForAppend)

		stat = os.fstat(self.head.fileno())

		contentSize = stat.st_size

		contentExp = int(lastIndex.offset)

		while contentExp != contentSize :

			if contentExp < contentSize :
				self.logger.warning("Truncating dangling head" + " indexed " + str(contentExp) + " stored " + str(contentSize))
				TruncateFreezerFile(self.head, contentExp)
				contentSize = contentExp

			if contentExp > contentSize :
				self.logger.warning("Truncating dangling indexes" + " indexed " + str(contentExp) + " stored " + str(contentSize))
				TruncateFreezerFile(self.index, offsetsSize - INDEX_ENTRY_SIZE)
				offsetsSize -= INDEX_ENTRY_SIZE
				self.index.ReadAt(buffer, offsetsSize - INDEX_ENTRY_SIZE)
				newLastIndex = IndexEntry()
				newLastIndex.UnmarshalBinary(buffer)

				if newLastIndex.filenum != lastIndex.filenum :

					self.releaseFile(lastIndex.filenum)
					self.head = self.OpenFile(newLastIndex.filenum, OpenFreezerFileForAppend)
					stat = os.fstat(self.head.fileno())

					contentSize = stat.st_size

				lastIndex = newLastIndex
				contentExp = int(lastIndex.offset)

		self.index.flush()

		self.head.flush()

		self.items = int(abs(self.itemOffset) + abs(offsetsSize / INDEX_ENTRY_SIZE - 1))
		self.headBytes = int(abs(contentSize))
		self.headId = lastIndex.filenum

		self.Preopen()

		self.logger.debug("Chain freezer table opened" + " items " + str(self.items) + " size " + str(self.headBytes))

	# getBounds returns the indexes for the item
	# returns start, end, filenumber and error
	def GetBounds(self, item):
		buffer = bytearray(INDEX_ENTRY_SIZE)

		startIdx = IndexEntry()
		endIdx = IndexEntry()

		self.index.seek((item + 1) * INDEX_ENTRY_SIZE)
		buffer = self.index.read(INDEX_ENTRY_SIZE)

		endIdx.UnmarshalBinary(buffer)

		if item != 0:
			self.index.seek(item * INDEX_ENTRY_SIZE)
			buffer = self.index.read(INDEX_ENTRY_SIZE)

			startIdx.UnmarshalBinary(buffer)
		else:
			return 0, endIdx.offset, endIdx.filenum

		if startIdx.filenum != endIdx.filenum:
			return 0, endIdx.offset, endIdx.filenum

		return startIdx.offset, endIdx.offset, endIdx.filenum

	def Retrieve(self, item):
		"""
		Retrieve one piece of data from the table.

		Args:
			item: A integer, which specifies the index of data to be retrieved.

		Returns:
			Bytes retrieved from the table.

		Raises:
			RuntimeError: when failed to retrieve data.
		"""

		with self.lock.gen_rlock():

			if (self.index is None) or (self.head is None):
				raise RuntimeError('The table or the item is inaccessible')

			if self.items <= item: #atomic.load(self.items)
				raise RuntimeError('The item number is out of bounds')

			if self.itemOffset > item:
				raise RuntimeError('The item offset number is out of bounds')

			startOffset, endOffset, filenum = self.GetBounds(item - self.itemOffset)

			dataFile = self.files.get(filenum, None)
			if dataFile is None:
				raise RuntimeError('missing data file {filenum}'.format(filenum=filenum))

			dataFile.seek(startOffset)
			blob = dataFile.read(endOffset - startOffset)

		# self.readMeter.Mark(len(blob) + 2 * INDEX_ENTRY_SIZE)

		if self.noCompression:
			return blob

		return snappy.uncompress(blob)

	def Has(self, number):
		"""
		Query if the data with the specified index number exist in the table.

		Args:
			number: A integer, which is the index number.

		Returns:
			True if it exist, otherwise, false.
		"""

		return self.items > number #atomic.load(self.items)

	def Size(self):
		"""
		Get the total size of the data stored in the Freezer table.

		Returns:
			A integer that indicates the size.
		"""

		with self.lock.gen_rlock():
			res = t.SizeNolock()
			return res

	# sizeNolock returns the total data size in the freezer table without obtaining
	# the mutex first.
	def SizeNolock(self):
		stat = os.fstat(self.index.fileno())
		total = self.maxFileSize * (self.headId - self.tailId) + (self.headBytes) + (stat.st_size)
		return total

	# Sync pushes any pending data from memory out to disk. This is an expensive
	# operation, so use it with care.
	def Sync(self):
		self.index.flush()
		self.head.flush()

	def printIndex(self):
		"""
		Print top 100 entries stored in the index file.
		"""
		buf = bytearray(INDEX_ENTRY_SIZE)

		print('|-----------------|')
		print('| fileno | offset |')
		print('|--------+--------|')

		for i in range(0, 102):
			self.index.seek(i * INDEX_ENTRY_SIZE)
			buf = self.index.read(INDEX_ENTRY_SIZE)

			entry = IndexEntry()
			entry.UnmarshalBinary(buf)
			print('|  {:03d}   |  {:03d}   | '.format(entry.filenum, entry.offset))
			if i > 100:
				print(' ... ')
				break

		print('|-----------------|')

def NewCustomTable(path, name, readMeter, writeMeter, sizeGauge, maxFilesize, noCompression):
	"""
	Open or create a FreezerDB table with customized maximum file size.
	This function is usually used internally.

	Args:
		path         : A string,  specifies the path to the folder that stores the Freezer table files.
		name         : A string,  specifies the name of the table.
		readMeter    : Not in used.
		writeMeter   : Not in used.
		sizeGauge    : Not in used.
		maxFilesize  : A Integer, indicates the maximum size of a table blob file.
		noCompression: A boolean, indicates whether or not the table data is compressed.
	"""

	os.makedirs(path, mode=0o755, exist_ok=True)

	idxName = '{name}.cidx'.format(name=name)
	if noCompression:
		idxName = '{name}.ridx'.format(name=name)

	offsets = OpenFreezerFileForAppend(os.path.join(path, idxName))

	tab = FreezerTable(
		index =        offsets,
		files =        {},
		# readMeter  =   readMeter,
		# writeMeter =   writeMeter,
		# sizeGauge  =   sizeGauge,
		name   =       name,
		path   =       path,
		logger =       logging.getLogger('FreezerTable:' + str(path) + ";Table:" + str(name)),
		noCompression= noCompression,
		maxFileSize =  maxFilesize,
	)

	tab.Repair()

	size = tab.SizeNolock()

	# tab.sizeGauge.Inc(int(size))

	return tab

def NewTable(path, name, readMeter, writeMeter, sizeGauge, disableSnappy):
	"""
	Open or create a FreezerDB table. The **preferred** way to create or open the
	FreezerDB table.

	Args:
		path         : A string,  specifies the path to the folder that stores the Freezer table files.
		name         : A string,  specifies the name of the table.
		readMeter    : Not in used.
		writeMeter   : Not in used.
		sizeGauge    : Not in used.
		disableSnappy: A boolean, indicates whether or not the table data is compressed.
	"""

	return NewCustomTable(path, name, readMeter, writeMeter, sizeGauge, 2*1000*1000*1000, disableSnappy)
