"""
Implementation for EthHeader class that provides interfaces to
parse the block header encoded in RLP, and
access inner fields of the block header.

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

import rlp # pip install rlp
from rlp.sedes import big_endian_int, binary

class EthHeader(rlp.Serializable):
	"""
	A class used to deserialize and hold the content of RLP data.
	"""

	fields = [
		('ParentHash' , binary),  #1
		('UncleHash'  , binary),  #2
		('Coinbase'   , binary),  #3
		('Root'       , binary),  #4
		('TxHash'     , binary),  #5
		('ReceiptHash', binary),  #6
		('Bloom'      , binary),  #7
		('Difficulty' , big_endian_int),  #8
		('Number'     , big_endian_int),  #9
		('GasLimit'   , big_endian_int),  #10
		('GasUsed'    , big_endian_int),  #11
		('Time'       , big_endian_int),  #12
		('Extra'      , binary),  #13
		('MixDigest'  , binary),  #14
		('Nonce'      , binary),  #15
	]

	def __repr__(self):

		s = '{\n'

		# 9
		s += '\t'
		s += 'Number      = ' + str(self.Number)
		s += '\n'

		# 12
		s += '\t'
		s += 'Time        = ' + str(self.Time)
		s += '\n'

		# 8
		s += '\t'
		s += 'Difficulty  = ' + str(self.Difficulty)
		s += '\n'

		# 10
		s += '\t'
		s += 'GasLimit    = ' + str(self.GasLimit)
		s += '\n'

		# 11
		s += '\t'
		s += 'GasUsed     = ' + str(self.GasUsed)
		s += '\n'

		# 1
		s += '\t'
		s += 'ParentHash  = ' + '0x' + str(self.ParentHash.hex())
		s += '\n'

		# 2
		s += '\t'
		s += 'UncleHash   = ' + '0x' + str(self.UncleHash.hex())
		s += '\n'

		# 3
		s += '\t'
		s += 'Coinbase    = ' + '0x' + str(self.Coinbase.hex())
		s += '\n'

		# 4
		s += '\t'
		s += 'Root        = ' + '0x' + str(self.Root.hex())
		s += '\n'

		# 5
		s += '\t'
		s += 'TxHash      = ' + '0x' + str(self.TxHash.hex())
		s += '\n'

		# 6
		s += '\t'
		s += 'ReceiptHash = ' + '0x' + str(self.ReceiptHash.hex())
		s += '\n'

		# 7
		s += '\t'
		s += 'Bloom       = ' + '0x' + str(self.Bloom.hex())
		s += '\n'

		# 13
		s += '\t'
		s += 'Extra       = ' + '0x' + str(self.Extra.hex())
		s += '\n'

		# 14
		s += '\t'
		s += 'MixDigest   = ' + '0x' + str(self.MixDigest.hex())
		s += '\n'

		# 15
		s += '\t'
		s += 'Nonce       = ' + '0x' + str(self.Nonce.hex())
		s += '\n'

		s+= '}'

		return s

def FromBytes(b):
	"""
	Construct a EthHeader class object by deserializing the RLP encoded bytes.

	Args:
		b: RLP encoded bytes.

	Returns:
		A EthHeader object.
	"""

	return rlp.decode(b, EthHeader)
