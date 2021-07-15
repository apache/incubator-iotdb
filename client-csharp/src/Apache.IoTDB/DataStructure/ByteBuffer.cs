/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
 
using System;
using System.Linq;
using System.Text;

namespace Apache.IoTDB.DataStructure
{
    public class ByteBuffer
    {
        private byte[] _buffer;
        private int _writePos;
        private int _readPos;
        private int _totalLength;
        private readonly bool _isLittleEndian = BitConverter.IsLittleEndian;

        public ByteBuffer(byte[] buffer)
        {
            _buffer = buffer;
            _readPos = 0;
            _writePos = buffer.Length;
            _totalLength = buffer.Length;
        }

        public ByteBuffer(int reserve = 1)
        {
            _buffer = new byte[reserve];
            _writePos = 0;
            _readPos = 0;
            _totalLength = reserve;
        }

        public bool has_remaining()
        {
            return _readPos < _writePos;
        }

        // these for read
        public byte get_byte()
        {
            var byteVal = _buffer[_readPos];
            _readPos += 1;
            return byteVal;
        }

        public bool get_bool()
        {
            var boolValue = BitConverter.ToBoolean(_buffer, _readPos);
            _readPos += 1;
            return boolValue;
        }

        public int get_int()
        {
            var intBuff = _buffer[_readPos..(_readPos + 4)];
            
            if (_isLittleEndian) intBuff = intBuff.Reverse().ToArray();

            var intValue = BitConverter.ToInt32(intBuff);
            _readPos += 4;
            return intValue;
        }

        public long get_long()
        {
            var longBuff = _buffer[_readPos..(_readPos + 8)];
            
            if (_isLittleEndian) longBuff = longBuff.Reverse().ToArray();

            var longValue = BitConverter.ToInt64(longBuff);
            _readPos += 8;
            return longValue;
        }

        public float get_float()
        {
            var floatBuff = _buffer[_readPos..(_readPos + 4)];
            
            if (_isLittleEndian) floatBuff = floatBuff.Reverse().ToArray();

            var floatValue = BitConverter.ToSingle(floatBuff);
            _readPos += 4;
            return floatValue;
        }

        public double get_double()
        {
            var doubleBuff = _buffer[_readPos..(_readPos + 8)];
            
            if (_isLittleEndian) doubleBuff = doubleBuff.Reverse().ToArray();

            var doubleValue = BitConverter.ToDouble(doubleBuff);
            _readPos += 8;
            return doubleValue;
        }

        public string get_str()
        {
            var length = get_int();
            var strBuff = _buffer[_readPos..(_readPos + length)];
            var strValue = Encoding.UTF8.GetString(strBuff);
            _readPos += length;
            return strValue;
        }

        public byte[] get_buffer()
        {
            return _buffer[.._writePos];
        }

        private void extend_buffer(int spaceNeed)
        {
            if (_writePos + spaceNeed >= _totalLength)
            {
                _totalLength = Math.Max(spaceNeed, _totalLength);
                var newBuffer = new byte[_totalLength * 2];
                _buffer.CopyTo(newBuffer, 0);
                _buffer = newBuffer;
                _totalLength = 2 * _totalLength;
            }
        }

        // these for write
        public void add_bool(bool value)
        {
            var boolBuffer = BitConverter.GetBytes(value);
            
            if (_isLittleEndian) boolBuffer = boolBuffer.Reverse().ToArray();

            extend_buffer(boolBuffer.Length);
            boolBuffer.CopyTo(_buffer, _writePos);
            _writePos += boolBuffer.Length;
        }

        public void add_int(int value)
        {
            var intBuff = BitConverter.GetBytes(value);
            
            if (_isLittleEndian) intBuff = intBuff.Reverse().ToArray();

            extend_buffer(intBuff.Length);
            intBuff.CopyTo(_buffer, _writePos);
            _writePos += intBuff.Length;
        }

        public void add_long(long value)
        {
            var longBuff = BitConverter.GetBytes(value);
            
            if (_isLittleEndian) longBuff = longBuff.Reverse().ToArray();

            extend_buffer(longBuff.Length);
            longBuff.CopyTo(_buffer, _writePos);
            _writePos += longBuff.Length;
        }

        public void add_float(float value)
        {
            var floatBuff = BitConverter.GetBytes(value);
            
            if (_isLittleEndian) floatBuff = floatBuff.Reverse().ToArray();

            extend_buffer(floatBuff.Length);
            floatBuff.CopyTo(_buffer, _writePos);
            _writePos += floatBuff.Length;
        }

        public void add_double(double value)
        {
            var doubleBuff = BitConverter.GetBytes(value);
            
            if (_isLittleEndian) doubleBuff = doubleBuff.Reverse().ToArray();

            extend_buffer(doubleBuff.Length);
            doubleBuff.CopyTo(_buffer, _writePos);
            _writePos += doubleBuff.Length;
        }

        public void add_str(string value)
        {
            add_int(value.Length);
            
            var strBuf = Encoding.UTF8.GetBytes(value);

            extend_buffer(strBuf.Length);
            strBuf.CopyTo(_buffer, _writePos);
            _writePos += strBuf.Length;
        }

        public void add_char(char value)
        {
            var charBuf = BitConverter.GetBytes(value);
            
            if (_isLittleEndian) charBuf = charBuf.Reverse().ToArray();

            extend_buffer(charBuf.Length);
            charBuf.CopyTo(_buffer, _writePos);
            _writePos += charBuf.Length;
        }
        public void add_byte(byte value){
            extend_buffer(1);
            _buffer[_writePos] = value;
            _writePos += 1;
        }
    }
}