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
namespace iotdb_client_csharp.client.utils
{
    public class ByteBuffer
    {
        private byte[] buffer;
        private int write_pos = 0, read_pos = 0;
        private int total_length;
        private bool is_little_endian;
        
        public ByteBuffer(byte[] buffer){   
            this.buffer = buffer;
            this.read_pos = 0;
            this.write_pos = buffer.Length;
            this.total_length = buffer.Length;
            this.is_little_endian = BitConverter.IsLittleEndian;
        }
        public ByteBuffer(int reserve = 1){
            this.buffer = new byte[reserve];
            this.write_pos = 0;
            this.read_pos = 0;
            this.total_length = reserve;
            this.is_little_endian = BitConverter.IsLittleEndian;
        }
        public bool has_remaining(){
            return read_pos < this.write_pos;
        }
        // these for read
        public byte get_byte(){
            var byte_val = buffer[read_pos];
            read_pos += 1;
            return byte_val;
        }
        public bool get_bool(){
            bool bool_value = BitConverter.ToBoolean(buffer, read_pos);
            read_pos += 1;
            return bool_value;
        }
        public int get_int(){

            var int_buff = buffer[read_pos..(read_pos+4)];
            if(is_little_endian){
                int_buff = int_buff.Reverse().ToArray();
            }
            int int_value = BitConverter.ToInt32(int_buff);
            read_pos += 4;
            return int_value;
        }
        public long get_long(){
            var long_buff = buffer[read_pos..(read_pos + 8)];
            if(is_little_endian){
                long_buff = long_buff.Reverse().ToArray();
            }
            long long_value = BitConverter.ToInt64(long_buff);
            read_pos += 8;
            return long_value;
        }
        public float get_float(){
            var float_buff = buffer[read_pos..(read_pos +4)];
            if(is_little_endian){
                float_buff = float_buff.Reverse().ToArray();
            }
            float float_value = BitConverter.ToSingle(float_buff);
            read_pos += 4;
            return float_value;
        }
        public double get_double(){
            var double_buff = buffer[read_pos..(read_pos+8)];
            if(is_little_endian){
                double_buff = double_buff.Reverse().ToArray();
            }
            double double_value = BitConverter.ToDouble(double_buff);
            read_pos += 8;
            return double_value;
        }
        public string get_str(){
            int length = get_int();
            var str_buff = buffer[read_pos..(read_pos+length)];
            string str_value = System.Text.Encoding.UTF8.GetString(str_buff);
            read_pos += length;
            return str_value;
        }
        public byte[] get_buffer(){
            return buffer[0..this.write_pos];
        }
        private int max(int a, int b){
            if(a <= b){
                return b;
            }
            return a;
        }
        private void extend_buffer(int space_need){
            if(write_pos + space_need >= total_length){
                total_length = max(space_need, total_length);
                byte[] new_buffer = new byte[total_length * 2];
                buffer.CopyTo(new_buffer, 0);
                buffer = new_buffer;
                total_length = 2 * total_length;
            }
        }
        // these for write
        public void add_bool(bool value){
            var bool_buffer = BitConverter.GetBytes(value);
            if(is_little_endian){
                bool_buffer = bool_buffer.Reverse().ToArray();
            }

            extend_buffer(bool_buffer.Length);
            bool_buffer.CopyTo(buffer, write_pos);
            write_pos += bool_buffer.Length;

        }
        public void add_int(Int32 value){
            var int_buff = BitConverter.GetBytes(value); 
            if(is_little_endian){
                int_buff = int_buff.Reverse().ToArray();
            }

            extend_buffer(int_buff.Length);
            int_buff.CopyTo(buffer, write_pos);
            write_pos += int_buff.Length;
        }
        public void add_long(long value){
            var long_buff = BitConverter.GetBytes(value);
            if(is_little_endian){
                long_buff = long_buff.Reverse().ToArray();
            }
            
            extend_buffer(long_buff.Length);
            long_buff.CopyTo(buffer, write_pos);
            write_pos += long_buff.Length;

        }
        public void add_float(float value){
            var float_buff = BitConverter.GetBytes(value);
            if(is_little_endian){
                float_buff = float_buff.Reverse().ToArray();
            }
            extend_buffer(float_buff.Length);
            float_buff.CopyTo(buffer, write_pos);
            write_pos += float_buff.Length;
        }
        public void add_double(double value){
            var double_buff = BitConverter.GetBytes(value);
            if(is_little_endian){
                double_buff = double_buff.Reverse().ToArray();
            }
            extend_buffer(double_buff.Length);
            double_buff.CopyTo(buffer, write_pos);
            write_pos += double_buff.Length;
        }
        public void add_str(string value){
            add_int(value.Length);
            var str_buf = System.Text.Encoding.UTF8.GetBytes(value);

            extend_buffer(str_buf.Length);
            str_buf.CopyTo(buffer, write_pos);
            write_pos += str_buf.Length;
        }
        public void add_char(char value){
           var char_buf = BitConverter.GetBytes(value);
           if(is_little_endian){
               char_buf = char_buf.Reverse().ToArray();
           }
           extend_buffer(char_buf.Length);
           char_buf.CopyTo(buffer, write_pos);
           write_pos += char_buf.Length;
        }

    }
    
}