use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
#[derive(Clone, PartialEq)]
pub enum MessageValue {
    String(String),
    Bytes(Vec<u8>),
    TinyInt(i8),
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    Float(f64),
    Bool(bool),
    Structure(MessageStructure),
    Null,
    //TODO: Impl HashMap and Vec values (probably with generic types that impl clone)
}

#[derive(Clone, PartialEq)]
pub struct MessageStructure {
    tag: u8,
    fields: Vec<MessageValue>,
}

impl MessageStructure {
    fn new(tag: u8, fields: Vec<MessageValue>) -> MessageStructure {
        MessageStructure {
            tag: tag,
            fields: fields,
        }
    }
    fn __eq__(&self, other: &MessageStructure) -> bool {
        self.tag == other.tag && self.fields == other.fields
    }
    fn __ne__(&self, other: &MessageStructure) -> bool {
        self.tag != other.tag || self.fields != other.fields
    }
    fn __len__(&self) -> usize {
        self.fields.len()
    }
    fn __getitem__(&self, index: usize) -> MessageValue {
        self.fields[index].clone()
    }
    fn __setitem__(&mut self, index: usize, value: MessageValue) {
        self.fields[index] = value;
    }
}

struct MessageBuffer {
    buffer: Vec<u8>,
    cursor: usize,
}

impl MessageBuffer {
    fn new(capacity: usize) -> Self {
        MessageBuffer {
            buffer: vec![0; capacity],
            cursor: 0,
        }
    }

    fn try_write(&mut self, data: &[u8]) -> Result<usize, std::io::Error> {
        let len = self.buffer.len();
        let remaining = len - self.cursor;
        let written = data.len().min(remaining);
        self.buffer[self.cursor..self.cursor + written].copy_from_slice(&data[..written]);
        self.cursor += written;
        Ok(written)
    }
}

struct Packer {
    stream: MessageBuffer,
}

impl Packer {
    pub fn new(stream: MessageBuffer) -> Packer {
        Packer { stream: stream }
    }

    fn pack_raw(&mut self, data: &[u8]) -> Result<(), std::io::Error> {
        self.stream.try_write(data)?;
        Ok(())
    }

    fn pack_struct(&mut self, sig: u8, fields: Vec<MessageValue>) -> Result<(), std::io::Error> {
        let size = fields.len();
        match size {
            0x00 => {
                self.stream.try_write(b"\xB0")?;
            }
            0x01 => {
                self.stream.try_write(b"\xB1")?;
            }
            0x02 => {
                self.stream.try_write(b"\xB2")?;
            }
            0x03 => {
                self.stream.try_write(b"\xB3")?;
            }
            0x04 => {
                self.stream.try_write(b"\xB4")?;
            }
            0x05 => {
                self.stream.try_write(b"\xB5")?;
            }
            0x06 => {
                self.stream.try_write(b"\xB6")?;
            }
            0x07 => {
                self.stream.try_write(b"\xB7")?;
            }
            0x08 => {
                self.stream.try_write(b"\xB8")?;
            }
            0x09 => {
                self.stream.try_write(b"\xB9")?;
            }
            0x0A => {
                self.stream.try_write(b"\xBA")?;
            }
            0x0B => {
                self.stream.try_write(b"\xBB")?;
            }
            0x0C => {
                self.stream.try_write(b"\xBC")?;
            }
            0x0D => {
                self.stream.try_write(b"\xBD")?;
            }
            0x0E => {
                self.stream.try_write(b"\xBE")?;
            }
            0x0F => {
                self.stream.try_write(b"\xBF")?;
            }
            _ => {
                panic!("Struct size overflow");
            }
        }
        self.stream.try_write(&[sig])?;
        for field in fields {
            self.pack(field)?;
        }
        Ok(())
    }

    fn pack_string_header(&mut self, length: usize) -> Result<(), std::io::Error> {
        match length {
            0x00 => {
                self.stream.try_write(b"\x80")?;
            }
            0x01 => {
                self.stream.try_write(b"\x81")?;
            }
            0x02 => {
                self.stream.try_write(b"\x82")?;
            }
            0x03 => {
                self.stream.try_write(b"\x83")?;
            }
            0x04 => {
                self.stream.try_write(b"\x84")?;
            }
            0x05 => {
                self.stream.try_write(b"\x85")?;
            }
            0x06 => {
                self.stream.try_write(b"\x86")?;
            }
            0x07 => {
                self.stream.try_write(b"\x87")?;
            }
            0x08 => {
                self.stream.try_write(b"\x88")?;
            }
            0x09 => {
                self.stream.try_write(b"\x89")?;
            }
            0x0A => {
                self.stream.try_write(b"\x8A")?;
            }
            0x0B => {
                self.stream.try_write(b"\x8B")?;
            }
            0x0C => {
                self.stream.try_write(b"\x8C")?;
            }
            0x0D => {
                self.stream.try_write(b"\x8D")?;
            }
            0x0E => {
                self.stream.try_write(b"\x8E")?;
            }
            0x0F => {
                self.stream.try_write(b"\x8F")?;
            }
            0x00..=0xF => {
                self.stream.try_write(b"\xD0")?;
                self.stream.try_write(length.to_be_bytes().as_ref())?;
            }
            0x100..=0xFFFF => {
                self.stream.try_write(b"\xD1")?;
                self.stream.try_write(length.to_be_bytes().as_ref())?;
            }
            0x10000..=0xFFFFFFFF => {
                self.stream.try_write(b"\xD2")?;
                self.stream.try_write(length.to_be_bytes().as_ref())?;
            }
            _ => panic!("String header size overflow"),
        }
        Ok(())
    }

    fn pack_bytes_header(&mut self, length: usize) -> Result<(), std::io::Error> {
        match length {
            0x00..=0xFF => {
                self.stream.try_write(b"\xCC")?;
                self.stream.try_write(length.to_be_bytes().as_ref())?;
            }
            0x100..=0xFFFF => {
                self.stream.try_write(b"\xCD")?;
                self.stream.try_write(length.to_be_bytes().as_ref())?;
            }
            0x10000..=0xFFFFFFFF => {
                self.stream.try_write(b"\xCE")?;
                self.stream.try_write(length.to_be_bytes().as_ref())?;
            }
            _ => panic!("Bytes header size overflow"),
        }
        Ok(())
    }

    pub fn pack(&mut self, val: MessageValue) -> Result<(), std::io::Error> {
        match val {
            MessageValue::Null => {
                self.stream.try_write(b"\xC0")?;
            }
            MessageValue::Bool(val) => {
                if val {
                    self.stream.try_write(b"\xC3")?;
                } else {
                    self.stream.try_write(b"\xC2")?;
                }
            }
            MessageValue::Float(f) => {
                self.stream.try_write(b"\xC1")?;
                self.stream.try_write(f.to_be_bytes().as_ref())?;
            }
            MessageValue::TinyInt(i) => {
                if -0x10 <= i {
                    self.stream.try_write(i.to_be_bytes().as_ref())?;
                } else {
                    self.stream.try_write(b"\xC8")?;
                    self.stream.try_write(i.to_be_bytes().as_ref())?;
                }
            }
            MessageValue::SmallInt(i) => {
                self.stream.try_write(b"\xC9")?;
                self.stream.try_write(i.to_be_bytes().as_ref())?;
            }
            MessageValue::Int(i) => {
                self.stream.try_write(b"\xCA")?;
                self.stream.try_write(i.to_be_bytes().as_ref())?;
            }
            MessageValue::BigInt(i) => {
                self.stream.try_write(b"\xCB")?;
                self.stream.try_write(i.to_be_bytes().as_ref())?;
            }
            MessageValue::String(s) => {
                let bytes = s.as_bytes();
                let length = bytes.len();
                self.pack_string_header(length)?;
                self.stream.try_write(bytes)?;
            }
            MessageValue::Bytes(b) => {
                let length = b.len();
                self.pack_bytes_header(length)?;
                self.stream.try_write(&b[..])?;
            }
            MessageValue::Structure(s) => self.pack_struct(s.tag, s.fields)?,
        }
        Ok(())
    }

    fn pack_end_of_stream(&mut self) -> Result<(), std::io::Error> {
        self.stream.try_write(b"\xDF")?;
        Ok(())
    }
}

struct UnpackableBuffer {
    buffer: Vec<u8>,
    used: usize,
    pos: usize,
}

impl UnpackableBuffer {
    fn new(buffer: Option<Vec<u8>>) -> Self {
        match buffer {
            Some(buffer) => {
                let len = buffer.len();
                Self {
                    buffer,
                    used: len,
                    pos: 0,
                }
            }
            None => Self {
                buffer: Vec::with_capacity(8192),
                used: 0,
                pos: 0,
            },
        }
    }
    fn reset(&mut self) {
        self.used = 0;
        self.pos = 0;
    }

    fn read(&mut self, n: usize) -> Result<&[u8], std::io::Error> {
        if self.pos + n > self.buffer.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "unpackable buffer exhausted",
            ));
        }
        let result = &self.buffer[self.pos..self.pos + n];
        self.pos += n;
        Ok(result)
    }

    fn read_u8(&mut self) -> Result<u8, std::io::Error> {
        let result = self.buffer[self.pos];
        self.pos += 1;
        Ok(result)
    }

    fn pop_u16(&mut self) -> u16 {
        if self.used >= 2 {
            let result =
                u16::from_be_bytes([self.buffer[self.used - 2], self.buffer[self.used - 1]]);
            self.used -= 2;
            result
        } else {
            return 0;
        }
    }
    fn resize_buffer(&mut self, new_size: usize) {
        let mut new_buffer = vec![0; new_size];
        new_buffer.copy_from_slice(&self.buffer[..self.used]);
        self.buffer = new_buffer;
        self.used = self.buffer.len();
    }

    fn receive(&mut self, sock: &mut ReadHalf, n_bytes: usize) -> Result<(), std::io::Error> {
        let end = self.used + n_bytes;
        if end > self.buffer.len() {
            self.resize_buffer(end);
        }
        while self.used < end {
            let n = sock.try_read(&mut self.buffer[self.used..end])?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "buffer exhausted",
                ));
            }
            self.used += n;
        }
        Ok(())
    }
}

struct Unpacker {
    unpackable: UnpackableBuffer,
}

impl Unpacker {
    pub fn new(unpackable: UnpackableBuffer) -> Self {
        Self { unpackable }
    }
    pub fn reset(&mut self) {
        self.unpackable.reset();
    }
    pub fn read(&mut self, n: usize) -> Result<&[u8], std::io::Error> {
        return Ok(self.unpackable.read(n)?);
    }
    pub fn read_u8(&mut self) -> Result<u8, std::io::Error> {
        return Ok(self.unpackable.read_u8()?);
    }
    pub fn unpack(&mut self) -> Result<MessageValue, std::io::Error> {
        let marker = self.read_u8()?;
        let marker_high = marker & 0xF0;
        match marker {
            //null
            0xC0 => {
                return Ok(MessageValue::Null);
            }
            //bool
            0xC2 => {
                return Ok(MessageValue::Bool(false));
            }
            0xC3 => {
                return Ok(MessageValue::Bool(true));
            }
            //float
            0xC1 => {
                let val = self.read(8)?;
                let f = f64::from_be_bytes(val.try_into().unwrap());
                return Ok(MessageValue::Float(f));
            }
            // tiny int
            0x00..=0x7f => {
                return Ok(MessageValue::TinyInt(marker as i8));
            }
            0xF0..=0xFF => {
                return Ok(MessageValue::TinyInt(marker as i8));
            }
            0xc8 => {
                let val = self.read(1)?;
                let i = i8::from_be_bytes(val.try_into().unwrap());
                return Ok(MessageValue::TinyInt(i));
            }
            // small int
            0xC9 => {
                let val = self.read(2)?;
                let i = i16::from_be_bytes(val.try_into().unwrap());
                return Ok(MessageValue::SmallInt(i));
            }
            // int
            0xCA => {
                let val = self.read(4)?;
                let i = i32::from_be_bytes(val.try_into().unwrap());
                return Ok(MessageValue::Int(i));
            }
            // big int
            0xCB => {
                let val = self.read(8)?;
                let i = i64::from_be_bytes(val.try_into().unwrap());
                return Ok(MessageValue::BigInt(i));
            }
            // bytes
            0xCC => {
                let size = self.read(1)?.as_ptr();
                return Ok(MessageValue::Bytes(self.read(size as usize)?.to_vec()));
            }
            0xCD => {
                let size = self.read(2)?.as_ptr();
                return Ok(MessageValue::Bytes(self.read(size as usize)?.to_vec()));
            }
            0xCE => {
                let size = self.read(4)?.as_ptr();
                return Ok(MessageValue::Bytes(self.read(size as usize)?.to_vec()));
            }
            // string
            0xD0 => {
                let size = self.read(1)?.as_ptr();
                let string_bytes = self.read(size as usize)?;
                return Ok(MessageValue::String(
                    String::from_utf8(string_bytes.to_vec()).unwrap(),
                ));
            }
            0xD1 => {
                let size = self.read(2)?.as_ptr();
                let string_bytes = self.read(size as usize)?;
                return Ok(MessageValue::String(
                    String::from_utf8(string_bytes.to_vec()).unwrap(),
                ));
            }
            0xD2 => {
                let size = self.read(4)?.as_ptr();
                let string_bytes = self.read(size as usize)?;
                return Ok(MessageValue::String(
                    String::from_utf8(string_bytes.to_vec()).unwrap(),
                ));
            }
            // structure
            0xB0..=0xBF => {
                let (size, tag) = self._unpack_structure_header(marker)?;
                let mut value = MessageStructure::new(tag, Vec::new());
                for _ in 0..size {
                    value.fields.push(self.unpack()?);
                }
                return Ok(MessageValue::Structure(value));
            }
            _ => {
                //tiny string
                if marker_high == 0x80 {
                    let size = marker & 0x0F;
                    let string_bytes = self.read(size as usize)?;
                    return Ok(MessageValue::String(
                        String::from_utf8(string_bytes.to_vec()).unwrap(),
                    ));
                } else {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "unpackable buffer exhausted",
                    ));
                }
            }
        }
    }

    fn unpack_structure_header(&mut self) -> Result<(u8, u8), std::io::Error> {
        let marker = self.read_u8()?;
        return self._unpack_structure_header(marker);
    }

    fn _unpack_structure_header(&mut self, marker: u8) -> Result<(u8, u8), std::io::Error> {
        let marker_high = marker & 0xF0;
        match marker_high {
            0xB0 => {
                let sig = self.read(1)?;
                let size = marker & 0x0F;
                return Ok((size, sig[0]));
            }
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "unpackable buffer exhausted",
                ));
            }
        }
    }
}

struct PackStream<'a> {
    reader: ReadHalf<'a>,
    writer: WriteHalf<'a>,
}

impl<'a> PackStream<'a> {
    pub fn new(stream: &'a mut TcpStream) -> Self {
        let (reader, writer) = stream.split();
        return Self { reader, writer };
    }

    pub async fn read_message(&mut self) -> Result<MessageValue, std::io::Error> {
        let mut data = Vec::new();
        let mut more = true;
        while more {
            let mut head_buf = [0; 2];
            let chunk_header = self.reader.read(&mut head_buf).await;
            match chunk_header {
                Ok(2) => {
                    let chunk_size = u16::from_be_bytes(head_buf.try_into().unwrap());
                    if chunk_size != 0 {
                        let chunk_size = chunk_size as usize;
                        let mut chunk_buf = vec![0; chunk_size];
                        let _chunk_read = self.reader.read(&mut chunk_buf).await;
                        data.copy_from_slice(chunk_buf.as_ref());
                    }
                }
                Ok(_) => {
                    more = false;
                }
                Err(e) => {
                    println!("{:?}", e);
                    more = false;
                }
            }
        }
        let unpack_buf = UnpackableBuffer::new(Some(data));
        let mut unpacker = Unpacker::new(unpack_buf);
        return unpacker.unpack();
    }

    pub async fn write_message(&mut self, message: MessageStructure) -> Result<(), std::io::Error> {
        let mut packer = Packer::new(MessageBuffer::new(8192));
        packer.pack(MessageValue::Structure(message))?;
        let data = packer.stream.buffer;
        let term = vec![0x00, 0x00];
        let div = data.len() / 0x100;
        let modu = data.len() % 0x100;
        let mut b_msg = vec![div as u8, modu as u8];
        b_msg.extend(data);
        b_msg.extend(term);
        let _write = self.writer.write(&b_msg).await;
        Ok(())
    }

    pub async fn drain(&mut self) -> Result<(), std::io::Error> {
        self.writer.flush().await?;
        Ok(())
    }

    pub async fn close(&mut self) -> Result<(), std::io::Error> {
        self.writer.flush().await?;
        self.writer.shutdown().await?;
        Ok(())
    }
}
