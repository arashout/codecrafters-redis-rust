use std::vec;

use bytes::BytesMut;

/// Parser for Redis RESP protocol
pub struct Parser {
    index: usize,
}

/// Fundamental struct for viewing byte slices
///
/// Used for zero-copy redis values.
#[derive(Debug, PartialEq, Clone, Copy)]
struct BufSplit(usize, usize);
impl BufSplit {
    fn len(&self) -> usize {
        self.1 - self.0
    }

    fn to_string(&self, src: &[u8]) -> String {
        String::from_utf8_lossy(&src[self.0..self.1]).to_string()
    }
}

/// BufSplit based equivalent to our output type RedisValueRef
#[derive(Debug, PartialEq, Clone)]
pub enum RedisBufSplit {
    String(BufSplit),
    Error(BufSplit),
    Int(i64),
    Array(Vec<RedisBufSplit>),
    NullArray,
    NullBulkString,
}

impl RedisBufSplit {
    pub fn to_string(&self, src: &BytesMut) -> String {
        match self {
            RedisBufSplit::String(word) => word.to_string(src),
            RedisBufSplit::Error(word) => word.to_string(src),
            RedisBufSplit::Int(i) => i.to_string(),
            RedisBufSplit::Array(words) => {
                let mut s = String::new();
                s.push('[');
                for (i, word) in words.iter().enumerate() {
                    if i > 0 {
                        s.push(',');
                    }
                    s.push_str(&word.to_string(src));
                }
                s.push(']');
                s
            }
            RedisBufSplit::NullArray => "[]".to_string(),
            RedisBufSplit::NullBulkString => "null".to_string(),
        }
    }
    pub fn to_resp(&self, src: &BytesMut) -> String {
        match self {
            RedisBufSplit::String(word) => {
                format!("${}\r\n{}\r\n", word.len(), word.to_string(src))
            }
            RedisBufSplit::Error(word) => format!("-{}\r\n", word.to_string(src)),
            RedisBufSplit::Int(i) => format!(":{}\r\n", i),
            RedisBufSplit::Array(words) => {
                let mut s = String::new();
                s.push('*');
                s.push_str(&words.len().to_string());
                s.push_str("\r\n");
                for (i, word) in words.iter().enumerate() {
                    if i > 0 {
                        s.push_str("\r\n");
                    }
                    s.push_str(&word.to_resp(src));
                }
                s
            }
            RedisBufSplit::NullArray => "*-1\r\n".to_string(),
            RedisBufSplit::NullBulkString => "$-1\r\n".to_string(),
        }
    }
}

#[derive(Debug)]
pub enum RESPError {
    UnexpectedEnd,
    UnknownStartingByte(u8),
    IOError(std::io::Error),
    IntParseFailure,
    BadBulkStringSize(i64),
    BadArraySize(i64),
}
type RedisResult = Result<Option<(usize, RedisBufSplit)>, RESPError>;
impl Parser {
    // fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
    //     unimplemented!()
    // }

    fn token(src: &BytesMut, index: usize) -> Option<(usize, BufSplit)> {
        let start = index;
        let mut end = index;
        while end < src.len() && src[end] != b'\r' {
            end += 1;
        }
        if end == src.len() {
            return None;
        }
        Some((end + 2, BufSplit(start, end)))
    }

    fn parse_int(src: &BytesMut, index: usize) -> Result<(usize, i64), RESPError> {
        if !vec![b'$', b':', b'*'].contains(&src[index]) {
            return Err(RESPError::UnknownStartingByte(src[index].clone()));
        }
        let (index, split) = Parser::token(src, index).unwrap();

        let num_str = String::from_utf8_lossy(&src[split.0 + 1..split.1]);
        let res = num_str.parse::<i64>();
        if res.is_err() {
            return Err(RESPError::IntParseFailure);
        }
        Ok((split.1 + 2, res.unwrap()))
    }

    pub fn parse_bulk_string(src: &BytesMut, index: usize) -> RedisResult {
        // Bulk String format:
        // $<usize>\r\n<data>\r\n
        assert!(src[index] == b'$');
        let (index, size) = Parser::parse_int(src, index).map(|x| (x.0, x.1 as usize))?;
        let start = index;
        let end = index + size;
        if end > src.len() {
            return Err(RESPError::UnexpectedEnd);
        }
        Ok(Some((end + 2, RedisBufSplit::String(BufSplit(start, end)))))
    }

    pub fn parse_array(src: &BytesMut, index: usize) -> RedisResult {
        // Array format:
        // *<usize>\r\n<element_1>\r\n<element_2>\r\n...
        assert!(src[index] == b'*');
        let (index, size) = Parser::parse_int(src, index).map(|x| (x.0, x.1 as usize))?;
        let mut tokens = vec![];
        let mut pos = index;
        for _ in 0..size {
            match src[pos] {
                b'$' => {
                    let (new_pos, word) = Parser::parse_bulk_string(src, pos)
                        .expect("failed to parse bulk string")
                        .unwrap();
                    tokens.push(word);
                    pos = new_pos
                }
                _ => {
                    unimplemented!("No implementations for parsing any other array elements except bulk strings");
                }
            }
        }
        Ok(Some((pos, RedisBufSplit::Array(tokens))))
    }

    pub fn simple_string(buf: &BytesMut, pos: usize) -> RedisResult {
        // Skip the first byte "+"
        match Parser::token(buf, pos+1) {
            Some((pos, word)) => Ok(Some((pos, RedisBufSplit::String(word)))),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;

    #[test]
    fn test_word() {
        let mut buf = BytesMut::from(&b"*2\r\n$3\r\nSET\r\n$3\r\nfoo\r\n"[..]);
        let (pos, word) = Parser::token(&mut buf, 0).unwrap();
        assert_eq!(word, BufSplit(0, 2));
        assert_eq!(pos, 4);
    }

    #[test]
    fn test_int() {
        let mut buf = BytesMut::from(&b"*2\r\n$10\r\nfoobarabcd\r\n"[..]);
        let (pos, u) = Parser::parse_int(&mut buf, 4)
            .map(|x| (x.0, x.1 as usize))
            .unwrap();
        assert_eq!(u, 10);
        assert_eq!(pos, 9);
        assert_eq!(buf[pos], b'f');
        assert_eq!(BufSplit(pos, pos + u).to_string(&buf), "foobarabcd");
    }

    #[test]
    fn test_words() {
        let mut i = 0;
        let buf = BytesMut::from(&b"*2\r\n$3\r\nSET\r\n$3\r\nfoo\r\n"[..]);
        let mut words = vec![];
        while i < buf.len() {
            let (pos, word) = Parser::token(&buf, i).unwrap();
            println!("{}", String::from_utf8_lossy(&buf[word.0..word.1]));
            words.push(String::from_utf8_lossy(&buf[word.0..word.1]));
            i = pos;
        }
        assert_eq!(words.len(), 5);
        assert_eq!(words[0], "*2");
        assert_eq!(words[1], "$3");
        assert_eq!(words[2], "SET");
        assert_eq!(words[3], "$3");
        assert_eq!(words[4], "foo");
    }

    #[test]
    fn test_bulk_string() {
        let mut buf = BytesMut::from(&b"$3\r\nSET\r\n$10\r\nfoobarabcd\r\n"[..]);
        let (pos, split) = Parser::parse_bulk_string(&mut buf, 0).unwrap().unwrap();
        match split {
            RedisBufSplit::String(word) => {
                assert_eq!(word.to_string(&buf), "SET");
            }
            _ => panic!("expected string"),
        }
        // Read the next bulk string
        let (pos, split) = Parser::parse_bulk_string(&mut buf, pos).unwrap().unwrap();
        match split {
            RedisBufSplit::String(word) => {
                assert_eq!(word.to_string(&buf), "foobarabcd");
            }
            _ => panic!("expected string"),
        }
    }

    #[test]
    fn test_array() {
        let mut buf = BytesMut::from(&b"*2\r\n$3\r\nSET\r\n$3\r\nfoo\r\n"[..]);
        let (pos, split) = Parser::parse_array(&mut buf, 0).unwrap().unwrap();
        match split {
            RedisBufSplit::Array(words) => {
                assert_eq!(words.len(), 2);
                assert_eq!(words[0].to_string(&buf), "SET");
                assert_eq!(words[1].to_string(&buf), "foo");
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_simple_string() {
        let mut buf = BytesMut::from(&b"+OK\r\n"[..]);
        let (pos, split) = Parser::simple_string(&mut buf, 0).unwrap().unwrap();
        match split {
            RedisBufSplit::String(word) => {
                assert_eq!(word.to_string(&buf), "OK");
            }
            _ => panic!("expected string"),
        }
    }
}
