use std::{time::Duration, vec};

use bytes::BytesMut;

use crate::{log::Logger};
use crate::cast;
/// Parser for Redis RESP protocol
pub struct Parser {
    index: usize,
}

pub enum RESPDataType {
    SimpleString,
    SimpleError,
    Integer,
    BulkString,
    Array,
    Null,
    // etc ...
}
/// Fundamental struct for viewing byte slices
///
/// Used for zero-copy redis values.
#[derive(Debug, PartialEq, Clone, Copy)]
pub struct BufSplit(usize, usize);
impl BufSplit {
    fn len(&self) -> usize {
        self.1 - self.0
    }

    pub fn to_string(&self, src: &[u8]) -> String {
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

#[derive(Debug, PartialEq, Clone)]
pub struct ParsedCommand {
    pub command: Command,
    pub bytes_read: usize,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Command {
    Set(String, String, Option<Duration>),
    Get(String),
    Ping,
    Echo(String),
    Docs,
    Info,
    ReplConf(String),
    Psync,
    Unknown,
}

#[derive(Debug)]
pub enum RESPError {
    UnexpectedEnd,
    UnknownStartingByte(u8),
    IOError(std::io::Error),
    InvalidArgument(String),
    IntParseFailure(String),
    BadBulkStringSize(i64),
    BadArraySize(i64),
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

type RedisResult = Result<Option<(usize, RedisBufSplit)>, RESPError>;
impl Parser {
    // fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
    //     unimplemented!()
    // }
    pub fn token(src: &BytesMut, index: usize) -> Option<(usize, BufSplit)> {
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
            return Err(RESPError::IntParseFailure(num_str.to_string()));
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
        match Parser::token(buf, pos + 1) {
            Some((pos, word)) => Ok(Some((pos, RedisBufSplit::String(word)))),
            None => Ok(None),
        }
    }

    pub fn find_start_resp_data_type(
        buf: &BytesMut,
        index: usize,
        query_type: &RESPDataType,
    ) -> Option<usize> {
        let mut pos = index;
        loop {
            if pos >= buf.len() {
                return None;
            }
            match (buf[pos], query_type) {
                (b'*', RESPDataType::Array) => {
                    let res = Parser::parse_int(buf, pos);
                    if res.is_err() {
                        pos += 1;
                        continue;
                    }
                    return Some(pos);
                }
                _ => {
                    pos += 1;
                    continue;
                }
            }
        }
    }

    pub fn parse_commands(logger: &Logger, bm: &BytesMut) -> Result<Vec<ParsedCommand>, RESPError> {
        let mut pos = 0;
        let mut commands = Vec::new();

        while pos < bm.len() {
            match bm[pos] {
                b'*' => {
                    let start_pos = pos;
                    let (i, res) = Parser::parse_array(bm, pos)?.unwrap();
                    pos = i;
                    let a = cast!(res, RedisBufSplit::Array);
                    let command = a[0].to_string(bm).to_lowercase();
                    let bytes_read = pos - start_pos;

                    match command.as_str() {
                        "echo" => {
                            let echo_str = a[1].to_string(bm);
                            commands.push(ParsedCommand {
                                command: Command::Echo(echo_str),
                                bytes_read,
                            });
                        }
                        "ping" => {
                            commands.push(ParsedCommand {
                                command: Command::Ping,
                                bytes_read,
                            });
                        }
                        "set" => {
                            let key = a[1].to_string(bm);
                            let value = a[2].to_string(bm);
                            let expiry = if a.len() == 5 {
                                let expiry_str = a[4].to_string(bm);
                                let expiry_num =
                                    expiry_str.parse::<u64>().map_err(|e| {
                                        RESPError::IntParseFailure(expiry_str)
                                    })?;
                                let duration = match a[3].to_string(bm).to_lowercase().as_str() {
                                    "px" => Ok(Duration::from_millis(expiry_num)),
                                    "ex" => Ok(Duration::from_secs(expiry_num)),
                                    _ => Err(RESPError::InvalidArgument(
                                        a[3].to_string(bm),
                                    )),
                                }?;
                                Some(duration)
                            } else {
                                None
                            };
                            commands.push(ParsedCommand {
                                command: Command::Set(key, value, expiry),
                                bytes_read,
                            });
                        }
                        "get" => {
                            let key = a[1].to_string(bm);
                            commands.push(ParsedCommand {
                                command: Command::Get(key),
                                bytes_read,
                            });
                        }
                        "docs" => {
                            commands.push(ParsedCommand {
                                command: Command::Docs,
                                bytes_read,
                            });
                        }
                        "info" => {
                            commands.push(ParsedCommand {
                                command: Command::Info,
                                bytes_read,
                            });
                        }
                        "replconf" => {
                            let subcommand = a[1].to_string(bm);
                            commands.push(ParsedCommand {
                                command: Command::ReplConf(subcommand),
                                bytes_read,
                            });
                        }
                        "psync" => {
                            commands.push(ParsedCommand {
                                command: Command::Psync,
                                bytes_read,
                            });
                        }
                        _ => {
                            unimplemented!("Command not implemented: {}", command);
                        }
                    }
                }
                b'$' => {
                    let start_pos = pos;
                    let r = Parser::parse_bulk_string(bm, pos)?;
                    if let Some(r) = r {
                        pos = r.0;
                        let bytes_read = pos - start_pos;
                        logger.log(&format!(
                            "Bulk string: '{}' not doing anything with it",
                            r.1.to_string(&bm)
                        ));
                        continue;
                        
                    }
                }
                c => {
                    unimplemented!("Unknown byte sequence: '{}'", String::from_utf8_lossy([c].as_slice()));
                }
            }
        }
        Ok(commands)
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

    #[test]
    fn test_token() {
        let pysnc_resp = b"+FULLRESYNC 75cd7bc10c49047e0d163660f3b90625b1af31dc 0\r\n$88\r\nREDIS0011\xEF\xBF\xBD       redis-ver7.2.0\xEF\xBF\xBD\r\nredis-bits\xEF\xBF\xBD@\xEF\xBF\xBDctime\xEF\xBF\xBD\xEF\xBF\xBDused-mem\xC2\xB0\xEF\xBF\xBDaof-base\xEF\xBF\xBD\xEF\xBF\xBD\xEF\xBF\xBDn;\xEF\xBF\xBD\xEF\xBF\xBDZ\xEF\xBF\xBD\r\n*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n";
        let mut buf = BytesMut::from(&pysnc_resp[..]);
        let mut i = 0;
        loop {
            let (pos, word) = Parser::token(&mut buf, i).unwrap();
            let word = word.to_string(&buf);
            if word.starts_with("*") {
                break;
            }
            i = pos;
        }
        let (i, split) = Parser::parse_array(&mut buf, i).unwrap().unwrap();
        match split {
            RedisBufSplit::Array(words) => {
                assert_eq!(words.len(), 3);
                assert_eq!(words[0].to_string(&buf), "SET");
                assert_eq!(words[1].to_string(&buf), "foo");
                assert_eq!(words[2].to_string(&buf), "123");
            }
            _ => panic!("expected array"),
        }
        let (i, split) = Parser::parse_array(&mut buf, i).unwrap().unwrap();
        match split {
            RedisBufSplit::Array(words) => {
                assert_eq!(words.len(), 3);
                assert_eq!(words[0].to_string(&buf), "SET");
                assert_eq!(words[1].to_string(&buf), "bar");
                assert_eq!(words[2].to_string(&buf), "456");
            }
            _ => panic!("expected array"),
        }
        let (i, split) = Parser::parse_array(&mut buf, i).unwrap().unwrap();
        match split {
            RedisBufSplit::Array(words) => {
                assert_eq!(words.len(), 3);
                assert_eq!(words[0].to_string(&buf), "SET");
                assert_eq!(words[1].to_string(&buf), "baz");
                assert_eq!(words[2].to_string(&buf), "789");
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_find_start_resp_type() {
        let mut buf = BytesMut::from(&"+FULLRESYNC 75cd7bc10c49047e0d163660f3b90625b1af31dc 0\r\n$88\r\nREDIS0011\u{fffd}\tredis-ver\u{5}7.2.0\u{fffd}\nredis-bits\u{fffd}@\u{fffd}\u{5}ctime\u{fffd}m\u{8}\u{fffd}e\u{fffd}\u{8}used-mem\u{b0}\u{fffd}\u{10}\u{fffd}\u{8}aof-base\u{fffd}\u{fffd}\u{fffd}n;\u{fffd}\u{fffd}\u{fffd}Z\u{fffd}*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n*2\r\n$3\r\nSET\r\n$3\r\nfoo\r\n"[..]);
        let pos = Parser::find_start_resp_data_type(&mut buf, 0, &RESPDataType::Array).unwrap();
        let (pos, split) = Parser::parse_array(&mut buf, pos).unwrap().unwrap();
        match split {
            RedisBufSplit::Array(words) => {
                assert_eq!(words.len(), 3);
                assert_eq!(words[0].to_string(&buf), "REPLCONF");
                assert_eq!(words[1].to_string(&buf), "GETACK");
                assert_eq!(words[2].to_string(&buf), "*");
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_parse_commands(){
        let log = Logger::new();
        let mut buf = BytesMut::from(&b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"[..]);
        let r = Parser::parse_commands(&log, &mut buf).unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].command, Command::Set("foo".to_string(), "bar".to_string(), None));
        assert_eq!(r[0].bytes_read, 31);
        // With expiry
        let mut buf = BytesMut::from(&b"*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nPX\r\n$2\r\n10\r\n"[..]);
        let r = Parser::parse_commands(&log, &mut buf).unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].command, Command::Set("foo".to_string(), "bar".to_string(), Some(Duration::from_millis(10))));
        assert_eq!(r[0].bytes_read, 47);

        // replconf getack *
        let mut buf = BytesMut::from(&b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"[..]);
        let r = Parser::parse_commands(&log, &mut buf).unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].command, Command::ReplConf("GETACK".to_string()));
        assert_eq!(r[0].bytes_read, 37);

        // ping command
        let mut buf = BytesMut::from(&b"*1\r\n$4\r\nPING\r\n"[..]);
        let r = Parser::parse_commands(&log, &mut buf).unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].command, Command::Ping);
        assert_eq!(r[0].bytes_read, 14);

        // multiple set commands
        let mut buf = BytesMut::from(&b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"[..]);
        let r = Parser::parse_commands(&log, &mut buf).unwrap();
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].command, Command::Set("foo".to_string(), "bar".to_string(), None));
        assert_eq!(r[0].bytes_read, 31);
        assert_eq!(r[1].command, Command::Set("foo".to_string(), "bar".to_string(), None));
        assert_eq!(r[1].bytes_read, 31);
    }
}
