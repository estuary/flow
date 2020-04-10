use redis_protocol::prelude as redis;
use bytes::BytesMut;

pub fn foo() {
  let frame = redis::Frame::BulkString("foobar".into());
  let mut buf = BytesMut::new();
  
  let len = match redis::encode_bytes(&mut buf, &frame) {
    Ok(l) => l,
    Err(e) => panic!("Error encoding frame: {:?}", e)
  };
  println!("Encoded {} bytes into buffer with contents {:?}", len, buf);
  
  let buf: BytesMut = "*3\r\n$3\r\nFoo\r\n$-1\r\n$3\r\nBar\r\n".into();
  let (frame, consumed) = match redis::decode_bytes(&buf) {
    Ok((f, c)) => (f, c),
    Err(e) => panic!("Error parsing bytes: {:?}", e)
  };
  
  if let Some(frame) = frame {
    println!("Parsed frame {:?} and consumed {} bytes", frame, consumed);
  } else{
    println!("Incomplete frame, parsed {} bytes", consumed);
  }
  
  let key = "foobarbaz";
  println!("Hash slot for {}: {}", key, redis::redis_keyslot(key));
}

#[cfg(test)]
mod test {

    #[test]
    fn test_foobar() {
        super::foo()
    }
}
