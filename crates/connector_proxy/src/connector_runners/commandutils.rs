use crate::errors::Error;

use prost::Message;

use byteorder::{ByteOrder, LittleEndian};
use std::io::{Read, Write};
use std::process::{Child, Command, ExitStatus, Stdio};

pub fn invoke_connector(
    entrypoint: Vec<String>,
    stdin: Stdio,
    stdout: Stdio,
    cmd: &str,
) -> Result<Child, Error> {
    if entrypoint.len() == 0 {
        return Err(Error::EmptyEntrypointError);
    }
    Command::new(entrypoint[0].clone())
        .stdin(stdin)
        .stdout(stdout)
        .stderr(Stdio::inherit())
        .args(&entrypoint[1..])
        .arg(cmd)
        .spawn()
        .map_err(|e| e.into())
}

fn handle_stream<T: Message + std::default::Default>(
    in_stream: &mut (impl Read + std::marker::Send),
    out_stream: &mut (impl Write + std::marker::Send),
    handlers: &Vec<Box<dyn Fn(&mut T) -> Result<(), Error>>>,
) -> Result<(), Error> {
    let mut length_buf: [u8; 4] = [0; 4];
    if let Err(_) = in_stream.read_exact(&mut length_buf) {
        // Not enough to fill a vector of 4 bytes.
        // It is not a non-trivial proto message. return.
        out_stream.write_all(&length_buf)?;
        return Ok(());
    }

    let message_length = LittleEndian::read_u32(&length_buf);
    let mut message_buf: Vec<u8> = vec![0; message_length as usize];
    in_stream.read_exact(&mut message_buf)?;
    let mut message = T::decode(&message_buf[..])?;

    for handler in handlers {
        handler(&mut message)?;
    }

    let mut outbuf: Vec<u8> = Vec::new();
    message.encode(&mut outbuf)?;

    let mut buf_len = [0; 4];
    LittleEndian::write_u32(&mut buf_len, outbuf.len() as u32);
    out_stream.write_all(&buf_len)?;
    out_stream.write_all(&outbuf)?;

    // Copy the rest of the stream as raw bytes.
    std::io::copy(in_stream, out_stream)?;

    Ok(())
}

pub fn invoke_and_handle_response<T: Message + std::default::Default>(
    entrypoint: Vec<String>,
    cmd: &str,
    handlers: &Vec<Box<dyn Fn(&mut T) -> Result<(), Error>>>,
) -> Result<(), Error> {
    let mut child = invoke_connector(entrypoint, Stdio::inherit(), Stdio::piped(), cmd)?;
    handle_stream(
        &mut child.stdout.take().ok_or(Error::MissingIOPipe)?,
        &mut std::io::stdout(),
        handlers,
    )?;

    check_exit_status(child.wait())
}

pub fn invoke_and_handle_request<T: Message + std::default::Default>(
    entrypoint: Vec<String>,
    cmd: &str,
    handlers: &Vec<Box<dyn Fn(&mut T) -> Result<(), Error>>>,
) -> Result<(), Error> {
    let mut child = invoke_connector(entrypoint, Stdio::piped(), Stdio::inherit(), cmd)?;
    handle_stream(
        &mut std::io::stdin(),
        &mut child.stdin.take().ok_or(Error::MissingIOPipe)?,
        handlers,
    )?;

    check_exit_status(child.wait())
}
// TODO: invoke_and_handle_request_and_response.

// replace this once ExitStatusError is stable.
// https://github.com/rust-lang/rust/issues/84908
pub fn check_exit_status(result: std::io::Result<ExitStatus>) -> Result<(), Error> {
    match result {
        Ok(status) => {
            if status.success() {
                Ok(())
            } else {
                match status.code() {
                    Some(code) => Err(Error::CommandExecutionError(format!(
                        "failed with code {}.",
                        code
                    ))),
                    None => Err(Error::CommandExecutionError(
                        "process terminated by signal".to_string(),
                    )),
                }
            }
        }
        Err(e) => Err(e.into()),
    }
}
