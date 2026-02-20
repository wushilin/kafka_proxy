use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use std::io::ErrorKind;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const MAX_FRAME_SIZE: usize = 100 * 1024 * 1024;

pub async fn read_frame_len<R>(reader: &mut R) -> Result<Option<usize>>
where
    R: AsyncRead + Unpin,
{
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e).context("Failed to read Kafka frame length"),
    }

    let len = i32::from_be_bytes(len_buf);
    if len < 0 {
        return Err(anyhow!("Invalid Kafka frame length: {}", len));
    }

    let len = len as usize;
    if len > MAX_FRAME_SIZE {
        return Err(anyhow!(
            "Kafka frame length {} exceeds max {}",
            len,
            MAX_FRAME_SIZE
        ));
    }

    Ok(Some(len))
}

pub async fn write_frame_len<W>(writer: &mut W, len: usize) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let len: i32 = len
        .try_into()
        .map_err(|_| anyhow!("Kafka frame too large to encode"))?;

    writer
        .write_all(&len.to_be_bytes())
        .await
        .context("Failed to write Kafka frame length")?;

    Ok(())
}

pub async fn write_kafka_frame<W>(writer: &mut W, payload: &Bytes) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_frame_len(writer, payload.len()).await?;
    writer
        .write_all(payload)
        .await
        .context("Failed to write Kafka frame payload")?;
    Ok(())
}

pub async fn copy_exact_n<R, W>(
    reader: &mut R,
    writer: &mut W,
    mut remaining: usize,
    scratch: &mut [u8],
) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    while remaining > 0 {
        let chunk = remaining.min(scratch.len());
        reader
            .read_exact(&mut scratch[..chunk])
            .await
            .context("Failed to read frame payload chunk")?;
        writer
            .write_all(&scratch[..chunk])
            .await
            .context("Failed to write frame payload chunk")?;
        remaining -= chunk;
    }
    Ok(())
}
