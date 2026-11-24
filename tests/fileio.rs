use dsh_remote::fileio;
use dsh_remote::protocol::ErrorCode;

fn temp_dir() -> std::path::PathBuf {
    let mut suffix = [0u8; 6];
    use rand::RngCore;
    rand::thread_rng().fill_bytes(&mut suffix);
    std::env::temp_dir().join(format!("dsh-remote-test-{}", hex::encode(suffix)))
}

#[tokio::test]
async fn writes_atomically_and_cleans_up() {
    let dir = temp_dir();
    tokio::fs::create_dir_all(&dir).await.unwrap();
    let path = dir.join("file.bin");
    let data: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();

    let mut src = data.as_slice();
    let (bytes, sha256) = fileio::recv_file(&mut src, &path, data.len() as u64, Some(0o640), true)
        .await
        .unwrap();
    assert_eq!(bytes, data.len() as u64);
    assert_eq!(tokio::fs::read(&path).await.unwrap(), data);
    assert_eq!(sha256.len(), 64);

    let mut src = data.as_slice();
    let failure = fileio::recv_file(&mut src, &path, data.len() as u64, None, false)
        .await
        .unwrap_err();
    assert_eq!(failure.code, ErrorCode::AlreadyExists);

    let short = b"abc";
    let mut src = short.as_slice();
    let target = dir.join("short.bin");
    let failure = fileio::recv_file(&mut src, &target, 10, None, true)
        .await
        .unwrap_err();
    assert_eq!(failure.code, ErrorCode::IoError);
    assert!(!target.exists());

    let mut names = Vec::new();
    let mut entries = tokio::fs::read_dir(&dir).await.unwrap();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        names.push(entry.file_name().to_string_lossy().into_owned());
    }
    names.sort();
    assert_eq!(names, vec!["file.bin".to_string()]);

    tokio::fs::remove_dir_all(&dir).await.unwrap();
}
