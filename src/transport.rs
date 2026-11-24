use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use quinn::rustls;
use quinn::rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use quinn::rustls::crypto::{verify_tls12_signature, verify_tls13_signature, CryptoProvider};
use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use quinn::rustls::{DigitallySignedStruct, SignatureScheme};
use quinn::{Connection, Endpoint, RecvStream, SendStream};
use rand::RngCore;
use sha2::{Digest, Sha256};

use crate::frame::{read_frame, write_frame};
use crate::protocol::{Failure, Role, ALPN, PROTO_VERSION, SERVER_NAME};

pub type Stream = tokio::io::Join<RecvStream, SendStream>;

pub fn wrap(send: SendStream, recv: RecvStream) -> Stream {
    tokio::io::join(recv, send)
}

pub async fn resolve(addr: &str) -> Result<SocketAddr> {
    let mut iter = tokio::net::lookup_host(addr)
        .await
        .with_context(|| format!("解析地址失败：{}", addr))?;
    iter.next()
        .ok_or_else(|| anyhow!("地址无法解析出任何 IP：{}", addr))
}

pub async fn open(conn: &Connection) -> Result<Stream> {
    let (send, recv) = conn.open_bi().await.context("打开流失败")?;
    Ok(wrap(send, recv))
}

pub async fn accept(conn: &Connection) -> Result<Stream> {
    let (send, recv) = conn.accept_bi().await.context("接收流失败")?;
    Ok(wrap(send, recv))
}

fn crypto_provider() -> Arc<CryptoProvider> {
    let provider = rustls::crypto::ring::default_provider();
    let _ = provider.clone().install_default();
    Arc::new(provider)
}

fn transport_config() -> quinn::TransportConfig {
    let mut config = quinn::TransportConfig::default();
    config.keep_alive_interval(Some(Duration::from_secs(5)));
    config.max_idle_timeout(Some(
        Duration::from_secs(20).try_into().expect("idle timeout"),
    ));
    config.max_concurrent_bidi_streams(1024u32.into());
    config
}

pub struct ServerEndpoint {
    pub endpoint: Endpoint,
    pub fingerprint: String,
}

pub fn server_endpoint(
    addr: &str,
    cert_path: Option<&Path>,
    key_path: Option<&Path>,
) -> Result<ServerEndpoint> {
    let addr: SocketAddr = addr
        .parse()
        .with_context(|| format!("监听地址无效：{}", addr))?;
    let (cert, key) = load_or_create_identity(cert_path, key_path)?;
    let fingerprint = hex::encode(Sha256::digest(cert.as_ref()));

    let provider = crypto_provider();
    let mut tls = rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .context("初始化 TLS 配置失败")?
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)
        .context("加载服务端证书失败")?;
    tls.alpn_protocols = vec![ALPN.to_vec()];

    let quic = quinn::crypto::rustls::QuicServerConfig::try_from(tls).context("构造 QUIC 配置失败")?;
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(quic));
    config.transport_config(Arc::new(transport_config()));

    let endpoint = Endpoint::server(config, addr).context("监听 QUIC 端口失败")?;
    Ok(ServerEndpoint {
        endpoint,
        fingerprint,
    })
}

pub fn client_endpoint(pin: Option<&str>) -> Result<Endpoint> {
    client_endpoint_with_bind(pin, "0.0.0.0:0".parse().expect("bind addr"))
}

pub fn client_endpoint_with_bind(pin: Option<&str>, bind: SocketAddr) -> Result<Endpoint> {
    let provider = crypto_provider();
    let expected = match pin {
        Some(value) => Some(parse_pin(value)?),
        None => None,
    };
    let verifier = Arc::new(PinVerifier {
        expected,
        provider: provider.clone(),
    });
    let mut tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .context("初始化 TLS 配置失败")?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    tls.alpn_protocols = vec![ALPN.to_vec()];

    let quic = quinn::crypto::rustls::QuicClientConfig::try_from(tls).context("构造 QUIC 配置失败")?;
    let mut config = quinn::ClientConfig::new(Arc::new(quic));
    config.transport_config(Arc::new(transport_config()));

    let mut endpoint = Endpoint::client(bind).context("创建 QUIC 客户端失败")?;
    endpoint.set_default_client_config(config);
    Ok(endpoint)
}

fn parse_pin(value: &str) -> Result<[u8; 32]> {
    let cleaned: String = value
        .chars()
        .filter(|c| !matches!(c, ':' | ' ' | '-'))
        .collect();
    let bytes = hex::decode(cleaned.trim()).context("证书指纹不是合法的十六进制字符串")?;
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| anyhow!("证书指纹长度应为 32 字节（SHA-256）"))?;
    Ok(bytes)
}

#[derive(Debug)]
struct PinVerifier {
    expected: Option<[u8; 32]>,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for PinVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        if let Some(expected) = self.expected {
            let actual = Sha256::digest(end_entity.as_ref());
            if actual.as_slice() != expected.as_slice() {
                return Err(rustls::Error::InvalidCertificate(
                    rustls::CertificateError::ApplicationVerificationFailure,
                ));
            }
        }
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

pub fn load_or_create_identity(
    cert_path: Option<&Path>,
    key_path: Option<&Path>,
) -> Result<(CertificateDer<'static>, PrivateKeyDer<'static>)> {
    if let (Some(cert_path), Some(key_path)) = (cert_path, key_path) {
        if cert_path.exists() && key_path.exists() {
            let cert_pem = std::fs::read(cert_path)
                .with_context(|| format!("读取证书失败：{}", cert_path.display()))?;
            let key_pem = std::fs::read(key_path)
                .with_context(|| format!("读取私钥失败：{}", key_path.display()))?;
            let cert = rustls_pemfile::certs(&mut cert_pem.as_slice())
                .next()
                .transpose()
                .context("解析证书失败")?
                .ok_or_else(|| anyhow!("证书文件为空：{}", cert_path.display()))?;
            let key = rustls_pemfile::private_key(&mut key_pem.as_slice())
                .context("解析私钥失败")?
                .ok_or_else(|| anyhow!("私钥文件为空：{}", key_path.display()))?;
            return Ok((cert, key));
        }
    }

    let key_pair = rcgen::KeyPair::generate_for(&rcgen::PKCS_ED25519).context("生成私钥失败")?;
    let params = rcgen::CertificateParams::new(vec![SERVER_NAME.to_string()])
        .context("生成证书参数失败")?;
    let cert = params.self_signed(&key_pair).context("签发自签证书失败")?;
    let cert_der = cert.der().clone();
    let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_pair.serialize_der()));

    if let (Some(cert_path), Some(key_path)) = (cert_path, key_path) {
        if let Some(dir) = cert_path.parent() {
            std::fs::create_dir_all(dir)
                .with_context(|| format!("创建证书目录失败：{}", dir.display()))?;
        }
        std::fs::write(cert_path, cert.pem())
            .with_context(|| format!("写入证书失败：{}", cert_path.display()))?;
        std::fs::write(key_path, key_pair.serialize_pem())
            .with_context(|| format!("写入私钥失败：{}", key_path.display()))?;
        restrict(key_path);
    }

    Ok((cert_der, key_der))
}

fn restrict(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    if let Ok(meta) = std::fs::metadata(path) {
        let mut perms = meta.permissions();
        perms.set_mode(0o600);
        let _ = std::fs::set_permissions(path, perms);
    }
}

fn auth_mac(psk: &[u8], nonce: &[u8], role: Role, node_id: &str) -> [u8; 32] {
    use hmac::{Hmac, Mac};
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(psk).expect("HMAC 密钥长度任意");
    mac.update(nonce);
    mac.update(role.as_str().as_bytes());
    mac.update(b"\0");
    mac.update(node_id.as_bytes());
    let out = mac.finalize().into_bytes();
    let mut digest = [0u8; 32];
    digest.copy_from_slice(&out);
    digest
}

pub async fn serve_auth(
    conn: &Connection,
    psk: &[u8],
    allowed: &[Role],
) -> Result<(Role, String, Vec<String>, Stream)> {
    let mut stream = accept(conn).await?;
    match read_frame::<_, crate::protocol::ControlMsg>(&mut stream).await? {
        crate::protocol::ControlMsg::Hello { version } if version == PROTO_VERSION => {}
        _ => {
            refuse(
                &mut stream,
                Failure::new(crate::protocol::ErrorCode::BadRequest, "协议版本不匹配"),
            )
            .await?;
            bail!("协议版本不匹配");
        }
    }
    let mut nonce = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut nonce);
    write_frame(
        &mut stream,
        &crate::protocol::ControlMsg::Challenge {
            nonce: hex::encode(nonce),
        },
    )
    .await?;

    let auth: crate::protocol::ControlMsg = read_frame(&mut stream).await?;
    let (role, node_id, tags, mac) = match auth {
        crate::protocol::ControlMsg::Auth {
            role,
            node_id,
            tags,
            mac,
        } => (role, node_id, tags, mac),
        _ => {
            refuse(&mut stream, Failure::new(crate::protocol::ErrorCode::AuthFailed, "首条消息不是认证请求")).await?;
            bail!("首条消息不是认证请求");
        }
    };

    let expected = auth_mac(psk, &nonce, role, &node_id);
    let provided = hex::decode(&mac).unwrap_or_default();
    let ok = constant_time_eq(&provided, &expected);

    if !ok || !allowed.contains(&role) {
        let failure = Failure::new(
            crate::protocol::ErrorCode::AuthFailed,
            if ok { "该角色不允许接入本服务" } else { "预共享密钥不匹配" },
        );
        refuse(&mut stream, failure.clone()).await?;
        bail!("{}", failure.text());
    }

    write_frame(
        &mut stream,
        &crate::protocol::ControlMsg::AuthOk {
            node_id: node_id.clone(),
            version: PROTO_VERSION,
        },
    )
    .await?;
    Ok((role, node_id, tags, stream))
}

async fn refuse(stream: &mut Stream, failure: Failure) -> Result<()> {
    write_frame(
        stream,
        &crate::protocol::ControlMsg::AuthErr {
            code: failure.code,
            message: failure.message,
        },
    )
    .await
}

pub async fn client_auth(
    conn: &Connection,
    psk: &[u8],
    role: Role,
    node_id: &str,
    tags: &[String],
) -> Result<Stream> {
    let mut stream = open(conn).await?;
    write_frame(
        &mut stream,
        &crate::protocol::ControlMsg::Hello {
            version: PROTO_VERSION,
        },
    )
    .await?;
    let challenge: crate::protocol::ControlMsg = read_frame(&mut stream).await?;
    let nonce = match challenge {
        crate::protocol::ControlMsg::Challenge { nonce } => {
            hex::decode(nonce).context("挑战值格式错误")?
        }
        crate::protocol::ControlMsg::AuthErr { code, message } => {
            bail!("{}", Failure { code, message }.text())
        }
        _ => bail!("服务端未发起认证挑战"),
    };
    let mac = hex::encode(auth_mac(psk, &nonce, role, node_id));
    write_frame(
        &mut stream,
        &crate::protocol::ControlMsg::Auth {
            role,
            node_id: node_id.to_string(),
            tags: tags.to_vec(),
            mac,
        },
    )
    .await?;

    let reply: crate::protocol::ControlMsg = read_frame(&mut stream).await?;
    match reply {
        crate::protocol::ControlMsg::AuthOk { .. } => Ok(stream),
        crate::protocol::ControlMsg::AuthErr { code, message } => {
            bail!("{}", Failure { code, message }.text())
        }
        _ => bail!("认证响应格式错误"),
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
}
