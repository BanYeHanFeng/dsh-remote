#![cfg(target_os = "linux")]

use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

const KEY: &str = "0123456789abcdef0123456789abcdef";

fn host_arch() -> Option<&'static str> {
    match env::consts::ARCH {
        "x86_64" => Some("amd64"),
        "aarch64" => Some("arm64"),
        _ => None,
    }
}

fn script() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scripts/dsh-remote.sh")
}

fn binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_dsh-remote"))
}

fn unique_root() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    env::temp_dir().join(format!("dsh-remote-m5-{}-{nanos}", std::process::id()))
}

struct TestEnv {
    root: PathBuf,
    listen: String,
}

impl TestEnv {
    fn new() -> Self {
        let root = unique_root();
        fs::create_dir_all(root.join("release")).unwrap();
        fs::create_dir_all(root.join("roots")).unwrap();
        Self {
            root,
            listen: "127.0.0.1:0".to_string(),
        }
    }

    fn bin(&self) -> PathBuf {
        self.root.join("bin/dsh-remote")
    }

    fn conf_dir(&self) -> PathBuf {
        self.root.join("etc")
    }

    fn state_dir(&self) -> PathBuf {
        self.root.join("state")
    }

    fn log_dir(&self) -> PathBuf {
        self.root.join("log")
    }

    fn release_dir(&self) -> PathBuf {
        self.root.join("release")
    }

    fn conf(&self, role: &str) -> PathBuf {
        self.conf_dir().join(format!("{role}.conf"))
    }

    fn pid_file(&self, role: &str) -> PathBuf {
        self.state_dir().join(format!("{role}.pid"))
    }

    fn systemd_dir(&self) -> PathBuf {
        self.root.join("systemd")
    }

    fn fake_systemctl(&self) -> PathBuf {
        let dir = self.root.join("fakebin");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("systemctl");
        fs::write(
            &path,
            "#!/bin/sh\ncase \"$1\" in\n  is-active) exit 0 ;;\n  *) exit 0 ;;\nesac\n",
        )
        .unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        dir
    }

    fn with_systemd(&self, command: &mut Command) {
        let path = env::var("PATH").unwrap_or_default();
        command
            .env("DSH_REMOTE_NO_SYSTEMD", "0")
            .env("DSH_REMOTE_SYSTEMD", "1")
            .env("DSH_REMOTE_SYSTEMD_DIR", self.systemd_dir())
            .env(
                "PATH",
                format!("{}:{path}", self.fake_systemctl().display()),
            );
    }

    fn base_command(&self) -> Command {
        let mut command = Command::new("sh");
        command
            .arg(script())
            .env("NO_COLOR", "1")
            .env("DSH_REMOTE_BIN", self.bin())
            .env("DSH_REMOTE_CONF_DIR", self.conf_dir())
            .env("DSH_REMOTE_STATE_DIR", self.state_dir())
            .env("DSH_REMOTE_LOG_DIR", self.log_dir())
            .env("DSH_REMOTE_NO_SYSTEMD", "1")
            .env("DSH_REMOTE_ALLOW_NONROOT", "1")
            .env("DSH_REMOTE_ASSUME_YES", "1")
            .env("DSH_REMOTE_DOWNLOAD_BASE", self.download_base())
            .env("DSH_REMOTE_HUB", "127.0.0.1:1")
            .env("DSH_REMOTE_KEY", KEY)
            .env("DSH_REMOTE_NAME", "m5test")
            .env("DSH_REMOTE_ROOTS", self.root.join("roots"))
            .env("DSH_REMOTE_LISTEN", &self.listen)
            .env_remove("DSH_REMOTE_CHANNEL")
            .env_remove("DSH_REMOTE_FORCE")
            .env_remove("DSH_REMOTE_SYSTEMD")
            .env_remove("DSH_REMOTE_REEXEC");
        command
    }

    fn command(&self, role: &str, action: &str) -> Command {
        let mut command = self.base_command();
        command.arg(role).arg(action);
        command
    }

    fn download_base(&self) -> String {
        format!("file://{}", self.release_dir().display())
    }

    fn run(&self, role: &str, action: &str) -> Output {
        let output = self.command(role, action).output().unwrap();
        assert!(
            output.status.success(),
            "dsh-remote.sh {role} {action} 失败\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }

    fn make_release(&self, extra: &[u8]) {
        let arch = host_arch().unwrap();
        let payload = self.release_dir().join(format!("dsh-remote-linux-{arch}"));
        fs::copy(binary(), &payload).unwrap();
        fs::set_permissions(&payload, fs::Permissions::from_mode(0o755)).unwrap();
        if !extra.is_empty() {
            let mut file = OpenOptions::new().append(true).open(&payload).unwrap();
            file.write_all(extra).unwrap();
        }
    }

    fn channel_file(&self) -> PathBuf {
        self.state_dir().join("channel")
    }

    fn read_pid(&self, role: &str) -> Option<u32> {
        fs::read_to_string(self.pid_file(role))
            .ok()?
            .trim()
            .parse()
            .ok()
    }

    fn wait_gone(&self, pid: u32) {
        for _ in 0..50 {
            if !Path::new(&format!("/proc/{pid}")).exists() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        for role in ["hub", "agent"] {
            if let Some(pid) = self.read_pid(role) {
                let _ = Command::new("kill").arg("-9").arg(pid.to_string()).status();
            }
        }
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn contains(output: &Output, needle: &str) -> bool {
    String::from_utf8_lossy(&output.stdout).contains(needle)
        || String::from_utf8_lossy(&output.stderr).contains(needle)
}

#[test]
fn management_script_installs_updates_and_uninstalls() {
    if host_arch().is_none() {
        eprintln!("跳过：只支持 x86_64 / aarch64");
        return;
    }
    let env = TestEnv::new();

    let help = Command::new("sh")
        .arg(script())
        .arg("--help")
        .output()
        .unwrap();
    assert!(help.status.success());
    assert!(contains(&help, "install"));

    env.make_release(b"");
    let output = env.run("agent", "install");
    assert!(contains(&output, "已安装 dsh-remote"));
    assert!(contains(&output, "agent 已启动"));

    let conf = env.conf("agent");
    let text = fs::read_to_string(&conf).unwrap();
    assert!(text.contains("HUB=127.0.0.1:1"), "{text}");
    assert!(text.contains(&format!("KEY={KEY}")), "{text}");
    assert!(text.contains("NAME=m5test"), "{text}");
    assert!(text.contains(&format!("ROOTS={}", env.root.join("roots").display())));
    let mode = fs::metadata(&conf).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600, "配置文件权限应为 600，实际 {mode:o}");
    assert!(env.bin().is_file());

    let pid = env.read_pid("agent").expect("安装后应写入 PID 文件");
    assert!(Path::new(&format!("/proc/{pid}")).exists());

    let status = env.run("agent", "status");
    assert!(contains(&status, "运行中"), "status 应显示运行中");

    let logs = env.run("agent", "logs");
    assert!(!logs.stdout.is_empty(), "logs 应输出最近日志");
    assert!(!fs::read(env.log_dir().join("agent.log"))
        .unwrap()
        .is_empty());

    env.run("agent", "stop");
    assert!(!env.pid_file("agent").exists(), "停止后应删除 PID 文件");
    env.wait_gone(pid);

    env.run("agent", "start");
    let pid_before = env.read_pid("agent").expect("启动后应写入 PID 文件");
    let inode_before = fs::metadata(env.bin()).unwrap().ino();

    let same = env.run("agent", "update");
    assert!(contains(&same, "已是最新版本"));
    assert_eq!(fs::metadata(env.bin()).unwrap().ino(), inode_before);

    env.make_release(b"m5-update-payload");
    let mut update = env.command("agent", "update");
    update.env("DSH_REMOTE_FORCE", "1");
    let output = update.output().unwrap();
    assert!(
        output.status.success(),
        "update 失败\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(contains(&output, "已更新"));
    assert_ne!(fs::metadata(env.bin()).unwrap().ino(), inode_before);
    let pid_after = env.read_pid("agent").expect("更新后应重启并写入 PID");
    assert_ne!(pid_before, pid_after, "更新后应重启进程");
    assert!(Path::new(&format!("/proc/{pid_after}")).exists());

    let server = env.run("server", "install");
    assert!(contains(&server, "不需要常驻服务"));
    assert!(env.conf("server").is_file());
    let server_start = env.run("server", "start");
    assert!(contains(&server_start, "由 DSH"));
    env.run("server", "stop");
    env.run("server", "restart");
    assert!(!env.pid_file("server").exists());

    let hub = env.run("hub", "install");
    assert!(contains(&hub, "hub 已启动"));
    assert!(env.conf("hub").is_file());
    let doctor = env.run("hub", "doctor");
    assert!(contains(&doctor, "自检通过"), "doctor: {:?}", doctor);
    env.run("hub", "stop");

    let mut uninstall = env.command("agent", "uninstall");
    uninstall.env_remove("DSH_REMOTE_ASSUME_YES");
    uninstall.stdin(Stdio::piped());
    uninstall.stdout(Stdio::piped());
    uninstall.stderr(Stdio::piped());
    let mut child = uninstall.spawn().unwrap();
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"y\nn\n\n")
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "uninstall 失败\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!env.pid_file("agent").exists());
    assert!(conf.is_file(), "默认应保留配置与 Key");
    assert!(env.bin().is_file(), "其他角色仍在时默认保留二进制");

    env.run("agent", "start");
    let pid = env.read_pid("agent").expect("再次启动应成功");
    env.run("agent", "stop");
    env.wait_gone(pid);
}

#[test]
fn management_script_writes_systemd_unit() {
    let env = TestEnv::new();
    fs::create_dir_all(env.bin().parent().unwrap()).unwrap();
    fs::copy(binary(), env.bin()).unwrap();
    fs::set_permissions(env.bin(), fs::Permissions::from_mode(0o755)).unwrap();

    let unit = env.systemd_dir().join("dsh-remote-agent.service");
    let mut install = env.command("agent", "install");
    env.with_systemd(&mut install);
    let output = install.output().unwrap();
    assert!(
        output.status.success(),
        "systemd 安装失败\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(unit.is_file(), "应生成 systemd unit");
    assert_eq!(
        fs::metadata(&unit).unwrap().permissions().mode() & 0o777,
        0o644
    );
    let text = fs::read_to_string(&unit).unwrap();
    assert!(text.contains(&format!("ExecStart={} agent --config", env.bin().display())));
    assert!(text.contains("WantedBy=multi-user.target"));
    assert!(text.contains("Restart=on-failure"));
    assert!(
        text.contains("PrivateTmp=false"),
        "ROOTS 在 /tmp 下应关闭 PrivateTmp"
    );
    assert!(env.conf("agent").is_file());

    let mut uninstall = env.command("agent", "uninstall");
    env.with_systemd(&mut uninstall);
    uninstall.env_remove("DSH_REMOTE_ASSUME_YES");
    uninstall.stdin(Stdio::piped());
    uninstall.stdout(Stdio::piped());
    uninstall.stderr(Stdio::piped());
    let mut child = uninstall.spawn().unwrap();
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"y\nn\n\n")
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "systemd 卸载失败\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!unit.exists(), "卸载后应删除 unit");
    assert!(env.conf("agent").is_file(), "默认应保留配置");
}

#[test]
fn management_script_supports_channels() {
    if host_arch().is_none() {
        eprintln!("跳过：只支持 x86_64 / aarch64");
        return;
    }
    let env = TestEnv::new();
    env.make_release(b"");

    let mut show = env.base_command();
    show.arg("channel");
    let output = show.output().unwrap();
    assert!(output.status.success());
    assert!(
        contains(&output, "正式版"),
        "默认渠道应为正式版，实际: {:?}",
        output
    );
    assert!(!env.channel_file().exists(), "只查询渠道不应写状态文件");

    let mut set = env.base_command();
    set.args(["channel", "dev"]);
    let output = set.output().unwrap();
    assert!(
        output.status.success(),
        "切换渠道失败\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(contains(&output, "预发布版"));
    assert_eq!(
        fs::read_to_string(env.channel_file()).unwrap().trim(),
        "dev"
    );

    let mut bad = env.base_command();
    bad.args(["channel", "nonsense"]);
    assert!(!bad.output().unwrap().status.success(), "非法渠道应失败");
    assert_eq!(
        fs::read_to_string(env.channel_file()).unwrap().trim(),
        "dev",
        "非法渠道不应改动状态文件"
    );

    let install = env.run("agent", "install");
    assert!(contains(&install, "发布渠道: 预发布版"));
    assert!(contains(&install, "已安装 dsh-remote"));
    let status = env.run("agent", "status");
    assert!(contains(&status, "渠道: 预发布版"), "status: {status:?}");

    let update = env.run("agent", "update");
    assert!(
        !contains(&update, "已是最新版本"),
        "预发布渠道不做版本比较，应重新下载: {update:?}"
    );
    assert!(contains(&update, "已更新"));

    let mut stable = env.command("agent", "update");
    stable.env("DSH_REMOTE_CHANNEL", "stable");
    let output = stable.output().unwrap();
    assert!(
        output.status.success(),
        "按渠道变量更新失败\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(contains(&output, "发布渠道: 正式版"));
    assert!(contains(&output, "已是最新版本"));
    assert_eq!(
        fs::read_to_string(env.channel_file()).unwrap().trim(),
        "stable"
    );

    env.run("agent", "stop");
}
