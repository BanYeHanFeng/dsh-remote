use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use crate::proto::params::FsEntry;

pub const ROOT_INO: u64 = 1;
const MAX_NODES: usize = 100_000;
const MAX_DIRS: usize = 2_048;

#[derive(Clone, Debug)]
pub struct Cached {
    pub entry: FsEntry,
    pub at: Instant,
}

pub struct DirCache {
    pub entries: Vec<FsEntry>,
    pub at: Instant,
}

struct Node {
    path: String,
    attr: Option<Cached>,
}

pub struct Tree {
    root: String,
    ttl: Duration,
    nodes: HashMap<u64, Node>,
    by_path: HashMap<String, u64>,
    order: VecDeque<u64>,
    dirs: HashMap<String, DirCache>,
}

impl Tree {
    pub fn new(root: String, ttl: Duration) -> Self {
        let mut tree = Self {
            root: normalize_root(root),
            ttl,
            nodes: HashMap::new(),
            by_path: HashMap::new(),
            order: VecDeque::new(),
            dirs: HashMap::new(),
        };
        let root = tree.root.clone();
        tree.intern(&root);
        tree
    }

    pub fn root(&self) -> &str {
        &self.root
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    pub fn intern(&mut self, path: &str) -> u64 {
        if let Some(ino) = self.by_path.get(path) {
            return *ino;
        }
        let ino = if path == self.root {
            ROOT_INO
        } else {
            let mut candidate = hash_path(path);
            while self.nodes.contains_key(&candidate) || candidate == ROOT_INO || candidate == 0 {
                candidate = candidate.wrapping_add(1);
            }
            candidate
        };
        self.nodes.insert(
            ino,
            Node {
                path: path.to_string(),
                attr: None,
            },
        );
        self.by_path.insert(path.to_string(), ino);
        self.order.push_back(ino);
        self.evict();
        ino
    }

    pub fn path(&self, ino: u64) -> Option<String> {
        self.nodes.get(&ino).map(|node| node.path.clone())
    }

    pub fn attr(&self, path: &str) -> Option<FsEntry> {
        let node = self.nodes.get(self.by_path.get(path)?)?;
        let cached = node.attr.as_ref()?;
        if cached.at.elapsed() > self.ttl {
            return None;
        }
        Some(cached.entry.clone())
    }

    pub fn set_attr(&mut self, path: &str, entry: FsEntry) {
        let ino = self.intern(path);
        if let Some(node) = self.nodes.get_mut(&ino) {
            node.attr = Some(Cached {
                entry,
                at: Instant::now(),
            });
        }
    }

    pub fn clear_attr(&mut self, path: &str) {
        if let Some(ino) = self.by_path.get(path).copied() {
            if let Some(node) = self.nodes.get_mut(&ino) {
                node.attr = None;
            }
        }
    }

    pub fn dir(&self, path: &str) -> Option<&DirCache> {
        let cached = self.dirs.get(path)?;
        if cached.at.elapsed() > self.ttl {
            return None;
        }
        Some(cached)
    }

    pub fn set_dir(&mut self, path: &str, entries: Vec<FsEntry>) {
        if self.dirs.len() >= MAX_DIRS {
            if let Some(oldest) = self
                .dirs
                .iter()
                .min_by_key(|(_, cache)| cache.at)
                .map(|(path, _)| path.clone())
            {
                self.dirs.remove(&oldest);
            }
        }
        for entry in &entries {
            self.set_attr(&join(path, &entry.name), entry.clone());
        }
        self.dirs.insert(
            path.to_string(),
            DirCache {
                entries,
                at: Instant::now(),
            },
        );
    }

    pub fn insert_child(&mut self, parent: &str, entry: FsEntry) {
        self.set_attr(&join(parent, &entry.name), entry.clone());
        let Some(cache) = self.dirs.get_mut(parent) else {
            return;
        };
        match cache
            .entries
            .iter_mut()
            .find(|existing| existing.name == entry.name)
        {
            Some(existing) => *existing = entry,
            None => {
                cache.entries.push(entry);
                cache
                    .entries
                    .sort_by(|left, right| left.name.cmp(&right.name));
            }
        }
    }

    pub fn remove_child(&mut self, parent: &str, name: &str) {
        self.clear_attr(&join(parent, name));
        if let Some(cache) = self.dirs.get_mut(parent) {
            cache.entries.retain(|entry| entry.name != name);
        }
    }

    pub fn invalidate(&mut self, path: &str) {
        self.clear_attr(path);
        let parent = self.parent_or_root(path);
        self.dirs.remove(&parent);
        if let Some(ino) = self.by_path.get(path).copied() {
            if let Some(node) = self.nodes.get_mut(&ino) {
                node.attr = None;
            }
        }
    }

    pub fn rename_path(&mut self, from: &str, to: &str) {
        let prefix = format!("{}/", from.trim_end_matches('/'));
        let moves: Vec<(u64, String)> = self
            .nodes
            .iter()
            .filter(|(_, node)| node.path == from || node.path.starts_with(&prefix))
            .map(|(ino, node)| (*ino, node.path.clone()))
            .collect();
        for (ino, old) in moves {
            let new_path = if old == from {
                to.to_string()
            } else {
                format!("{}{}", to, &old[from.len()..])
            };
            self.by_path.remove(&old);
            if let Some(node) = self.nodes.get_mut(&ino) {
                node.path = new_path.clone();
            }
            self.by_path.insert(new_path, ino);
        }
        self.dirs
            .retain(|key, _| !(key == from || key.starts_with(&prefix)));
        self.dirs.remove(to);
        let from_parent = self.parent_or_root(from);
        let to_parent = self.parent_or_root(to);
        self.dirs.remove(&from_parent);
        self.dirs.remove(&to_parent);
    }

    pub fn invalidate_subtree(&mut self, path: &str) {
        self.invalidate(path);
        self.dirs.remove(path);
        let prefix = format!("{}/", path.trim_end_matches('/'));
        self.dirs.retain(|key, _| !key.starts_with(&prefix));
        let stale: Vec<u64> = self
            .nodes
            .iter()
            .filter(|(_, node)| node.path.starts_with(&prefix))
            .map(|(ino, _)| *ino)
            .collect();
        for ino in stale {
            if let Some(node) = self.nodes.remove(&ino) {
                self.by_path.remove(&node.path);
            }
        }
    }

    pub fn child(&self, parent: &str, name: &str) -> Option<String> {
        match name {
            "." => Some(parent.to_string()),
            ".." => Some(self.parent_or_root(parent)),
            _ if name.contains('/') || name.is_empty() => None,
            _ => Some(join(parent, name)),
        }
    }

    pub fn parent_or_root(&self, path: &str) -> String {
        if path == self.root {
            return self.root.clone();
        }
        let candidate = match path.rfind('/') {
            Some(0) | None => "/".to_string(),
            Some(index) => path[..index].to_string(),
        };
        if self.contains(&candidate) {
            candidate
        } else {
            self.root.clone()
        }
    }

    fn contains(&self, path: &str) -> bool {
        if self.root == "/" {
            return path.starts_with('/');
        }
        path == self.root || path.starts_with(&format!("{}/", self.root))
    }

    fn evict(&mut self) {
        while self.nodes.len() > MAX_NODES {
            let Some(ino) = self.order.pop_front() else {
                break;
            };
            if ino == ROOT_INO {
                self.order.push_back(ino);
                continue;
            }
            if let Some(node) = self.nodes.remove(&ino) {
                self.by_path.remove(&node.path);
                let prefix = format!("{}/", node.path.trim_end_matches('/'));
                self.dirs.retain(|key, _| !key.starts_with(&prefix));
            }
        }
    }
}

fn normalize_root(root: String) -> String {
    let trimmed = root.trim_end_matches('/');
    if trimmed.is_empty() {
        "/".to_string()
    } else {
        trimmed.to_string()
    }
}

fn join(parent: &str, name: &str) -> String {
    if parent == "/" {
        format!("/{name}")
    } else {
        format!("{parent}/{name}")
    }
}

fn hash_path(path: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in path.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(name: &str, kind: &str) -> FsEntry {
        FsEntry {
            name: name.to_string(),
            kind: kind.to_string(),
            ..FsEntry::default()
        }
    }

    #[test]
    fn root_gets_fixed_ino() {
        let tree = Tree::new("/data".into(), Duration::from_secs(1));
        assert_eq!(tree.path(ROOT_INO).as_deref(), Some("/data"));
        assert_eq!(tree.root(), "/data");
    }

    #[test]
    fn inode_is_stable_per_path() {
        let mut tree = Tree::new("/data".into(), Duration::from_secs(1));
        let first = tree.intern("/data/a.txt");
        let second = tree.intern("/data/a.txt");
        assert_eq!(first, second);
        assert_eq!(tree.path(first).as_deref(), Some("/data/a.txt"));
    }

    #[test]
    fn joins_children_and_clamps_parent() {
        let tree = Tree::new("/data".into(), Duration::from_secs(1));
        assert_eq!(tree.child("/data", "a").as_deref(), Some("/data/a"));
        assert_eq!(tree.child("/data/a", "..").as_deref(), Some("/data"));
        assert_eq!(tree.child("/data", "..").as_deref(), Some("/data"));
        assert_eq!(tree.child("/data", ".").as_deref(), Some("/data"));
        assert_eq!(tree.child("/data", "a/b"), None);
        assert_eq!(tree.child("/data", ""), None);
        assert_eq!(tree.parent_or_root("/data/a/b"), "/data/a");
        assert_eq!(tree.parent_or_root("/"), "/data");
    }

    #[test]
    fn attributes_expire_and_can_be_cleared() {
        let mut tree = Tree::new("/data".into(), Duration::from_millis(20));
        tree.set_attr("/data/a", entry("a", "file"));
        assert_eq!(tree.attr("/data/a").unwrap().kind, "file");
        tree.clear_attr("/data/a");
        assert!(tree.attr("/data/a").is_none());
        tree.set_attr("/data/a", entry("a", "file"));
        std::thread::sleep(Duration::from_millis(30));
        assert!(tree.attr("/data/a").is_none());
    }

    #[test]
    fn dir_listing_fills_child_attributes() {
        let mut tree = Tree::new("/data".into(), Duration::from_secs(1));
        tree.set_dir("/data", vec![entry("a", "file"), entry("b", "dir")]);
        assert_eq!(tree.dir("/data").unwrap().entries.len(), 2);
        assert_eq!(tree.attr("/data/b").unwrap().kind, "dir");
        tree.invalidate("/data/b");
        assert!(tree.attr("/data/b").is_none());
        assert!(tree.dir("/data").is_none());
    }

    #[test]
    fn child_entries_follow_mutations() {
        let mut tree = Tree::new("/data".into(), Duration::from_secs(1));
        tree.set_dir("/data", vec![entry("a", "file")]);
        tree.insert_child("/data", entry("b", "file"));
        let names: Vec<_> = tree
            .dir("/data")
            .unwrap()
            .entries
            .iter()
            .map(|entry| entry.name.clone())
            .collect();
        assert_eq!(names, vec!["a", "b"]);
        assert_eq!(tree.attr("/data/b").unwrap().kind, "file");
        tree.remove_child("/data", "a");
        assert_eq!(tree.dir("/data").unwrap().entries.len(), 1);
        assert!(tree.attr("/data/a").is_none());
    }

    #[test]
    fn rename_keeps_inode_identity() {
        let mut tree = Tree::new("/data".into(), Duration::from_secs(1));
        let ino = tree.intern("/data/a.txt");
        tree.set_attr("/data/a.txt", entry("a.txt", "file"));
        tree.set_dir("/data", vec![entry("a.txt", "file")]);
        tree.rename_path("/data/a.txt", "/data/b.txt");
        assert_eq!(tree.path(ino).as_deref(), Some("/data/b.txt"));
        assert_eq!(tree.attr("/data/b.txt").unwrap().kind, "file");
        assert!(tree.dir("/data").is_none());
    }

    #[test]
    fn rename_moves_a_whole_subtree() {
        let mut tree = Tree::new("/data".into(), Duration::from_secs(1));
        let child = tree.intern("/data/old/x.txt");
        tree.rename_path("/data/old", "/data/new");
        assert_eq!(tree.path(child).as_deref(), Some("/data/new/x.txt"));
    }

    #[test]
    fn subtree_invalidation_drops_children() {
        let mut tree = Tree::new("/data".into(), Duration::from_secs(1));
        let ino = tree.intern("/data/x/a");
        tree.set_attr("/data/x/a", entry("a", "file"));
        tree.set_dir("/data/x", vec![entry("a", "file")]);
        tree.invalidate_subtree("/data/x");
        assert!(tree.path(ino).is_none());
        assert!(tree.dir("/data/x").is_none());
    }
}
