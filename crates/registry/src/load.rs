//! 从目录加载描述文件，并支持不重启重载。
//!
//! 重载是原子替换：新快照全部校验通过才切换，任一文件出错则保留旧快照。
//! 半份目录比旧目录更危险。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::compile::{Loaded, compile};
use crate::error::RegistryError;
use crate::hook::HookRegistry;
use crate::schema::Descriptor;

/// 读取一个目录下的全部 `*.yaml` 描述文件。
///
/// # Errors
/// 目录不可读、任一文件解析失败或校验不通过时返回错误。
pub fn load_dir(dir: &Path, hooks: &HookRegistry, version: u64) -> Result<Loaded, RegistryError> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .map_err(|source| RegistryError::Io {
            path: dir.display().to_string(),
            source,
        })?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "yaml" || e == "yml"))
        .collect();
    // 目录序不稳定，排序让错误信息与路由冲突的归因可复现
    files.sort();

    let mut descriptors = Vec::with_capacity(files.len());
    for path in &files {
        descriptors.push(parse_file(path)?);
    }
    compile(descriptors, hooks, version)
}

/// 解析单份描述文件。
///
/// # Errors
/// 文件不可读或不合 schema 时返回错误，错误信息带行列位置。
pub fn parse_file(path: &Path) -> Result<Descriptor, RegistryError> {
    let text = std::fs::read_to_string(path).map_err(|source| RegistryError::Io {
        path: path.display().to_string(),
        source,
    })?;
    parse_str(&text).map_err(|message| RegistryError::Parse {
        path: path.display().to_string(),
        message,
    })
}

/// 解析描述文件文本。错误信息由 `serde-saphyr` 提供，含行列与出错字段。
///
/// # Errors
/// 不是合法 YAML、或不合 schema 时返回带位置的错误信息。
pub fn parse_str(text: &str) -> Result<Descriptor, String> {
    serde_saphyr::from_str(text).map_err(|e| e.to_string())
}

/// 目录的活动快照。热更新原子替换，在途请求持有发起时的 `Arc`。
#[derive(Debug)]
pub struct Registry {
    dir: PathBuf,
    hooks: HookRegistry,
    current: arc_swap::ArcSwap<crate::catalog::Catalog>,
    version: AtomicU64,
}

impl Registry {
    /// 首次加载。
    ///
    /// # Errors
    /// 加载或校验失败时返回错误——启动期没有旧快照可退回。
    pub fn open(dir: impl Into<PathBuf>, hooks: HookRegistry) -> Result<Self, RegistryError> {
        let dir = dir.into();
        let loaded = load_dir(&dir, &hooks, 1)?;
        report_deferred(&loaded);
        Ok(Self {
            dir,
            hooks,
            current: arc_swap::ArcSwap::from_pointee(loaded.catalog),
            version: AtomicU64::new(1),
        })
    }

    #[must_use]
    pub fn snapshot(&self) -> Arc<crate::catalog::Catalog> {
        self.current.load_full()
    }

    /// 重新加载。失败时保留旧快照并把错误交给调用方告警。
    ///
    /// # Errors
    /// 新目录校验不通过时返回错误，此时活动快照不变。
    pub fn reload(&self) -> Result<u64, RegistryError> {
        let next = self.version.load(Ordering::Relaxed) + 1;
        let loaded = load_dir(&self.dir, &self.hooks, next)?;
        report_deferred(&loaded);
        self.current.store(Arc::new(loaded.catalog));
        self.version.store(next, Ordering::Relaxed);
        Ok(next)
    }
}

fn report_deferred(loaded: &Loaded) {
    for d in &loaded.deferred {
        let needs = d.gaps.iter().map(|g| g.needs).max();
        tracing::warn!(
            provider = d.provider.as_str(),
            endpoint = %d.endpoint,
            needs = ?needs,
            fields = ?d.gaps.iter().map(|g| g.field).collect::<Vec<_>>(),
            "端点已声明但当前解释器未实现，不挂入站路由"
        );
    }
}
