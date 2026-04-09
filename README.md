# repodata_diff.py

一个小工具：**对比两次 `repodata`（准确说是两份 `repomd.xml` 所指向的 `primary.xml.*`）**，列出增量 RPM 包（新增/删除/版本变化）。

## 为什么需要“两次 repodata”

仓库地址（示例）是一个“当前最新”的 YUM 源：

- `https://update.cs2c.com.cn/NS/V10/V10SP3-2403/os/adv/lic/updates/aarch64/Packages/`

要看到“增量”，你需要两份不同时刻的元数据（例如昨天 vs 今天），也就是：

- **旧的 repodata（old）**
- **新的 repodata（new）**

这两份可以来自：

- 两个不同的 URL（例如你自己保存了两个快照目录并通过 HTTP 访问）
- 或者两个本地目录（推荐：你把仓库元数据下载两份保存起来）

## 用法

### 1) 对比两个本地快照目录（推荐）

假设你把同一个 repo 在两个时间点保存成：

- `/data/snapshots/repo_2026-02-01/`
- `/data/snapshots/repo_2026-02-06/`

它们目录里都包含 `repodata/repomd.xml` 以及 `repodata/*-primary.xml.*` 等文件。

运行：

```bash
python3 repodata_diff.py \
  --old /data/snapshots/repo_2026-02-01 \
  --new /data/snapshots/repo_2026-02-06
```

如需同时输出删除/版本变化：

```bash
python3 repodata_diff.py \
  --old /data/snapshots/repo_2026-02-01 \
  --new /data/snapshots/repo_2026-02-06 \
  --show-removed \
  --show-changed
```

### 2) 直接对比两个 URL（当你确实有两个 URL 时）

```bash
python3 repodata_diff.py \
  --old https://example.com/repo_snap_1/ \
  --new https://example.com/repo_snap_2/
```

### 3) 也可以直接传 `repodata/repomd.xml`

```bash
python3 repodata_diff.py \
  --old /data/snapshots/repo_2026-02-01/repodata/repomd.xml \
  --new /data/snapshots/repo_2026-02-06/repodata/repomd.xml
```

### 4) JSON 输出（便于二次处理）

```bash
python3 repodata_diff.py --old OLD --new NEW --json > diff.json
```

### 5) `--old` / `--new` 直接传归档文件
现在不需要额外参数，`--old` 或 `--new` 传本地归档时会自动识别，并仅解压包内 `repodata/*` 参与对比。

支持格式：
- `zip`
- `tar`
- `tar.gz` / `tgz`
- `tar.xz` / `txz`
- `tar.bz2` / `tbz2` / `tbz`

示例：

```bash
python3 repodata_diff.py \
  --old /data/snapshots/repo_old.tar.xz \
  --new /data/snapshots/repo_new.zip
```

## 输出说明

默认只输出 **Added（新增）**，每行只显示 RPM 文件名，例如：

- `bind-9.11.21-22.p01.ky10.x86_64.rpm`

`--show-removed` 会输出 **Removed（旧有新无）**。

`--show-changed` 会输出 **Changed (latest by name+arch)**：按 `(name, arch)` 聚合后，“旧最新”和“新最新”不一致的条目（用于快速看升级/降级）。

> 说明：脚本为了保持纯标准库实现，版本比较对 `ver/rel` 使用了简化的字典序策略；多数常见版本号格式下可用，但它不是严格的 RPM version compare。

## 适配的 repodata 格式

- `repomd.xml`
- `primary.xml.gz` / `primary.xml.xz` / `primary.xml.bz2` / `primary.xml`（脚本会根据后缀自动解压）

## 下载 repodata 和新增 RPM
如果你希望把“新仓库的 repodata + Added 的 RPM 包”一起下载到本地，使用 `--download`：

```bash
python3 repodata_diff.py \
  --old old --new new \
  --download \
  --dir ./repo
```

说明：
- 下载来源固定为 `--new`
- `--dir` 默认是 `repo`
- 下载目录不存在时会自动创建
- 下载内容会保持常见目录结构，例如 `repo/repodata/repomd.xml`、`repo/Packages/*.rpm`

## `--compress` + `--download` 压缩并清理目录
当指定 `--download` 后，可使用 `--compress` 对 `--dir` 目录进行压缩。

支持格式：
- `zip`
- `gz`（生成 `.tar.gz`）
- `xz`（生成 `.tar.xz`）
- `bz2`（生成 `.tar.bz2`）

行为：
- 压缩文件输出在 `--dir` 的父目录，文件名与目录同名
- 压缩成功后会自动删除原始 `--dir` 目录
- `--compress` 必须与 `--download` 一起使用

示例：

```bash
python3 repodata_diff.py \
  --old old.tar.gz \
  --new https://example.com/repo/ \
  --download \
  --dir ./repo \
  --compress xz
```

