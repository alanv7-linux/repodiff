#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
repodata_diff.py

对比两份 YUM/DNF 仓库的 repodata，列出增量 RPM 包（新增/删除/变更）。

支持输入：
- 仓库根目录/URL（脚本会自动拼出 repodata/repomd.xml）
- 或直接输入 repodata/repomd.xml 的本地路径/URL
- 本地归档文件（zip/tar/tar.gz/tar.xz/tar.bz2 等）：自动仅解压包内 repodata 参与对比

原理：
repomd.xml -> 找到 <data type="primary"> 的 location href -> 下载/读取 primary.xml.* -> 解析出每个包的 NEVRA 与 rpm 路径 -> 做集合差分。
"""

from __future__ import annotations

import argparse
import bz2
import gzip
import json
import lzma
import os
import posixpath
import sys
import tarfile
import tempfile
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass
import shutil
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple


USER_AGENT = "repodata-diff/1.0 (+python urllib)"
SUPPORTED_ARCHIVE_SUFFIXES = (
    ".zip",
    ".tar",
    ".tar.gz",
    ".tgz",
    ".tar.xz",
    ".txz",
    ".tar.bz2",
    ".tbz2",
    ".tbz",
)


def _is_url(s: str) -> bool:
    try:
        p = urllib.parse.urlparse(s)
        return p.scheme in ("http", "https")
    except Exception:
        return False


def _ensure_trailing_slash(url: str) -> str:
    return url if url.endswith("/") else url + "/"


def _join_url(base: str, rel: str) -> str:
    # urljoin 在 base 不以 / 结尾时会丢掉最后一段路径，所以先确保 /
    return urllib.parse.urljoin(_ensure_trailing_slash(base), rel)


def _render_progress_bar(current: int, total: int, width: int = 24) -> str:
    if total <= 0:
        return "[unknown]"
    ratio = min(max(current / total, 0.0), 1.0)
    done = int(ratio * width)
    return "[" + "#" * done + "-" * (width - done) + "]"


def _zip_normalize_member_name(name: str) -> str:
    return name.replace("\\", "/").rstrip("/")


def _find_repodata_prefix(member_names: Sequence[str], archive_desc: str) -> str:
    """
    在 zip 成员列表中定位「仓库根」相对前缀，使得存在 {prefix}/repodata/repomd.xml。
    prefix 为空表示 zip 根下即为 repodata/repomd.xml。
    若有多个匹配，取路径层级最浅的一个。
    """
    best: Optional[str] = None
    best_depth = 10**9
    for raw in member_names:
        norm = _zip_normalize_member_name(raw)
        if not norm or norm.endswith("/"):
            continue
        if not norm.endswith("repodata/repomd.xml"):
            continue
        prefix = norm[: -len("repodata/repomd.xml")].rstrip("/")
        depth = 0 if not prefix else len(prefix.split("/"))
        if best is None or depth < best_depth or (depth == best_depth and prefix < best):
            best_depth = depth
            best = prefix
    if best is None:
        raise ValueError(
            f"在 {archive_desc} 中未找到 repodata/repomd.xml。\n"
            "请确认压缩包内路径形如：.../repodata/repomd.xml（或根目录 repodata/repomd.xml）"
        )
    return best


def _safe_target_path(dest_root_abs: str, member_norm: str) -> str:
    """防止解压越界：目标路径必须在 dest_root 之下。"""
    parts = [p for p in member_norm.split("/") if p and p != "."]
    target = dest_root_abs
    for p in parts:
        if p == "..":
            raise ValueError(f"非法归档路径: {member_norm!r}")
        target = os.path.join(target, p)
    target_abs = os.path.abspath(target)
    root_abs = os.path.abspath(dest_root_abs)
    if target_abs != root_abs and not target_abs.startswith(root_abs + os.sep):
        raise ValueError(f"非法归档路径（越界）: {member_norm!r}")
    return target_abs


def _is_supported_archive(path: str) -> bool:
    lower = path.lower()
    return any(lower.endswith(suf) for suf in SUPPORTED_ARCHIVE_SUFFIXES)


def _extract_repodata_from_archive(archive_path: str, dest_root: str) -> str:
    """
    仅从本地归档中解压 repodata 目录树到 dest_root，保持包内相对路径。
    支持 zip/tar/tar.gz/tar.xz/tar.bz2 及常见缩写后缀。
    返回可供 _resolve_repomd_source 使用的「仓库根」本地路径（含解压出的前缀目录）。
    """
    arc_abs = os.path.abspath(archive_path)
    if not os.path.isfile(arc_abs):
        raise FileNotFoundError(f"找不到归档文件: {arc_abs}")
    dest_abs = os.path.abspath(dest_root)
    os.makedirs(dest_abs, exist_ok=True)

    lower = arc_abs.lower()
    if lower.endswith(".zip"):
        with zipfile.ZipFile(arc_abs, "r") as zf:
            prefix = _find_repodata_prefix(zf.namelist(), "zip")
            repodata_prefix = f"{prefix}/repodata/" if prefix else "repodata/"

            for info in zf.infolist():
                name_norm = _zip_normalize_member_name(info.filename)
                if not name_norm or name_norm.endswith("/"):
                    continue
                if not name_norm.startswith(repodata_prefix):
                    continue
                out_path = _safe_target_path(dest_abs, name_norm)
                os.makedirs(os.path.dirname(out_path), exist_ok=True)
                with zf.open(info, "r") as src, open(out_path, "wb") as out:
                    shutil.copyfileobj(src, out)
    else:
        with tarfile.open(arc_abs, "r:*") as tf:
            names = [_zip_normalize_member_name(m.name) for m in tf.getmembers()]
            prefix = _find_repodata_prefix(names, "tar")
            repodata_prefix = f"{prefix}/repodata/" if prefix else "repodata/"

            for member in tf.getmembers():
                if not member.isfile():
                    continue
                name_norm = _zip_normalize_member_name(member.name)
                if not name_norm or not name_norm.startswith(repodata_prefix):
                    continue
                out_path = _safe_target_path(dest_abs, name_norm)
                os.makedirs(os.path.dirname(out_path), exist_ok=True)
                src = tf.extractfile(member)
                if src is None:
                    continue
                with src, open(out_path, "wb") as out:
                    shutil.copyfileobj(src, out)

    if prefix:
        return os.path.join(dest_abs, *prefix.split("/"))
    return dest_abs


def _compress_directory_next_to_parent(dir_path: str, compress_format: str) -> str:
    """
    将 dir_path 整棵目录压缩，文件名与目录 basename 相同，放在 dir_path 的父目录下。
    支持: zip / gz / xz / bz2
    """
    root = os.path.abspath(dir_path)
    if not os.path.isdir(root):
        raise NotADirectoryError(root)
    parent = os.path.dirname(root)
    base = os.path.basename(root.rstrip(os.sep))
    fmt = compress_format.strip().lower()

    if fmt == "zip":
        out_path = os.path.join(parent, f"{base}.zip")
        with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for walk_root, _dirs, files in os.walk(root):
                for fn in files:
                    full = os.path.join(walk_root, fn)
                    arc = os.path.relpath(full, parent)
                    zf.write(full, arc.replace(os.sep, "/"))
        return out_path

    mode_map = {
        "gz": ("w:gz", ".tar.gz"),
        "xz": ("w:xz", ".tar.xz"),
        "bz2": ("w:bz2", ".tar.bz2"),
    }
    if fmt not in mode_map:
        raise ValueError(f"不支持的压缩格式: {compress_format}")

    mode, suffix = mode_map[fmt]
    out_path = os.path.join(parent, f"{base}{suffix}")
    with tarfile.open(out_path, mode) as tf:
        tf.add(root, arcname=base)
    return out_path


def _resolve_repomd_source(src: str) -> str:
    """
    把用户输入规范化成「repodata/repomd.xml」的 URL 或本地路径。
    
    支持的输入格式：
    - 仓库根目录（如 old/ 或 new/），会自动查找 repodata/repomd.xml
    - repodata 目录（如 old/repodata/），会自动查找 repomd.xml
    - 直接指定 repomd.xml 文件路径（如 old/repodata/repomd.xml）
    - 仓库 URL（同上规则）
    
    如果路径不存在或格式不正确，会抛出 FileNotFoundError 或 ValueError。
    """
    src = src.strip()

    if _is_url(src):
        # 很多人会把 .../Packages/ 当作"仓库地址"贴过来；实际 repodata 在上一级
        u = urllib.parse.urlparse(src)
        path = u.path
        # 处理 /Packages 或 /Packages/ 两种
        path_no_slash = path[:-1] if path.endswith("/") else path
        if posixpath.basename(path_no_slash) == "Packages":
            parent = posixpath.dirname(path_no_slash) + "/"
            src = urllib.parse.urlunparse((u.scheme, u.netloc, parent, "", "", ""))

        if src.endswith("repodata/repomd.xml"):
            return src
        # 用户可能给的是 .../repodata/ 或仓库根目录
        if src.endswith("repodata/"):
            return _join_url(src, "repomd.xml")
        return _join_url(src, "repodata/repomd.xml")

    # 本地路径
    src_abs = os.path.abspath(src)
    
    # 如果是目录
    if os.path.isdir(src_abs):
        # 处理 .../Packages 目录输入
        if os.path.basename(src_abs) == "Packages":
            src_abs = os.path.dirname(src_abs)
        
        # 检查是否是 repodata 目录
        if os.path.basename(src_abs) == "repodata":
            repomd_path = os.path.join(src_abs, "repomd.xml")
            if os.path.exists(repomd_path):
                return repomd_path
            raise FileNotFoundError(
                f"在 repodata 目录中未找到 repomd.xml: {src_abs}\n"
                f"请确认这是一个有效的 YUM/DNF 仓库 repodata 目录"
            )
        
        # 否则当作仓库根目录，查找 repodata/repomd.xml
        repomd_path = os.path.join(src_abs, "repodata", "repomd.xml")
        if os.path.exists(repomd_path):
            return repomd_path
        
        # 如果不存在，检查是否有 repodata 目录
        repodata_dir = os.path.join(src_abs, "repodata")
        if os.path.isdir(repodata_dir):
            raise FileNotFoundError(
                f"在仓库目录中找到 repodata 目录，但未找到 repomd.xml: {repomd_path}\n"
                f"仓库目录: {src_abs}\n"
                f"repodata 目录: {repodata_dir}"
            )
        else:
            raise FileNotFoundError(
                f"在目录中未找到 repodata 目录: {src_abs}\n"
                f"请确认这是一个有效的 YUM/DNF 仓库根目录"
            )
    
    # 如果是文件路径
    if os.path.isfile(src_abs):
        # 检查是否是 repomd.xml
        if os.path.basename(src_abs) == "repomd.xml":
            return src_abs
        else:
            raise ValueError(
                f"指定的文件不是 repomd.xml: {src_abs}\n"
                f"请指定 repomd.xml 文件路径，或包含 repodata 的目录"
            )
    
    # 如果路径不存在
    raise FileNotFoundError(
        f"路径不存在: {src}\n"
        f"请指定：\n"
        f"  - 仓库根目录（包含 repodata/ 子目录）\n"
        f"  - repodata 目录\n"
        f"  - repomd.xml 文件路径\n"
        f"  - 仓库 URL"
    )


def _download_to_file(url: str, dst_path: str) -> None:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req) as resp, open(dst_path, "wb") as f:
        # 8MB chunk
        while True:
            chunk = resp.read(8 * 1024 * 1024)
            if not chunk:
                break
            f.write(chunk)


def _parse_repomd_for_all_data_hrefs(repomd_local_path: str) -> List[str]:
    """
    从 repomd.xml 中解析所有 data/location 的 href。
    返回值为原始 href（相对 repo base 的路径通常如 repodata/xxx.xml.gz）。
    """
    tree = ET.parse(repomd_local_path)
    root = tree.getroot()

    hrefs: List[str] = []
    seen: Set[str] = set()

    # 优先：找 data->location
    for data in root.findall(".//{*}data"):
        loc = data.find("{*}location")
        if loc is None:
            continue
        href = (loc.attrib.get("href") or "").strip()
        if href and href not in seen:
            hrefs.append(href)
            seen.add(href)

    if hrefs:
        return hrefs

    # 兜底：找任意 location@href
    for elem in root.iter():
        if _strip_ns(elem.tag) != "location":
            continue
        href = (elem.attrib.get("href") or "").strip()
        if href and href not in seen:
            hrefs.append(href)
            seen.add(href)
    return hrefs


def _download_added_and_repodata_from_new_repo(
    new_src: str,
    download_dir: str,
    added_items: Sequence[dict],
    *,
    quiet: bool = False,
) -> None:
    """
    下载 Added RPM + repodata（repomd.xml 引用的所有 data/location 文件），来源从 --new。

    目标目录结构（保持 YUM 常见相对路径）：
    - <download_dir>/repodata/repomd.xml
    - <download_dir>/repodata/<xxx-primary.xml.gz|...>
    - <download_dir>/Packages/<xxx.rpm>
    """
    if not quiet:
        print("[下载] 步骤 1/4：准备下载目录", file=sys.stderr)
    os.makedirs(download_dir, exist_ok=True)

    repomd_src = _resolve_repomd_source(new_src)
    base = _repo_base_from_repomd_src(repomd_src)
    is_url = _is_url(repomd_src)
    if not quiet:
        print(f"[下载] 来源: {repomd_src}", file=sys.stderr)

    # 解析 repomd.xml 引用的所有 data/location href
    if not quiet:
        print("[下载] 步骤 2/4：读取 repomd 元数据清单", file=sys.stderr)
    if is_url:
        with tempfile.TemporaryDirectory(prefix="repodata_down_new_") as tmpdir:
            repomd_local = _fetch_to_tempfile(repomd_src, tmpdir)
            hrefs = _parse_repomd_for_all_data_hrefs(repomd_local)
    else:
        hrefs = _parse_repomd_for_all_data_hrefs(repomd_src)

    downloaded = 0
    skipped = 0
    failed = 0

    repodata_dir_dst = os.path.join(download_dir, "repodata")
    packages_dir_dst = os.path.join(download_dir, "Packages")
    os.makedirs(repodata_dir_dst, exist_ok=True)
    os.makedirs(packages_dir_dst, exist_ok=True)

    if not quiet:
        print("[下载] 步骤 3/4：下载 repodata 文件", file=sys.stderr)

    # repomd.xml
    repomd_dst = os.path.join(repodata_dir_dst, "repomd.xml")
    if os.path.exists(repomd_dst) and os.path.getsize(repomd_dst) > 0:
        skipped += 1
    else:
        try:
            if is_url:
                _download_to_file(repomd_src, repomd_dst)
            else:
                shutil.copy2(repomd_src, repomd_dst)
            downloaded += 1
        except Exception:
            failed += 1
            if not quiet:
                print(f"[下载/复制失败] repomd.xml: {new_src}", file=sys.stderr)

    def _save_href(href: str, content_kind: str) -> None:
        nonlocal downloaded, skipped, failed
        href_norm = (href or "").strip().lstrip("/")
        if not href_norm:
            return

        # repodata 元数据文件：确保落盘在 repodata/ 下
        if content_kind == "repodata":
            rel = href_norm if href_norm.startswith("repodata/") else posixpath.join("repodata", href_norm)
        else:
            rel = href_norm if href_norm.startswith("Packages/") else posixpath.join("Packages", href_norm)

        dst_path = os.path.join(download_dir, *rel.split("/"))
        os.makedirs(os.path.dirname(dst_path), exist_ok=True)

        if os.path.exists(dst_path) and os.path.getsize(dst_path) > 0:
            skipped += 1
            return

        try:
            if is_url:
                url = _join_url(_ensure_trailing_slash(base), rel)
                _download_to_file(url, dst_path)
            else:
                src_rel = rel  # 相对 repo base，通常包含 repodata/... 或 Packages/...
                src_norm = src_rel.replace("/", os.sep)
                src_path = os.path.join(base, src_norm)
                if not os.path.exists(src_path):
                    # 兜底：如果 href 原本不带 repodata/ 前缀，尝试 base/repodata
                    if content_kind == "repodata" and not href_norm.startswith("repodata/"):
                        src_path2 = os.path.join(base, "repodata", posixpath.basename(href_norm))
                        if os.path.exists(src_path2):
                            shutil.copy2(src_path2, dst_path)
                            downloaded += 1
                            return
                    raise FileNotFoundError(src_path)
                shutil.copy2(src_path, dst_path)
            downloaded += 1
        except Exception:
            failed += 1
            if not quiet:
                print(f"[下载/复制失败] {rel}", file=sys.stderr)

    # 下载 repodata 里引用的所有 metadata 文件
    total_meta = len(hrefs)
    for idx, href in enumerate(hrefs, start=1):
        if not quiet:
            print(f"[下载][repodata] {idx}/{total_meta}: {href}", file=sys.stderr)
        _save_href(href, "repodata")

    # 下载 Added RPM
    if not quiet:
        print("[下载] 步骤 4/4：下载 Added RPM", file=sys.stderr)
    total_rpm = len(added_items)
    for idx, item in enumerate(added_items, start=1):
        rpm_name = (item.get("rpm", "") or "").strip()
        href = (item.get("href", "") or "").strip()
        href_norm = href.lstrip("/")

        if not quiet:
            show_name = rpm_name or href_norm or "<unknown>"
            print(f"[下载][rpm] {idx}/{total_rpm}: {show_name}", file=sys.stderr)

        if href_norm:
            _save_href(href_norm, "rpm")
        elif rpm_name:
            _save_href(posixpath.join("Packages", rpm_name), "rpm")
        else:
            failed += 1
            if not quiet:
                print("[跳过] Added 条目缺少 href/rpm", file=sys.stderr)

    if not quiet:
        print(
            f"Download repodata + Added RPMs from --new: downloaded={downloaded}, skipped={skipped}, failed={failed}",
            file=sys.stderr,
        )


def _fetch_to_tempfile(src: str, tmpdir: str) -> str:
    """
    读取 URL/本地文件 到临时目录，返回临时文件路径。
    """
    if _is_url(src):
        filename = os.path.basename(urllib.parse.urlparse(src).path) or "download.bin"
        dst = os.path.join(tmpdir, filename)
        _download_to_file(src, dst)
        return dst

    # 本地文件
    if not os.path.exists(src):
        raise FileNotFoundError(src)
    # 直接返回原路径也可以，但为了统一后续处理（尤其是相对路径），复制到临时目录更稳
    filename = os.path.basename(src) or "local.bin"
    dst = os.path.join(tmpdir, filename)
    with open(src, "rb") as rf, open(dst, "wb") as wf:
        while True:
            chunk = rf.read(8 * 1024 * 1024)
            if not chunk:
                break
            wf.write(chunk)
    return dst


def _open_maybe_compressed(path: str):
    """
    根据后缀打开（可压缩）的 XML/SQLite 等文件。
    """
    lower = path.lower()
    if lower.endswith(".gz"):
        return gzip.open(path, "rb")
    if lower.endswith(".xz") or lower.endswith(".lzma"):
        return lzma.open(path, "rb")
    if lower.endswith(".bz2"):
        return bz2.open(path, "rb")
    return open(path, "rb")


def _strip_ns(tag: str) -> str:
    # {namespace}tag -> tag
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _parse_repomd_for_primary_location(repomd_path: str) -> str:
    """
    从 repomd.xml 中找到 primary（xml 或 db 都可以，优先 xml）。
    返回其 location href（相对仓库根目录的路径），例如 repodata/xxx-primary.xml.gz
    """
    tree = ET.parse(repomd_path)
    root = tree.getroot()

    candidates: List[Tuple[str, str]] = []
    all_elements = []  # 用于调试
    
    # 方法1: 使用命名空间通配符查找 data 元素
    for data in root.findall(".//{*}data"):
        t = data.attrib.get("type", "")
        loc = data.find("{*}location")
        if loc is not None:
            href = loc.attrib.get("href")
            if href:
                candidates.append((t, href))
                all_elements.append(f"方法1: type={t}, href={href}")
    
    # 方法2: 遍历所有元素（忽略命名空间），查找 data 元素
    for elem in root.iter():
        tag = _strip_ns(elem.tag)
        if tag == "data":
            t = elem.attrib.get("type", "")
            for child in elem:
                child_tag = _strip_ns(child.tag)
                if child_tag == "location":
                    href = child.attrib.get("href")
                    if href:
                        # 避免重复添加
                        if (t, href) not in candidates:
                            candidates.append((t, href))
                            all_elements.append(f"方法2: type={t}, href={href}")
                    break
    
    # 方法3: 查找所有包含 "primary" 的 location href（作为兜底）
    for elem in root.iter():
        for child in elem:
            child_tag = _strip_ns(child.tag)
            if child_tag == "location":
                href = child.attrib.get("href", "")
                if "primary" in href.lower() and (None, href) not in [(t, h) for t, h in candidates]:
                    parent_tag = _strip_ns(elem.tag)
                    t = elem.attrib.get("type", "")
                    # 如果父元素是 data 但没有 type，尝试从 href 推断
                    if not t and parent_tag == "data":
                        if "primary.xml" in href.lower():
                            t = "primary"
                        elif "primary.sqlite" in href.lower() or "primary.db" in href.lower():
                            t = "primary_db"
                    if t:
                        candidates.append((t, href))
                        all_elements.append(f"方法3: type={t}, href={href}")

    # 优先 primary.xml，其次 primary_db
    for t, href in candidates:
        if t == "primary":
            return href
    for t, href in candidates:
        if t == "primary_db":
            return href
    
    # 如果 candidates 中有包含 "primary.xml" 的 href，即使 type 不匹配也尝试使用
    for t, href in candidates:
        if "primary.xml" in href.lower():
            return href
    for t, href in candidates:
        if "primary.sqlite" in href.lower() or "primary.db" in href.lower():
            return href

    # 如果还是找不到，打印详细的调试信息
    if candidates:
        available_types = ", ".join(sorted(set(t for t, _ in candidates if t)))
        available_hrefs = "\n  ".join(set(h for _, h in candidates[:10]))  # 显示前10个
        debug_info = "\n".join(all_elements[:10]) if all_elements else "无"
        raise RuntimeError(
            f"repomd.xml 中未找到 primary/primary_db 元数据。\n"
            f"找到的 data 类型: {available_types}\n"
            f"找到的 href 示例:\n  {available_hrefs}\n"
            f"调试信息:\n{debug_info}"
        )
    else:
        # 尝试打印根元素和所有子元素信息用于调试
        root_tag = _strip_ns(root.tag)
        child_tags = [f"{_strip_ns(c.tag)}" for c in root]
        raise RuntimeError(
            f"repomd.xml 中未找到任何 data 元素，请检查文件格式。\n"
            f"根元素: {root_tag}\n"
            f"直接子元素: {', '.join(child_tags[:10])}\n"
            f"文件路径: {repomd_path}\n"
            f"提示: 请确认这是一个有效的 repomd.xml 文件"
        )


@dataclass(frozen=True)
class Pkg:
    name: str
    epoch: str
    ver: str
    rel: str
    arch: str
    location_href: str  # e.g. Packages/xxx.rpm

    @property
    def nevra(self) -> str:
        e = self.epoch if self.epoch not in ("", None) else "0"
        return f"{self.name}-{e}:{self.ver}-{self.rel}.{self.arch}"

    @property
    def rpm_filename(self) -> str:
        return posixpath.basename(self.location_href)


def _iter_primary_packages(primary_xml_path: str, progress_label: Optional[str] = None) -> Iterator[Pkg]:
    """
    以流式方式解析 primary.xml，产出包信息。
    直接遍历子元素，忽略命名空间，这样更可靠。
    """
    package_count = 0
    skipped_count = 0
    last_error = None
    expected_total: Optional[int] = None
    last_progress_print = 0
    
    try:
        with _open_maybe_compressed(primary_xml_path) as f:
            # iterparse 需要 file-like
            # 使用 start 和 end 事件，在 end 事件时子元素应该还在
            context = ET.iterparse(f, events=("start", "end"))
            for event, elem in context:
                tag = _strip_ns(elem.tag)

                if event == "start" and tag == "metadata" and expected_total is None:
                    packages_attr = elem.attrib.get("packages")
                    if packages_attr and packages_attr.isdigit():
                        expected_total = int(packages_attr)
                
                # 只处理 package 元素的 end 事件
                if tag != "package" or event != "end":
                    # 对于非 package 元素，在 start 事件时不清除（让它们保持）
                    if tag != "package":
                        continue
                    continue

                package_count += 1
                if progress_label and expected_total and (
                    package_count - last_progress_print >= 200 or package_count == expected_total
                ):
                    bar = _render_progress_bar(package_count, expected_total)
                    percent = (package_count / expected_total) * 100
                    print(
                        f"\r[{progress_label}] 解析进度 {bar} {percent:6.2f}% ({package_count}/{expected_total})",
                        end="",
                        file=sys.stderr,
                    )
                    last_progress_print = package_count
                
                # 直接遍历子元素（忽略命名空间）- 这是最可靠的方法
                name_el = None
                arch_el = None
                ver_el = None
                loc_el = None
                
                # 获取所有直接子元素（在 clear 之前）
                children = list(elem)
                child_tags_debug = []  # 用于调试
                
                for child in children:
                    child_tag = _strip_ns(child.tag)
                    if package_count == 1:  # 只在第一个包时记录调试信息
                        child_tags_debug.append(f"{child.tag} -> {child_tag}")
                    
                    if child_tag == "name":
                        name_el = child
                    elif child_tag == "arch":
                        arch_el = child
                    elif child_tag == "version":
                        ver_el = child
                    elif child_tag == "location":
                        loc_el = child
                    
                    # 如果都找到了，可以提前退出
                    if name_el and arch_el and ver_el and loc_el:
                        break

                if name_el is None or arch_el is None or ver_el is None or loc_el is None:
                    skipped_count += 1
                    # 记录第一个错误用于调试
                    if last_error is None:
                        missing = []
                        if name_el is None:
                            missing.append("name")
                        if arch_el is None:
                            missing.append("arch")
                        if ver_el is None:
                            missing.append("version")
                        if loc_el is None:
                            missing.append("location")
                        # 添加调试信息：显示实际找到的子元素标签
                        debug_msg = f"缺少元素: {', '.join(missing)}"
                        if package_count == 1:
                            debug_msg += f"\n实际找到的子元素标签: {', '.join(child_tags_debug[:20])}"
                            debug_msg += f"\npackage 元素的直接子元素数量: {len(children)}"
                            debug_msg += f"\npackage 元素的 tag: {elem.tag}"
                        last_error = debug_msg
                    elem.clear()
                    continue

                # 提取文本和属性（在 clear 之前提取所有信息）
                name = (name_el.text or "").strip() if name_el.text is not None else ""
                arch = (arch_el.text or "").strip() if arch_el.text is not None else ""
                epoch = ver_el.attrib.get("epoch", "0").strip() if ver_el.attrib.get("epoch") else "0"
                ver = ver_el.attrib.get("ver", "").strip() if ver_el.attrib.get("ver") else ""
                rel = ver_el.attrib.get("rel", "").strip() if ver_el.attrib.get("rel") else ""
                href = loc_el.attrib.get("href", "").strip() if loc_el.attrib.get("href") else ""

                if name and arch and ver and rel and href:
                    yield Pkg(name=name, epoch=epoch, ver=ver, rel=rel, arch=arch, location_href=href)
                else:
                    skipped_count += 1
                    # 记录第一个错误用于调试（只在第一个包时记录详细信息）
                    if last_error is None and package_count == 1:
                        # 打印第一个包的详细信息用于调试
                        debug_info = []
                        debug_info.append(f"name_el: {name_el.tag if name_el else None}, text={repr(name_el.text if name_el else None)}")
                        debug_info.append(f"arch_el: {arch_el.tag if arch_el else None}, text={repr(arch_el.text if arch_el else None)}")
                        debug_info.append(f"ver_el: {ver_el.tag if ver_el else None}, attrib={ver_el.attrib if ver_el else None}")
                        debug_info.append(f"loc_el: {loc_el.tag if loc_el else None}, attrib={loc_el.attrib if loc_el else None}")
                        debug_info.append(f"提取的值: name={repr(name)}, arch={repr(arch)}, ver={repr(ver)}, rel={repr(rel)}, href={repr(href)}")
                        missing_fields = []
                        if not name:
                            missing_fields.append("name")
                        if not arch:
                            missing_fields.append("arch")
                        if not ver:
                            missing_fields.append("ver")
                        if not rel:
                            missing_fields.append("rel")
                        if not href:
                            missing_fields.append("href")
                        last_error = f"字段为空: {', '.join(missing_fields)}\n调试信息:\n" + "\n".join(debug_info)
                    elif last_error is None:
                        missing_fields = []
                        if not name:
                            missing_fields.append("name")
                        if not arch:
                            missing_fields.append("arch")
                        if not ver:
                            missing_fields.append("ver")
                        if not rel:
                            missing_fields.append("rel")
                        if not href:
                            missing_fields.append("href")
                        last_error = f"字段为空: {', '.join(missing_fields)}"

                # 释放内存（在提取完所有信息后）
                elem.clear()
    except Exception as e:
        raise RuntimeError(
            f"解析 primary.xml 时出错: {e}\n"
            f"文件路径: {primary_xml_path}\n"
            f"已处理的 package 元素: {package_count}\n"
            f"跳过的 package 元素: {skipped_count}"
        ) from e
    
    # 如果找到了 package 元素但没有产出任何包，给出警告
    if package_count > 0 and skipped_count == package_count:
        error_msg = f"primary.xml 中找到 {package_count} 个 package 元素，但无法解析出任何包信息。\n"
        error_msg += f"文件路径: {primary_xml_path}\n"
        if last_error:
            error_msg += f"错误信息: {last_error}\n"
        error_msg += "请检查 XML 格式是否正确"
        raise RuntimeError(error_msg)

    if progress_label:
        if expected_total:
            bar = _render_progress_bar(expected_total, expected_total)
            print(
                f"\r[{progress_label}] 解析进度 {bar} 100.00% ({package_count}/{expected_total})",
                file=sys.stderr,
            )
        else:
            print(f"[{progress_label}] 解析完成：{package_count} 个包", file=sys.stderr)


def _repo_base_from_repomd_src(repomd_src: str) -> str:
    """
    给定 repomd.xml 的 URL 或路径，推导仓库根（用于拼接 primary/location href）。
    """
    if _is_url(repomd_src):
        # .../repodata/repomd.xml -> .../
        u = urllib.parse.urlparse(repomd_src)
        path = u.path
        if path.endswith("repodata/repomd.xml"):
            base_path = path[: -len("repodata/repomd.xml")]
        else:
            # 兜底：去掉最后一段
            base_path = path.rsplit("/", 1)[0] + "/"
        return urllib.parse.urlunparse((u.scheme, u.netloc, base_path, "", "", ""))

    # 本地路径
    # .../repodata/repomd.xml -> repo root
    p = os.path.abspath(repomd_src)
    # 标准化路径分隔符，统一用 / 进行比较
    p_norm = p.replace(os.sep, "/")
    if p_norm.endswith("/repodata/repomd.xml") or p_norm.endswith("repodata/repomd.xml"):
        return os.path.dirname(os.path.dirname(p))
    # 如果只是 repomd.xml（在 repodata 目录下），返回上一级目录
    if os.path.basename(p) == "repomd.xml":
        return os.path.dirname(os.path.dirname(p))
    return os.path.dirname(p)


def _materialize_primary_xml(repomd_src: str, repomd_file: str, tmpdir: str) -> str:
    """
    从 repomd.xml 找到 primary 元数据并下载/复制到本地，返回本地文件路径。
    """
    primary_href = _parse_repomd_for_primary_location(repomd_file)
    base = _repo_base_from_repomd_src(repomd_src)

    if _is_url(repomd_src):
        primary_url = _join_url(base, primary_href)
        return _fetch_to_tempfile(primary_url, tmpdir)

    # 本地 repo：primary_href 通常是相对 repo root 的路径
    # 如果 primary_href 已经是绝对路径，直接使用；否则拼接
    if os.path.isabs(primary_href):
        primary_path = primary_href
    else:
        # 标准化路径分隔符
        primary_href_normalized = primary_href.replace("/", os.sep)
        primary_path = os.path.join(base, primary_href_normalized)
    
    # 检查文件是否存在
    if not os.path.exists(primary_path):
        raise FileNotFoundError(
            f"找不到 primary 元数据文件: {primary_path}\n"
            f"repomd.xml 位置: {repomd_src}\n"
            f"仓库根目录: {base}\n"
            f"primary_href: {primary_href}"
        )
    
    return _fetch_to_tempfile(primary_path, tmpdir)


def _load_pkgs(src: str, *, progress_label: Optional[str] = None) -> List[Pkg]:
    repomd_src = _resolve_repomd_source(src)
    with tempfile.TemporaryDirectory(prefix="repodata_diff_") as tmpdir:
        repomd_file = _fetch_to_tempfile(repomd_src, tmpdir)
        primary_file = _materialize_primary_xml(repomd_src, repomd_file, tmpdir)

        # primary_db（sqlite）这里不解析：绝大多数 repo 都有 primary.xml.*，且 XML 解析无需额外依赖
        return list(_iter_primary_packages(primary_file, progress_label=progress_label))


def _index_by_nevra(pkgs: Iterable[Pkg]) -> Dict[str, Pkg]:
    d: Dict[str, Pkg] = {}
    for p in pkgs:
        # 理论上 NEVRA 唯一；如果重复（极少），保留第一个
        d.setdefault(p.nevra, p)
    return d


def _index_latest_by_name_arch(pkgs: Iterable[Pkg]) -> Dict[Tuple[str, str], Pkg]:
    """
    用于计算"变更/升级"：按 (name, arch) 聚合，选取版本号字典序最大的作为"最新"。
    说明：这是一个近似策略（不做 RPM 版本比较规则），但对常见 ver/rel 形式够用。
    """
    best: Dict[Tuple[str, str], Pkg] = {}
    for p in pkgs:
        key = (p.name, p.arch)
        cur = best.get(key)
        if cur is None:
            best[key] = p
            continue
        # 简化比较：epoch、ver、rel 字典序
        if (p.epoch, p.ver, p.rel) > (cur.epoch, cur.ver, cur.rel):
            best[key] = p
    return best


def diff_repodata(
    old_src: str,
    new_src: str,
    *,
    old_source_label: Optional[str] = None,
    new_source_label: Optional[str] = None,
) -> dict:
    old_pkgs = _load_pkgs(old_src, progress_label="old")
    new_pkgs = _load_pkgs(new_src, progress_label="new")

    old_by_nevra = _index_by_nevra(old_pkgs)
    new_by_nevra = _index_by_nevra(new_pkgs)

    old_set = set(old_by_nevra.keys())
    new_set = set(new_by_nevra.keys())

    added_nevra = sorted(new_set - old_set)
    removed_nevra = sorted(old_set - new_set)

    # 额外给出"按 name+arch 的版本变更"（旧最新 != 新最新）
    old_latest = _index_latest_by_name_arch(old_pkgs)
    new_latest = _index_latest_by_name_arch(new_pkgs)

    changed: List[dict] = []
    for key in sorted(set(old_latest.keys()) | set(new_latest.keys())):
        o = old_latest.get(key)
        n = new_latest.get(key)
        if o is None or n is None:
            continue
        if o.nevra != n.nevra:
            changed.append(
                {
                    "name": key[0],
                    "arch": key[1],
                    "old": {"nevra": o.nevra, "href": o.location_href},
                    "new": {"nevra": n.nevra, "href": n.location_href},
                }
            )

    def _details(nevra_list: Sequence[str], idx: Dict[str, Pkg]) -> List[dict]:
        out: List[dict] = []
        for n in nevra_list:
            p = idx.get(n)
            if p is None:
                out.append({"nevra": n})
            else:
                out.append({"nevra": p.nevra, "href": p.location_href, "rpm": p.rpm_filename})
        return out

    old_src_display = (
        old_source_label if old_source_label is not None else _resolve_repomd_source(old_src)
    )
    new_src_display = (
        new_source_label if new_source_label is not None else _resolve_repomd_source(new_src)
    )
    return {
        "old": {"count": len(old_pkgs), "source": old_src_display},
        "new": {"count": len(new_pkgs), "source": new_src_display},
        "added": _details(added_nevra, new_by_nevra),
        "removed": _details(removed_nevra, old_by_nevra),
        "changed_latest": changed,
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="对比两次 repodata（repomd.xml/primary.xml），列出增量 RPM 包",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p.add_argument(
        "--old",
        required=True,
        help="旧仓库：可以是以下任一形式：\n"
        "  - 仓库根目录（如 old/，会自动查找 repodata/repomd.xml）\n"
        "  - repodata 目录（如 old/repodata/，会自动查找 repomd.xml）\n"
        "  - repomd.xml 文件路径（如 old/repodata/repomd.xml）\n"
        "  - 仓库 URL\n"
        "  - 本地归档文件（zip/tar/tar.gz/tar.xz/tar.bz2 等，自动仅解压 repodata 参与对比）",
    )
    p.add_argument(
        "--new",
        required=True,
        help="新仓库：可以是以下任一形式：\n"
        "  - 仓库根目录（如 new/，会自动查找 repodata/repomd.xml）\n"
        "  - repodata 目录（如 new/repodata/，会自动查找 repomd.xml）\n"
        "  - repomd.xml 文件路径（如 new/repodata/repomd.xml）\n"
        "  - 仓库 URL\n"
        "  - 本地归档文件（zip/tar/tar.gz/tar.xz/tar.bz2 等，自动仅解压 repodata 参与对比）",
    )
    p.add_argument("--show-removed", action="store_true", help="同时输出 removed（旧有新无）")
    p.add_argument("--show-changed", action="store_true", help="同时输出 changed_latest（按 name+arch 的最新版本变化）")
    p.add_argument("--json", action="store_true", help="以 JSON 输出（默认纯文本）")
    p.add_argument(
        "--download",
        action="store_true",
        help="从 --new 自动下载 Added 的 RPM 包 + repodata 元数据到目录中。",
    )
    p.add_argument(
        "--dir",
        default="repo",
        help="下载目标目录（配合 --download 使用；不存在会自动创建）。默认：repo",
    )
    p.add_argument(
        "--compress",
        default=None,
        choices=["zip", "gz", "xz", "bz2"],
        help="下载完成后压缩 --dir 目录并删除原目录。可选：zip/gz/xz/bz2。",
    )
    args = p.parse_args(argv)

    old_raw = args.old.strip()
    new_raw = args.new.strip()
    old_effective = old_raw
    new_effective = new_raw
    old_label: Optional[str] = None
    new_label: Optional[str] = None
    extract_tmps: List[str] = []

    try:
        if not _is_url(old_raw):
            old_abs = os.path.abspath(old_raw)
            if os.path.isfile(old_abs) and _is_supported_archive(old_abs):
                tmp_old = tempfile.mkdtemp(prefix="repodiff_old_archive_")
                extract_tmps.append(tmp_old)
                old_effective = _extract_repodata_from_archive(old_abs, tmp_old)
                old_label = f"{old_abs} (archive, repodata only)"

        if not _is_url(new_raw):
            new_abs = os.path.abspath(new_raw)
            if os.path.isfile(new_abs) and _is_supported_archive(new_abs):
                tmp_new = tempfile.mkdtemp(prefix="repodiff_new_archive_")
                extract_tmps.append(tmp_new)
                new_effective = _extract_repodata_from_archive(new_abs, tmp_new)
                new_label = f"{new_abs} (archive, repodata only)"

        result = diff_repodata(
            old_effective,
            new_effective,
            old_source_label=old_label,
            new_source_label=new_label,
        )
        added = result["added"]
        if args.download:
            _download_added_and_repodata_from_new_repo(
                new_effective,
                args.dir,
                added,
                quiet=bool(args.json),
            )
            if args.compress:
                try:
                    print(
                        f"[压缩] 开始压缩目录: {os.path.abspath(args.dir)} (format={args.compress})",
                        file=sys.stderr,
                    )
                    pack_out = _compress_directory_next_to_parent(args.dir, args.compress)
                    shutil.rmtree(os.path.abspath(args.dir))
                except (OSError, ValueError) as e:
                    print(f"[压缩失败] {e}", file=sys.stderr)
                else:
                    print(f"已压缩: {pack_out}", file=sys.stderr)
            # 下载模式下不再输出 old/new 对比信息
            return 0
        elif args.compress:
            raise SystemExit("错误: --compress 需要配合 --download 一起使用")

        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0

        removed = result["removed"]
        changed = result["changed_latest"]

        print(f"old: {result['old']['count']} pkgs  ({result['old']['source']})")
        print(f"new: {result['new']['count']} pkgs  ({result['new']['source']})")
        print("")

        print(f"== Added: {len(added)} ==")
        for item in added:
            # 优先使用 rpm 字段（RPM 文件名），如果没有则从 href 提取
            rpm_name = item.get("rpm", "")
            if not rpm_name and item.get("href"):
                rpm_name = posixpath.basename(item["href"])
            print(rpm_name)

        if args.show_removed:
            print("")
            print(f"== Removed: {len(removed)} ==")
            for item in removed:
                # 优先使用 rpm 字段（RPM 文件名），如果没有则从 href 提取
                rpm_name = item.get("rpm", "")
                if not rpm_name and item.get("href"):
                    rpm_name = posixpath.basename(item["href"])
                print(rpm_name)

        if args.show_changed:
            print("")
            print(f"== Changed (latest by name+arch): {len(changed)} ==")
            for c in changed:
                print(f"{c['name']}.{c['arch']}")
                old_rpm = posixpath.basename(c['old'].get('href', ''))
                new_rpm = posixpath.basename(c['new'].get('href', ''))
                print(f"  - old: {old_rpm}")
                print(f"  - new: {new_rpm}")

        return 0
    finally:
        for tmp in extract_tmps:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())


