"""原件字节存储：只写不改、内容寻址、两阶段（暂存 -> 提升）原子落盘。

两阶段流程（修复单号冲突残留的关键）
------------------------------------
* ``stage`` 把请求字节写入共享卷的临时暂存区（每请求唯一文件名），
  暂存文件不属于内容寻址原件库，冲突拒绝/幂等复用/限额拒绝时直接丢弃，
  原件库不会出现仅由被拒请求产生的文件；
* ``promote`` 在数据库发布事务内、提交前调用，把暂存文件 ``os.replace``
  原子提升到内容寻址路径 ``ab/cd/<digest>``。同摘要文件已存在则直接复用；
* 任一阶段写盘失败都抛 :class:`BlobError`，调用方回滚事务、清理暂存文件，
  返回 507 且无库记录，客户端可原样重试。

崩溃安全：暂存区残留文件不被任何记录引用，重启自检只核对数据库引用的摘要，
孤立暂存/原件均无害；只要事务未提交，卷上就不会产生新的已引用原件。
"""
from __future__ import annotations

import hashlib
import os
import uuid
from dataclasses import dataclass
from pathlib import Path

from .config import Settings


class BlobError(Exception):
    """写盘失败（空间、权限、IO 等）。"""


@dataclass
class StagedBlob:
    """已暂存但尚未提升进内容寻址库的请求原件。

    生命周期只有两种正常结局：``promote`` 提升为正式原件（事务内），
    或 ``discard`` 丢弃（拒绝/复用/失败）。用作上下文管理器时，
    退出作用域仍未提升的暂存文件会被自动清理，杜绝异常路径残留。
    """

    store: "BlobStore"
    digest: str
    staged_path: Path
    size: int

    def promote(self) -> str:
        """原子提升到内容寻址路径，返回摘要。同摘要已存在则复用，绝不覆盖。"""
        self.store.promote(self)
        return self.digest

    def discard(self) -> None:
        """丢弃暂存文件；提升完成后再调用为空操作。"""
        try:
            self.staged_path.unlink(missing_ok=True)
        except OSError:
            # 清理失败不改变请求语义（暂存文件本就不被任何记录引用）。
            pass

    def __enter__(self) -> "StagedBlob":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.discard()


class BlobStore:
    def __init__(self, settings: Settings):
        self._settings = settings
        settings.ensure_dirs()

    @staticmethod
    def _target_path(blob_dir: Path, digest: str) -> Path:
        return blob_dir / digest[:2] / digest[2:4] / digest

    def path_for(self, digest: str) -> Path:
        return self._target_path(self._settings.blob_dir, digest)

    def exists(self, digest: str) -> bool:
        return self._target_path(self._settings.blob_dir, digest).is_file()

    # ------------------------------------------------------------------ #
    # 阶段 1：暂存（不触碰内容寻址原件库）
    # ------------------------------------------------------------------ #
    def stage(self, data: bytes) -> StagedBlob:
        """把请求字节原子写入暂存区，返回 :class:`StagedBlob`。

        文件名带随机后缀，双实例并发暂存同摘要也互不干扰；
        写盘失败抛 :class:`BlobError`，且不留下半截暂存文件。
        """
        digest = hashlib.sha256(data).hexdigest()
        tmp_path = (
            self._settings.temp_dir
            / f".staged-{digest}-{os.getpid()}-{uuid.uuid4().hex}"
        )
        try:
            with open(tmp_path, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
        except OSError as exc:
            tmp_path.unlink(missing_ok=True)
            raise BlobError(f"原件暂存失败: {exc}") from exc
        return StagedBlob(self, digest, tmp_path, len(data))

    # ------------------------------------------------------------------ #
    # 阶段 2：提升（仅在数据库发布事务内调用）
    # ------------------------------------------------------------------ #
    def promote(self, staged: StagedBlob) -> None:
        """把暂存文件原子改名为内容寻址原件，并 fsync 所在目录。

        * 目标已存在（同摘要此前已提交，或并发实例刚刚提升同字节）：直接复用，
          丢弃本次暂存文件，绝不覆盖既有内容；
        * 改名/建目录失败抛 :class:`BlobError`，调用方回滚数据库事务。
        """
        target = self._target_path(self._settings.blob_dir, staged.digest)
        if target.is_file():
            staged.discard()
            return
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            os.replace(staged.staged_path, target)
        except OSError as exc:
            raise BlobError(f"原件提升失败: {exc}") from exc
        self._fsync_dir(target.parent)
        # 提升成功：暂存路径已不存在，discard 成为空操作，保留它供 with 退出兜底。

    def read(self, digest: str) -> bytes:
        return self._target_path(self._settings.blob_dir, digest).read_bytes()

    @staticmethod
    def _fsync_dir(path: Path) -> None:
        try:
            fd = os.open(path, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def verify_present(self, digest: str) -> bool:
        """启动自检用：摘要与文件实际字节一致且存在。"""
        path = self._target_path(self._settings.blob_dir, digest)
        if not path.is_file():
            return False
        return hashlib.sha256(path.read_bytes()).hexdigest() == digest
