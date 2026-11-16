"""行政区版本资料：加载、校验与按版本解析。

同一区域代码在不同版本中可以覆盖不同的稳定地点集合。解析影响范围时必须
使用预警自带的 ``boundary_version``，不得用“当前版本”替代，也不得根据正文
文字推断区域。
"""

import hashlib
import json
from pathlib import Path


class BoundaryError(ValueError):
    """边界资料本身不合法。"""


def _non_empty_text(value, field):
    if not isinstance(value, str) or not value.strip():
        raise BoundaryError(field + " must be a non-empty string")
    return value.strip()


def normalize_document(raw):
    """校验一份边界版本资料并返回规范化副本。

    ``covers`` 是不可再分的稳定地点标识。区域代码只是该版本内的行政代码，
    可以跨版本复用，但含义可以变化。
    """
    if not isinstance(raw, dict):
        raise BoundaryError("boundary document must be an object")
    version = _non_empty_text(raw.get("version"), "version")
    regions = raw.get("regions")
    if not isinstance(regions, list) or not regions:
        raise BoundaryError("regions must be a non-empty array")
    normalized = []
    seen = set()
    for index, region in enumerate(regions):
        if not isinstance(region, dict):
            raise BoundaryError(f"regions[{index}] must be an object")
        code = _non_empty_text(region.get("code"), f"regions[{index}].code")
        if code in seen:
            raise BoundaryError("duplicate region code: " + code)
        seen.add(code)
        covers = region.get("covers")
        if not isinstance(covers, list) or not covers:
            raise BoundaryError(f"regions[{index}].covers must be a non-empty array")
        places = []
        for place in covers:
            places.append(_non_empty_text(place, f"regions[{index}].covers[]"))
        if len(set(places)) != len(places):
            raise BoundaryError(f"regions[{index}].covers contains duplicates")
        name = region.get("name", code)
        name = _non_empty_text(name, f"regions[{index}].name")
        normalized.append({"code": code, "name": name, "covers": places})
    normalized.sort(key=lambda item: item["code"])
    return {"version": version, "regions": normalized}


def document_digest(document):
    encoded = json.dumps(document, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def discover_files(paths):
    """展开文件与目录，目录中只读取 ``*.json``。找不到任何文件不是错误。"""
    files = []
    for path in paths:
        candidate = Path(path)
        if candidate.is_dir():
            files.extend(sorted(candidate.glob("*.json")))
        elif candidate.is_file():
            files.append(candidate)
        else:
            raise BoundaryError("boundary path not found: " + str(candidate))
    unique = []
    seen = set()
    for path in files:
        resolved = str(path.resolve())
        if resolved not in seen:
            seen.add(resolved)
            unique.append(path)
    return unique


def load_documents(paths):
    """读取并校验边界资料。

    返回 ``(documents, errors)``。错误按文件隔离，一个坏文件不会阻止其他
    版本被加载；错误会进入报告，让值班员看到缺什么。
    """
    documents = []
    errors = []
    for path in discover_files(paths):
        try:
            raw = json.loads(Path(path).read_text(encoding="utf-8"))
            document = normalize_document(raw)
        except (OSError, json.JSONDecodeError, BoundaryError) as error:
            errors.append({"source": str(path), "reason": "invalid_boundary_document", "detail": str(error)})
            continue
        documents.append({"document": document, "digest": document_digest(document), "source": str(path)})
    return documents, errors


def catalog_from_rows(rows):
    """把存储中的版本/区域行转换为 ``{version: {code: region}}``。"""
    catalog = {}
    for row in rows:
        version = row["version"]
        catalog.setdefault(version, {})[row["code"]] = {
            "name": row["name"],
            "covers": list(row["covers"]),
        }
    return catalog


def resolve_regions(catalog, version, codes):
    """按指定版本把区域代码解析为稳定地点集合。

    返回 ``(places, missing)``。任一资料缺失时 ``places`` 为 ``None``，调用方
    必须把事件视为范围未确定，而不是用已解析的部分区域或正文猜测。
    """
    missing = []
    if not isinstance(version, str) or not version.strip():
        return None, [{"reason": "missing_boundary_version", "boundary_version": None}]
    regions = catalog.get(version)
    if regions is None:
        return None, [{"reason": "missing_boundary_version", "boundary_version": version}]
    places = set()
    for code in codes:
        region = regions.get(code)
        if region is None:
            missing.append({
                "reason": "missing_region",
                "boundary_version": version,
                "region": code,
            })
            continue
        places.update(region["covers"])
    if missing:
        return None, missing
    return frozenset(places), []
