"""AI 识别外挂服务。

通过 OpenAI 兼容 API 或 Google Gemini API 分析本地视频文件与 TMDB 剧集的映射关系，
辅助修正季号 / 集号误判问题。

功能开关：settings.ai_enabled — 默认 False，不影响原有识别流程。
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Literal, Optional

from pydantic import BaseModel, Field, ValidationError

from ..api.logs import append_log
from ..config import settings

# ---------------------------------------------------------------------------
# 数据模型（与 Bangumi_Auto_Rename 项目的 AI 模块对齐，精简版）
# ---------------------------------------------------------------------------

class EpisodeMapping(BaseModel):
    """单个文件的剧集映射。"""
    file_path: str = Field(..., description="本地文件相对路径（相对于媒体目录）")
    tmdb_season: int = Field(..., ge=0, description="TMDB 季号（0=特典）")
    tmdb_episode: int = Field(..., ge=1, description="TMDB 集号")
    confidence: Literal["High", "Medium", "Low"] = Field(default="Medium")

    class Config:
        populate_by_name = True


class AIAnalysisResult(BaseModel):
    """AI 分析整体结果。"""
    confidence: Literal["High", "Medium", "Low"] = Field(..., description="整体置信度")
    reason: str = Field(..., description="分析说明")
    file_mapping: list[EpisodeMapping] = Field(default_factory=list, description="文件映射列表")
    extra_notes: Optional[str] = Field(default=None, description="额外说明")

    @classmethod
    def _json_schema_for_api(cls) -> dict:
        """生成去除 additionalProperties 的 JSON Schema（Gemini 兼容）。"""
        schema = cls.model_json_schema()

        def _strip(obj: object) -> None:
            if isinstance(obj, dict):
                obj.pop("additionalProperties", None)
                for v in obj.values():
                    _strip(v)
            elif isinstance(obj, list):
                for v in obj:
                    _strip(v)

        _strip(schema)
        return schema

    class Config:
        populate_by_name = True


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------

_THINKING_RE = re.compile(r"<thinking>.*?</thinking>", re.DOTALL)
_JSON_PATTERNS = [
    re.compile(r"```json\s*(\{.*?\})\s*```", re.DOTALL),
    re.compile(r"```\s*(\{.*?\})\s*```", re.DOTALL),
    re.compile(r"(\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\})", re.DOTALL),
]

_CONFIDENCE_RANK: dict[str, int] = {"High": 2, "Medium": 1, "Low": 0}


def _extract_json(content: str) -> Optional[dict]:
    """从文本中提取第一个合法的 JSON 对象。兼容思维链输出。"""
    content = _THINKING_RE.sub("", content).strip()
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        pass
    for regex in _JSON_PATTERNS:
        for match in regex.findall(content):
            try:
                return json.loads(match)
            except json.JSONDecodeError:
                continue
    return None


# ---------------------------------------------------------------------------
# Prompt 构建（与 Bangumi_Auto_Rename 对齐的通用提示词）
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = (
    "你是一个专业的动漫文件重命名助手。你需要分析本地动漫文件与TMDB数据库中剧集信息的对应关系，"
    "特别关注动漫BD发布与官方分季的差异。"
    "请只输出匹配到的季度和剧集信息，不要输出其他未匹配到TMDB信息的内容。"
)


def _build_prompt(series_info: dict, local_files: list[dict]) -> str:
    name = series_info.get("name") or series_info.get("title") or "未知"
    first_air = series_info.get("first_air_date", "未知")
    num_seasons = series_info.get("number_of_seasons", 0)
    num_eps = series_info.get("number_of_episodes", 0)

    tmdb_block = (
        f"动漫名称: {name}\n"
        f"首播日期: {first_air}\n"
        f"总季数: {num_seasons}\n"
        f"总集数: {num_eps}\n"
    )
    seasons_block = "TMDB 季度信息：\n" + json.dumps(
        series_info.get("seasons", []), ensure_ascii=False
    )
    files_block = "本地文件信息（路径均为相对路径）:\n"
    for f in local_files:
        dur = f" (时长: {f['duration']:.1f}分钟)" if f.get("duration") else ""
        files_block += f"  {f['path']}{dur}\n"

    return f"""请分析以下动漫的本地文件与TMDB数据的对应关系：

{tmdb_block}

{seasons_block}

{files_block}

请特别注意以下常见情况：
0. TMDB的第0季通常是特典或OVA集
1. 本地目录可能将多季合并为一个目录，或者相反
2. 本地目录剧集的标号可能会是总集号，而不是TMDB的季集号
3. 本地目录可能会给总集篇标注4.5这样的半集号，而TMDB会将其放在第0季
4. OVA/特典可能被放在正片季度末尾，而TMDB会将其放在第0季
5. 本地目录的不同季度可能仅用名称区分，没有明确季号
6. 剧场版有时被混在TV版中，一般会被TMDB视为特典处理

只输出匹配到的文件映射信息，不要输出其他未匹配到TMDB信息的内容。"""


def _schema_instructions() -> str:
    schema = AIAnalysisResult._json_schema_for_api()
    schema.pop("title", None)
    schema.pop("description", None)
    return (
        "请严格按照以下 JSON Schema 格式返回分析结果。不要添加任何额外的解释或注释，只返回 JSON 对象。\n\n"
        f"JSON Schema:\n```json\n{json.dumps(schema, indent=2, ensure_ascii=False)}\n```"
    )


# ---------------------------------------------------------------------------
# OpenAI 客户端（懒加载，不安装 openai 包时仅记录警告）
# ---------------------------------------------------------------------------

class _OpenAIClient:
    def __init__(self) -> None:
        self._client: object = None
        self._init_error: bool = False

    def _ensure(self) -> bool:
        if self._client is not None:
            return True
        if self._init_error:
            return False
        try:
            from openai import OpenAI  # type: ignore[import-untyped]
            api_key = settings.ai_api_key or ""
            base_url = settings.ai_base_url or None
            if not api_key:
                append_log("WARNING: [AI识别] ai_api_key 未配置，OpenAI 客户端不可用")
                self._init_error = True
                return False
            self._client = OpenAI(api_key=api_key, base_url=base_url)
            return True
        except ImportError:
            append_log(
                "WARNING: [AI识别] openai 包未安装，请执行: pip install openai>=1.0.0"
            )
            self._init_error = True
            return False
        except Exception as e:
            append_log(f"WARNING: [AI识别] OpenAI 客户端初始化失败: {e}")
            self._init_error = True
            return False

    def call(self, series_info: dict, local_files: list[dict]) -> Optional[AIAnalysisResult]:
        if not self._ensure():
            return None
        prompt = _build_prompt(series_info, local_files)
        model = settings.ai_model or "gpt-4o-mini"
        try:
            from openai import OpenAI  # type: ignore[import-untyped]
            client: OpenAI = self._client  # type: ignore[assignment]
            resp = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": _SYSTEM_PROMPT + "\n" + _schema_instructions()},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.1,
            )
            content = resp.choices[0].message.content
            if not content:
                append_log("WARNING: [AI识别] OpenAI 返回空响应")
                return None
            raw = _extract_json(content)
            if raw is None:
                append_log(f"WARNING: [AI识别] OpenAI 无法提取 JSON: {content[:300]}")
                return None
            return AIAnalysisResult.model_validate(raw)
        except ValidationError as e:
            append_log(f"WARNING: [AI识别] OpenAI 结果验证失败: {e}")
            return None
        except Exception as e:
            append_log(f"WARNING: [AI识别] OpenAI 调用失败: {e}")
            return None


# ---------------------------------------------------------------------------
# Gemini 客户端（懒加载，不安装 google-genai 包时仅记录警告）
# ---------------------------------------------------------------------------

class _GeminiClient:
    def __init__(self) -> None:
        self._client: object = None
        self._init_error: bool = False

    def _ensure(self) -> bool:
        if self._client is not None:
            return True
        if self._init_error:
            return False
        try:
            from google import genai  # type: ignore[import-untyped]
            from google.genai.types import HttpOptions  # type: ignore[import-untyped]

            api_key = settings.ai_gemini_api_key or ""
            if not api_key:
                append_log("WARNING: [AI识别] ai_gemini_api_key 未配置，Gemini 客户端不可用")
                self._init_error = True
                return False

            base_url = settings.ai_gemini_base_url or "https://generativelanguage.googleapis.com"
            http_options: Optional[HttpOptions] = None
            if base_url != "https://generativelanguage.googleapis.com":
                http_options = HttpOptions()
                http_options.base_url = base_url

            if http_options is not None:
                self._client = genai.Client(api_key=api_key, http_options=http_options)
            else:
                self._client = genai.Client(api_key=api_key)
            return True
        except ImportError:
            append_log(
                "WARNING: [AI识别] google-genai 包未安装，请执行: pip install google-genai"
            )
            self._init_error = True
            return False
        except Exception as e:
            append_log(f"WARNING: [AI识别] Gemini 客户端初始化失败: {e}")
            self._init_error = True
            return False

    def call(self, series_info: dict, local_files: list[dict]) -> Optional[AIAnalysisResult]:
        if not self._ensure():
            return None
        prompt = _build_prompt(series_info, local_files)
        schema = AIAnalysisResult._json_schema_for_api()
        model = settings.ai_gemini_model or "gemini-2.5-flash"
        try:
            from google.genai.types import GenerateContentConfig  # type: ignore[import-untyped]

            cfg = GenerateContentConfig(
                system_instruction=_SYSTEM_PROMPT,
                response_mime_type="application/json",
                response_schema=schema,
                temperature=0.5,
            )
            client = self._client  # type: ignore[attr-defined]
            resp = client.models.generate_content(
                model=model,
                contents=prompt,
                config=cfg,
            )
            if not resp or not resp.text:
                append_log("WARNING: [AI识别] Gemini 返回空响应")
                return None
            if hasattr(resp, "parsed") and resp.parsed:
                return AIAnalysisResult.model_validate(resp.parsed)
            raw = _extract_json(resp.text)
            if raw is None:
                append_log(f"WARNING: [AI识别] Gemini 无法提取 JSON: {resp.text[:300]}")
                return None
            return AIAnalysisResult.model_validate(raw)
        except ValidationError as e:
            append_log(f"WARNING: [AI识别] Gemini 结果验证失败: {e}")
            return None
        except Exception as e:
            append_log(f"WARNING: [AI识别] Gemini 调用失败: {e}")
            return None


# ---------------------------------------------------------------------------
# 单例客户端缓存（避免重复创建连接）
# ---------------------------------------------------------------------------

_openai_client: Optional[_OpenAIClient] = None
_gemini_client: Optional[_GeminiClient] = None


def _get_client() -> Optional[_OpenAIClient | _GeminiClient]:
    global _openai_client, _gemini_client
    provider = (settings.ai_provider or "openai").lower().strip()
    if provider == "gemini":
        if _gemini_client is None:
            _gemini_client = _GeminiClient()
        return _gemini_client
    else:
        if _openai_client is None:
            _openai_client = _OpenAIClient()
        return _openai_client


# ---------------------------------------------------------------------------
# 对外公开 API
# ---------------------------------------------------------------------------

def analyze_episode_mapping(
    series_info: dict,
    media_dir: Path,
    video_files: list[Path],
) -> Optional[dict[str, tuple[int, int]]]:
    """分析本地视频文件与 TMDB 剧集的映射关系。

    Args:
        series_info: TMDB 剧集详情（需含 name, seasons, number_of_seasons 等字段）
        media_dir: 媒体目录（用于计算相对路径和构建结果 key）
        video_files: 待映射的视频文件列表（绝对路径）

    Returns:
        ``{str(video_file_absolute_path): (tmdb_season, tmdb_episode)}``
        或 ``None``（AI 未启用 / 调用失败 / 置信度不足）
    """
    if not settings.ai_enabled:
        return None
    if not video_files:
        return None

    threshold_str = (settings.ai_confidence_threshold or "Medium").strip()
    threshold_rank = _CONFIDENCE_RANK.get(threshold_str, 1)

    local_files = [
        {"path": str(p.relative_to(media_dir)), "duration": None}
        for p in video_files
    ]
    provider = (settings.ai_provider or "openai").lower().strip()
    client = _get_client()
    if client is None:
        return None

    append_log(
        f"INFO: [AI识别] 调用 {provider.upper()} 分析 {len(video_files)} 个文件 "
        f"| 目录={media_dir.name}"
    )

    result = client.call(series_info, local_files)
    if result is None:
        return None

    result_rank = _CONFIDENCE_RANK.get(result.confidence, 0)
    append_log(
        f"INFO: [AI识别] {provider.upper()} 返回 confidence={result.confidence}, "
        f"映射数={len(result.file_mapping)}, 理由={result.reason}"
    )

    if result_rank < threshold_rank:
        append_log(
            f"WARNING: [AI识别] 置信度 {result.confidence} 低于阈值 {threshold_str}，"
            f"忽略本次 AI 映射结果"
        )
        return None

    if not result.file_mapping:
        append_log("WARNING: [AI识别] AI 返回空映射列表，跳过")
        return None

    mapping: dict[str, tuple[int, int]] = {}
    for em in result.file_mapping:
        try:
            full_path = str(media_dir / em.file_path)
            mapping[full_path] = (em.tmdb_season, em.tmdb_episode)
        except Exception:
            pass  # 相对路径异常时跳过单条

    if not mapping:
        append_log("WARNING: [AI识别] 映射结果构建失败（路径解析异常），跳过")
        return None

    append_log(f"INFO: [AI识别] 构建映射成功，共 {len(mapping)} 条")
    return mapping
