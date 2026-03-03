"""Application settings and .env persistence.

Notes:
- AMM_* runtime OS environment variables are intentionally ignored.
- Config is loaded from persisted .env file and can be updated from Web UI.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


def _default_db_url() -> str:
  data_dir = Path('/app/data')
  if data_dir.exists():
    return f"sqlite:///{(data_dir / 'anime_media.db').as_posix()}"
  local_db = Path(__file__).resolve().parent.parent / 'anime_media.db'
  return f"sqlite:///{local_db.as_posix()}"


def _parse_bool(v: str | None, default: bool) -> bool:
  if v is None:
    return default
  return v.strip().lower() in {'1', 'true', 'yes', 'on'}


def _parse_int(v: str | None, default: int) -> int:
  if v is None or not str(v).strip():
    return default
  try:
    return int(str(v).strip())
  except ValueError:
    return default


def _read_env_file(path: Path) -> dict[str, str]:
  if not path.exists():
    return {}
  data: dict[str, str] = {}
  for raw in path.read_text(encoding='utf-8').splitlines():
    line = raw.strip()
    if not line or line.startswith('#') or '=' not in line:
      continue
    k, v = line.split('=', 1)
    data[k.strip()] = v.strip()
  return data


@dataclass
class Settings:
  media_root: str = '/media'
  source_tv: str = '/media/source_tv'
  source_movie: str = '/media/source_movie'
  target_tv: str = '/media/target_tv'
  target_movie: str = '/media/target_movie'
  temp_dir: str = '/media/temp'

  database_url: str = _default_db_url()

  # compatibility: frontend still exposes bangumi key field
  bangumi_api_key: str = ''
  tmdb_api_key: str = ''
  emby_url: str = ''
  emby_api_key: str = ''
  emby_library_ids: str = ''

  movie_fallback_strategy: str = 'auto'
  movie_fallback_hints: str = ''

  stats_ignore_specials: bool = True
  stats_ignore_extras: bool = True
  stats_ignore_trailers_featurettes: bool = True

  auth_enabled: bool = True
  auth_username: str = 'admin'
  auth_password: str = 'admin123'
  auth_secret: str = ''
  auth_token_expire_hours: int = 24

  log_retention_days: int = 14
  log_max_task_files: int = 200
  log_cleanup_interval_seconds: int = 600

  debug: bool = False

  env_file: Path = Path(__file__).resolve().parent.parent / '.env'
  env_prefix: str = 'AMM_'

  def __post_init__(self) -> None:
    self.load_from_env_file()

  def load_from_env_file(self) -> None:
    # Deliberately load from .env only. Do not consume runtime OS env AMM_*.
    data = _read_env_file(self.env_file)
    p = self.env_prefix

    self.bangumi_api_key = data.get(f'{p}BANGUMI_API_KEY', self.bangumi_api_key)
    self.tmdb_api_key = data.get(f'{p}TMDB_API_KEY', self.tmdb_api_key)
    self.emby_url = data.get(f'{p}EMBY_URL', self.emby_url)
    self.emby_api_key = data.get(f'{p}EMBY_API_KEY', self.emby_api_key)
    self.emby_library_ids = data.get(f'{p}EMBY_LIBRARY_IDS', self.emby_library_ids)

    self.movie_fallback_strategy = data.get(f'{p}MOVIE_FALLBACK_STRATEGY', self.movie_fallback_strategy)
    self.movie_fallback_hints = data.get(f'{p}MOVIE_FALLBACK_HINTS', self.movie_fallback_hints)

    self.stats_ignore_specials = _parse_bool(data.get(f'{p}STATS_IGNORE_SPECIALS'), self.stats_ignore_specials)
    self.stats_ignore_extras = _parse_bool(data.get(f'{p}STATS_IGNORE_EXTRAS'), self.stats_ignore_extras)
    self.stats_ignore_trailers_featurettes = _parse_bool(
      data.get(f'{p}STATS_IGNORE_TRAILERS_FEATURETTES'),
      self.stats_ignore_trailers_featurettes,
    )

    self.auth_enabled = _parse_bool(data.get(f'{p}AUTH_ENABLED'), self.auth_enabled)
    self.auth_username = data.get(f'{p}AUTH_USERNAME', self.auth_username)
    self.auth_password = data.get(f'{p}AUTH_PASSWORD', self.auth_password)
    self.auth_secret = data.get(f'{p}AUTH_SECRET', self.auth_secret)
    self.auth_token_expire_hours = _parse_int(data.get(f'{p}AUTH_TOKEN_EXPIRE_HOURS'), self.auth_token_expire_hours)

    self.log_retention_days = _parse_int(data.get(f'{p}LOG_RETENTION_DAYS'), self.log_retention_days)
    self.log_max_task_files = _parse_int(data.get(f'{p}LOG_MAX_TASK_FILES'), self.log_max_task_files)
    self.log_cleanup_interval_seconds = _parse_int(
      data.get(f'{p}LOG_CLEANUP_INTERVAL_SECONDS'),
      self.log_cleanup_interval_seconds,
    )

  def save_to_env(self) -> None:
    env_path = self.env_file
    existing: list[str] = []
    if env_path.exists():
      existing = env_path.read_text(encoding='utf-8').splitlines(keepends=True)

    prefix = self.env_prefix
    updates = {
      f'{prefix}BANGUMI_API_KEY': self.bangumi_api_key,
      f'{prefix}TMDB_API_KEY': self.tmdb_api_key,
      f'{prefix}EMBY_URL': self.emby_url,
      f'{prefix}EMBY_API_KEY': self.emby_api_key,
      f'{prefix}EMBY_LIBRARY_IDS': self.emby_library_ids,
      f'{prefix}MOVIE_FALLBACK_STRATEGY': self.movie_fallback_strategy,
      f'{prefix}MOVIE_FALLBACK_HINTS': self.movie_fallback_hints,
      f'{prefix}STATS_IGNORE_SPECIALS': str(bool(self.stats_ignore_specials)).lower(),
      f'{prefix}STATS_IGNORE_EXTRAS': str(bool(self.stats_ignore_extras)).lower(),
      f'{prefix}STATS_IGNORE_TRAILERS_FEATURETTES': str(bool(self.stats_ignore_trailers_featurettes)).lower(),
      f'{prefix}AUTH_ENABLED': str(bool(self.auth_enabled)).lower(),
      f'{prefix}AUTH_USERNAME': self.auth_username,
      f'{prefix}AUTH_PASSWORD': self.auth_password,
      f'{prefix}AUTH_SECRET': self.auth_secret,
      f'{prefix}AUTH_TOKEN_EXPIRE_HOURS': str(int(self.auth_token_expire_hours)),
      f'{prefix}LOG_RETENTION_DAYS': str(int(self.log_retention_days)),
      f'{prefix}LOG_MAX_TASK_FILES': str(int(self.log_max_task_files)),
      f'{prefix}LOG_CLEANUP_INTERVAL_SECONDS': str(int(self.log_cleanup_interval_seconds)),
    }

    out: list[str] = []
    seen = set()
    for line in existing:
      stripped = line.strip()
      if not stripped or stripped.startswith('#') or '=' not in line:
        out.append(line)
        continue
      k = line.split('=', 1)[0].strip()
      if k in updates:
        out.append(f'{k}={updates[k]}\n')
        seen.add(k)
      else:
        out.append(line)

    for k, v in updates.items():
      if k not in seen:
        out.append(f'{k}={v}\n')

    env_path.write_text(''.join(out), encoding='utf-8')


settings = Settings()
