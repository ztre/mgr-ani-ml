# Anime Media Manager

番剧/剧场版自动识别与硬链接整理系统（FastAPI + Vue3 + Element Plus）

## 核心功能

- **自动识别整理** — TV / Movie 自动分流，硬链接输出
- **媒体记录** — 按资源聚合展示，支持详细信息抽屉查看
- **Inode 管理** — 筛选、清理、批量删除功能
- **配置中心** — TMDB/Emby 与整理策略一体化配置
- **人工修正** — pending / unprocessed / review 日志前端查看与处理
- **任务控制** — 扫描任务实时中断管理

## 快速开始

### 本地开发

```bash
# 后端
pip install -r backend/requirements.txt
python backend/run.py

# 前端（另一个终端）
cd frontend
npm install
npm run dev
```

前端访问：`http://localhost:5173` | 后端：`http://localhost:8000`

### Docker 部署

```bash
# 复制环境变量文件
cp .env.example .env

# 启动服务
docker compose up -d --build
```

访问：`http://localhost:8000`

> 首次启动后进入 **配置** 页面填写 TMDB API Key 等必要信息

## 配置说明

**环境变量** (`.env`)

```
PUID=1000                                    # 用户 ID
PGID=100                                     # 组 ID
AMM_ROOT_DIR=/app/amm-state                  # 数据存储路径
AMM_PENDING_JSONL_PATH=/app/pending/pending.jsonl
AMM_UNPROCESSED_ITEMS_JSONL_PATH=/app/pending/unprocessed_items.jsonl
AMM_REVIEW_JSONL_PATH=/app/pending/review.jsonl
```

**数据库** — SQLite 默认位置：`${AMM_ROOT_DIR}/data/anime_media.db`，首次启动自动创建

**配置文件** — 位置：`${AMM_ROOT_DIR}/data/.env`，前端可直接修改
