# Anime Media Manager

番剧/剧场版自动识别与硬链接整理系统（FastAPI + Vue3 + Element Plus）。

## 主要特性

- 识别与整理：TV / Movie 自动分流，硬链接输出
- 媒体记录：按资源聚合展示，右侧抽屉查看明细
- Inode 管理：支持筛选、清理、批量删除
- 配置中心：TMDB/Emby 与整理策略统一由网页填写与保存

## 本地开发

```bash
# 后端
pip install -r backend/requirements.txt
python backend/run.py

# 前端（另一个终端）
cd frontend
npm install
npm run dev
```

- 前端默认 `http://localhost:5173`
- 后端默认 `http://localhost:8000`

## Docker 部署

### 1) 准备环境变量（仅用户映射）

```bash
cp .env.example .env
# 默认：
# PUID=1000, PGID=100
# AMM_ROOT_DIR=/app/amm-state
```

### 2) 构建并启动

```bash
docker compose up -d --build
```

### 3) 打开页面

```text
http://localhost:8000
```

首次启动后请进入 **配置** 页面填写：

- TMDB API Key（必填）
- Emby 地址 / API Key（如使用 Emby 刷新）
- 其他策略项（按需）

## SQLite 初始化逻辑

后端启动会执行：

1. 解析 `database_url`
2. 若为 SQLite 且本地 DB 文件不存在：自动创建目录并创建空文件
3. 自动建表（`Base.metadata.create_all`）

默认 Docker 路径为：`/app/data/anime_media.db`（映射到宿主机 `${AMM_ROOT_DIR}/data`）

配置文件默认路径为：`/app/data/.env`（首次启动自动生成，位于 `${AMM_ROOT_DIR}/data`）

## Docker 镜像打包

```bash
docker build -t yourname/anime-media-manager:latest .
docker push yourname/anime-media-manager:latest
```

## 目录结构

```text
backend/
  api/
  services/
frontend/
Dockerfile
docker-compose.yml
```
