# Anime Media Manager

番剧/剧场版自动识别与硬链接整理系统（FastAPI + Vue3 + Element Plus）。

## 主要特性

- 识别与整理：TV / Movie 自动分流，硬链接输出
- 媒体记录：按资源聚合展示，右侧抽屉查看明细
- Inode 管理：支持筛选、清理、批量删除
- 配置中心：TMDB/Emby 与整理策略统一由网页填写与保存
- 人工修正日志：`pending / unprocessed / review` 三类 JSONL 可在前端查看与登记处理结果

### TV 集号识别与冲突规避（2026-03 更新）

- TV 集号优先识别显式模式：`SxxEyy` / `xxyy` / `EPxx` / `Exx` / `Episode xx` / `第xx集`
- 弱数字规则已收紧：编码参数中的数字（如 `yuv420p10`、`10bit`、`x264/x265`、`flacx2`）不再当作集号
- 当 TV 文件无法识别到可信集号，且也不属于已识别的 Special/OPED 时，会自动按 `making` 分类落入 `extras`
- `extras` 命名采用可读标签；同目录同类同标签会自动追加序号（如 `#02`）避免“多个源文件映射到同一目标”

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
# AMM_PENDING_JSONL_PATH=/app/pending/pending.jsonl
# AMM_UNPROCESSED_ITEMS_JSONL_PATH=/app/pending/unprocessed_items.jsonl
# AMM_REVIEW_JSONL_PATH=/app/pending/review.jsonl
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
- 如需自定义人工修正日志位置，可直接在前端 **配置** 页面修改 `pending / unprocessed / review` 三个路径

## SQLite 初始化逻辑

后端启动会执行：

1. 解析 `database_url`
2. 若为 SQLite 且本地 DB 文件不存在：自动创建目录并创建空文件
3. 自动建表（`Base.metadata.create_all`）

默认 Docker 路径为：`/app/data/anime_media.db`（映射到宿主机 `${AMM_ROOT_DIR}/data`）

配置文件默认路径为：`/app/data/.env`（首次启动自动生成，位于 `${AMM_ROOT_DIR}/data`）

人工修正日志默认路径为：

- `/app/pending/pending.jsonl`
- `/app/pending/unprocessed_items.jsonl`
- `/app/pending/review.jsonl`

对应宿主机目录为：`${AMM_ROOT_DIR}/pending`

## 前端说明

- `媒体记录` 和 `Inode 管理` 都按资源级聚合展示，TV 多季会合并到同一行
- `媒体记录` 抽屉中，TV 资源提供 `Season` 选择器；抽屉不再显示类型选择器
- `媒体记录 / Inode 管理` 的封面与 TMDB 标题使用浏览器本地缓存，减少重复请求
- 左侧工具栏新增 `人工修正日志` 页面，可查看 `pending / unprocessed / review` 并追加人工处理登记

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
