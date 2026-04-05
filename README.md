# Anime Media Manager

番剧 / 剧场版自动识别与硬链接整理系统，后端基于 FastAPI，前端基于 Vue 3 + Element Plus。

## 核心能力

- **自动识别整理**：TV / Movie 自动分流，按目标规则生成硬链接结果
- **目录级识别增强**：季感知识别、低置信度 AniList 英文名兜底、冲突重排与安全降级
- **待办与人工修正**：统一处理 pending / unprocessed / review，支持手动整理与日志追踪
- **媒体记录与日志监控**：任务日志抽屉、媒体记录修正、批量扫描与实时中断
- **Inode 管理**：按 inode 聚合查看、筛选、清理、批量删除缓存记录
- **配置中心**：TMDB、Emby、日志保留、统计口径、整理策略、鉴权密码集中管理

## 运行要求

- Python 3.11+
- Node.js 20+
- npm 10+

## 快速开始

### 本地开发

```bash
# 1) 后端依赖
python -m venv .venv
source .venv/bin/activate
pip install -r backend/requirements.txt

# 2) 启动后端
python backend/run.py

# 3) 启动前端（另一个终端）
cd frontend
npm install
npm run dev
```

- 前端开发地址：`http://localhost:5173`
- 后端接口地址：`http://localhost:8000`

### Docker 部署

```bash
# 1) 准备 compose 用环境文件
cp .env.example .env

# 2) 构建并启动
docker compose up -d --build
```

访问地址：`http://localhost:8000`

## 首次启动说明

- 应用会在数据目录自动生成运行时配置文件：`${AMM_ROOT_DIR}/data/.env`
- 若开启鉴权，系统会在首次生成配置时写入随机密码与随机密钥
- 启动后请进入“配置”页面填写 TMDB API Key、Emby 连接信息等必要项
- Web UI 中保存的配置会写回 `${AMM_ROOT_DIR}/data/.env`

## 顶层 `.env` 与运行时配置的区别

仓库根目录的 `.env` 仅供 Docker Compose 使用，主要作用是：

- 映射运行用户 `PUID` / `PGID`
- 指定宿主机持久化目录 `AMM_ROOT_DIR`
- 调整 pending / unprocessed / review 日志挂载位置

识别策略、TMDB/Emby、鉴权密码等业务配置，不从仓库根 `.env` 直接读取；这些内容统一保存在运行时配置文件 `${AMM_ROOT_DIR}/data/.env` 中，并由 Web UI 管理。

## 数据与目录

- 数据库：`${AMM_ROOT_DIR}/data/anime_media.db`
- 运行时配置：`${AMM_ROOT_DIR}/data/.env`
- 任务日志：`${AMM_ROOT_DIR}/logs`
- 待办日志：`${AMM_ROOT_DIR}/pending`
- 媒体根目录：容器内固定挂载为 `/media`

默认挂载关系见 [docker-compose.yml](docker-compose.yml)。

## 常用验证命令

```bash
# 前端生产构建
cd frontend
npm run build

# 后端全量回归
cd ..
source .venv/bin/activate
python -m pytest
```

当前仓库最近一次本地校验结果：

- 前端 `npm run build` 通过
- 后端全量测试 `324 passed`

## 项目结构

```text
backend/     FastAPI 服务、识别逻辑、配置与任务 API
frontend/    Vue 3 前端页面与 API 客户端
tests/       后端回归与数据集回放测试
docker/      容器入口脚本
scripts/     辅助脚本
logs/        示例或本地调试日志
tdocs/       调试分析文档与临时工具
```
