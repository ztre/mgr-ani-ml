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

## 升级注意事项

- **数据库结构调整**：部分版本更新后，`media_records` 表新增了 `season`、`category`、`file_type` 等字段；升级后建议在 Web UI 触发一次**全量扫描**，以补全已有记录的新增字段，否则部分筛选与统计功能可能显示不完整。

## 使用限制

- **同步组源目录下的单文件暂不支持整理**：扫描器以目录为最小处理单元，源目录的直接子文件（即放在同步组源目录根层的媒体文件）不会被识别与整理。请将媒体文件放在源目录下的子目录中，例如 `源目录/番剧名/ep01.mkv`，而非 `源目录/ep01.mkv`。

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

## 更新记录

### 2025-04（任务队列 + 手动整理优化）

**后端**

- `task_queue.py`（新增）：基于线程队列的任务执行器，支持串行排队、可中断任务与取消语义
- `database.py`：拆分任务专用数据库连接（`TaskSessionLocal`），配置 SQLite WAL 模式与 busy_timeout，隔离任务写入与请求读取，消除锁竞争；增加 `_migrate_scan_tasks_to_task_db` 迁移逻辑
- `api/media.py`：重构手动整理任务执行路径，统一走 `_enqueue_logged_media_task` 入队；新增 `_manual_organize_log_cleanup_rows` 负责任务完成后清理冗余日志行；新增 `_pending_has_visible_mainline_files_fast` 快速判断目录是否有可见正片文件
- `api/tasks.py`：任务列表 / 日志 / 取消 / 删除接口全部切换到 `get_task_db` 依赖；新增 `_is_running_interruptible_task` 校验可中断状态
- `api/scan.py`：扫描任务入队逻辑迁移至 task_queue，支持按同步组扫描
- `services/scanner.py`：新增 `_cleanup_nonreview_logs_for_directory` 在整理完成后清理非 review 日志；修复 `_task_session` 确保任务内操作使用任务专用 DB session
- `api/logs.py`：日志追加路径兼容任务专用 session

**前端**

- `Pending.vue`（手动识别整理弹窗）：
  - 修复 scoped `:deep()` 对 teleport 节点无效问题，改用全局 `<style>` 块约束弹窗布局
  - 弹窗垂直居中显示；压缩表单行间距，为文件列表腾出更多空间
  - `el-table` 启用 `height="100%"` 固定表头 + 内部滚动，支持鼠标滚轮
  - 修复 `selectedPendingFileItems` 计算逻辑，确保"已选"视图与实际加载数据联动
  - 修复切换全部 / 已选视图时表格 key 刷新，避免 Element Plus 内部状态错位
- `Dashboard.vue`：任务状态标签新增 `cancelling` / `cancelled` 状态显示
- `App.vue`：样式与主题变量拆分至 `frontend/src/styles/element/`
- `main.js`：引入 Element Plus 主题覆盖样式
- `vite.config.js`：配置 SCSS 预处理变量路径
