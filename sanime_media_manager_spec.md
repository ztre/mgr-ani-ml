# Anime Media Manager 项目说明书（分阶段代码生成指令版本）

## 一、项目目标

构建一个可部署在 Linux NAS 上的 Docker 化前后端应用，用于：

-   番剧与动画电影资源自动识别
-   硬链接整理（inode 去重）
-   自动分类（TV / Movie）
-   自动重命名（符合 Emby 规范）
-   元数据刮削（Bangumi + TMDB）
-   自动刷新 Emby 媒体库
-   支持多组同步关系
-   提供 Web UI 管理界面

------------------------------------------------------------------------

# 二、总体技术架构

## 1. 前端

-   Vue3
-   Vite
-   Pinia
-   Element Plus
-   Axios

## 2. 后端

-   Python 3.11+
-   FastAPI
-   SQLAlchemy
-   SQLite
-   httpx
-   apscheduler

## 3. 外部系统

-   Bangumi API
-   TMDB API
-   Emby Server（Docker 部署，通过 HTTP API 访问）

## 4. 部署方式

-   Dockerfile
-   docker-compose.yml
-   仅映射 /media 目录
-   端口 8000（后端）
-   前端构建后由 FastAPI 静态托管

------------------------------------------------------------------------

# 三、目录逻辑设计

## 1. 逻辑目录

/media /source_tv /source_movie /target_tv /target_movie /temp

支持多组 sync_groups，每组包含：

-   source
-   source_type (tv \| movie)
-   target
-   include
-   exclude
-   enable

------------------------------------------------------------------------

# 四、处理流程（完整逻辑）

1.  扫描 source
2.  include / exclude 过滤
3.  文件识别（调用 API 判断 TV 或 Movie）
4.  自动识别集数
5.  创建硬链接（使用 inode 判断避免重复）
6.  重命名为 Emby 标准格式
7.  刮削元数据
8.  刷新 Emby 媒体库

------------------------------------------------------------------------

# 五、命名规范实现要求

## 番剧

番剧名 (年份) \[tmdbid=编号\] Season 01 番剧名 - S01E01.mkv

特典统一放入：

Season 0

## 电影

电影名 (年份) \[tmdbid=编号\] extras 电影名 (年份) - 1080p.mkv

------------------------------------------------------------------------

# 六、数据库设计

## sync_groups

-   id
-   name
-   source
-   source_type
-   target
-   include
-   exclude
-   enabled

## media_records

-   id
-   original_path
-   target_path
-   type
-   tmdb_id
-   bangumi_id
-   status
-   created_at

------------------------------------------------------------------------

# 七、阶段化代码生成指令

------------------------------------------------------------------------

## 阶段1：项目骨架生成

生成：

backend/ main.py config.py models.py database.py

frontend/ vite + vue3 基础项目

Dockerfile docker-compose.yml

------------------------------------------------------------------------

## 阶段2：硬链接模块实现

实现：

-   inode 检查
-   include/exclude 规则
-   多组同步逻辑
-   API 识别后再决定目标目录

------------------------------------------------------------------------

## 阶段3：Bangumi + TMDB 识别模块

实现：

-   标题解析
-   API 搜索
-   精确匹配
-   获取 tmdbid

------------------------------------------------------------------------

## 阶段4：自动重命名模块

实现：

-   TV SxxExx 自动识别
-   电影自动命名
-   生成 Season 结构
-   特典归类 Season 0

------------------------------------------------------------------------

## 阶段5：元数据刮削模块

实现：

-   下载 poster
-   生成 NFO
-   生成 fanart

------------------------------------------------------------------------

## 阶段6：Emby API 刷新

实现：

POST /emby/refresh

调用 Emby Library Refresh API

------------------------------------------------------------------------

## 阶段7：Web UI 完整管理系统

实现：

-   同步组管理
-   扫描任务触发
-   日志展示
-   API Key 配置

------------------------------------------------------------------------

# 八、完整项目目录结构草案

anime-media-manager/

    backend/
        main.py
        api/
        services/
            linker.py
            parser.py
            metadata.py
            emby.py
        models.py
        database.py
        config.py

    frontend/
        src/
            views/
            components/
            api/
            store/

    docker-compose.yml
    Dockerfile

------------------------------------------------------------------------

# 九、必须实现功能清单

-   多组同步
-   include/exclude
-   inode 去重
-   API 预判类型
-   自动识别集数
-   特典归 Season 0
-   自动生成 tmdbid 命名
-   元数据刮削
-   Emby 自动刷新
-   Web UI 管理

------------------------------------------------------------------------

# 十、最终目标

构建一个：

-   可视化管理
-   自动整理
-   完全符合 Emby 规范
-   可扩展
-   可 Docker 部署

的 Anime Media 自动整理系统。
