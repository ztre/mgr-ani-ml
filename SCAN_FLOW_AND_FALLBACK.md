# 扫描流程与 Movie Fallback 判定说明

本文梳理当前扫描逻辑的主流程、关键分支、Movie fallback 的触发条件，以及针对 SAO 类样例的判定矩阵，便于排查日志与回归测试。

## 1. 主流程（`run_scan` -> `_process_sync_group` -> `_process_file`）

1. `run_scan`
- 创建 `ScanTask`，设置 `running`。
- 校验 TMDB Key，缺失则任务失败。
- 遍历启用的同步组，逐组调用 `_process_sync_group`。
- 结束后触发 Emby 刷新并写任务结束状态。

2. `_process_sync_group`
- 读取同步组 `source/target/source_type/include/exclude`。
- 调用 `resolve_movie_target_root` 预先计算 TV->Movie 的目标路径。
- 扫描文件后分两轮处理。
- 第 1 轮：视频文件（`.mkv/.mp4/.avi/.mov/.webm`）。
- 第 2 轮：附属文件（`.ass/.srt/.ssa/.vtt/.mka`）。
- 扫描时若文件位于“待办目录”中，则直接跳过，不自动重试识别。

3. `_process_file`
- 非媒体后缀直接跳过。
- `xx.5` 半集编号直接跳过（不整理、不进待办）。
- inode 去重：若目标已有同 inode 文件，直接跳过。
- 优先尝试目录缓存；缓存不可用时走解析与 TMDB 匹配。
- 计算目标路径，做同轮目标冲突检查。
- 创建硬链接，写 media/inode 记录。
- 根据类型执行刮削。
- `movie/extras` 下跳过刮削。

## 2. 关键分支

1. 解析失败
- 条件：`parse_result is None`。
- 行为：整目录转 `pending_manual`。

2. TMDB 未匹配
- 条件：TV/Movie 搜索都没命中（或不允许 fallback）。
- 行为：整目录转 `pending_manual`。

3. 同轮目标冲突
- 条件：多个不同源文件映射到同一个目标文件。
- 行为：整目录转 `pending_manual`，避免覆盖。

4. `SPs/Specials` 目录
- 强制按 extras 语义处理。
- TV：按 TV extras 规则落到季目录下的 extras 分类目录。
- Movie：落到电影目录下 `extras/`。

5. 附属文件
- 会跟随主规则建硬链接。
- 不走主视频“硬链接创建成功 + 刮削”路径。

## 3. Movie fallback 逻辑（TV 同步组）

### 3.1 入口一：`_parse_with_fallback`

优先级如下：

1. 识别到 `episode`
- 直接按 TV。
- 即使原先 `is_ambiguous=True`，也会改为 `False` 后按 TV。

2. 识别到 `extra_type`
- 直接按 TV。
- 防止 `Menu/OP/ED/PV/Offline` 被误判成 Movie。

3. TV 解析稳定（非 ambiguous）
- 按 TV。

4. TV 解析不稳定或失败
- 仅当命中 Movie fallback 关键词时，才尝试 Movie parse。
- 未命中关键词，不走 Movie。

### 3.2 入口二：TV 搜 TMDB 失败后的二次 fallback

需同时满足：

1. 当前仍是 TV 路径。
2. `parse_result.extra_type` 为空。
3. 不在 `SPs/Specials` 目录。
4. 命中 Movie fallback 关键词。
5. Movie TMDB 搜索命中。

否则不转 Movie，转待办。

### 3.3 关键词来源（配置驱动）

1. 优先读取 `AMM_MOVIE_FALLBACK_HINTS`。
- 支持逗号、分号、换行分隔。

2. 未配置时使用通用默认词：
- `the movie`
- `movie`
- `film`
- `剧场版`
- `电影版`

说明：测试样例词不再硬编码在逻辑里，应通过配置扩展。

### 3.4 fallback 到 Movie 后目标路径如何选

`resolve_movie_target_root` 规则：

1. 当前组是 `movie`
- 直接用当前组 `target`。

2. 全局启用的 movie 组只有 1 个目标
- 使用唯一目标。

3. 有多个 movie 目标
- 按 `movie_fallback_strategy` 选择：
- `prefer_name_match`
- `prefer_source_prefix`
- `auto`（先 name，再 source_prefix）
- `unique_only`

4. 仍无法唯一确定
- 转待办。

## 4. 缓存与污染防护

1. 目录缓存用于减少重复 TMDB 搜索。
2. TV 组中某文件触发 Movie fallback 后，不写入目录级缓存。
3. 这样同目录后续文件不会被“整体 movie 化”。

## 5. 待办回滚的边界

1. 转待办时会回滚该目录本轮已写入结果（media/inode/目标文件）。
2. 目录清理仅允许在目标库下的问题子目录执行。
3. 同步组 `source` 根目录、`target` 根目录、待办目录本身都受保护，不会被删。

## 6. SAO 样例判定矩阵（当前逻辑）

| 输入样例 | 关键特征 | 预期路径类型 | 说明 |
|---|---|---|---|
| `[VCB-Studio] Sword Art Online II [14][Ma10p_1080p]...mkv` | 有集号 `14` | TV 正片 | 强制按 TV，不走 movie fallback |
| `[VCB-Studio] Sword Art Online II [14.5]...mkv` | 半集 `.5` | 跳过 | 不整理、不进待办 |
| `[VCB-Studio] Sword Art Online II [Menu01]...mkv` | `extra_type=Menu01` | TV extras | 按 TV extras 分类目录 |
| `.../SPs/... [PV04]...mkv` | `SPs` 目录 + extra | TV 或 Movie 的 extras | 不按正片处理 |
| `[VCB-Studio] Sword Art Online -Ordinal Scale- ...mkv` | 无集号，通常需要关键词命中 | 取决于 fallback 关键词配置 | 命中则可转 Movie，否则待办 |
| `普通 TV 文件无集号无 extra` | 缺少稳定 TV 特征 | 通常待办 | 除非命中 movie fallback 关键词且 movie 能匹配 |

## 7. 推荐回归用例

1. `SxxExx`、`[14]`、`第14集` 类型文件应稳定进入 TV 正片目录。
2. `OP/ED/IV/PV/CM/Menu/Offline` 应进入 TV extras 或 Movie extras，不应挤入正片。
3. `14.5` 半集应始终被跳过且无待办记录。
4. 在 TV 目录放入剧场版，仅当关键词配置命中时允许转 Movie。
5. 单目录内多文件同名映射冲突时应触发目录级待办，不应覆盖写。
