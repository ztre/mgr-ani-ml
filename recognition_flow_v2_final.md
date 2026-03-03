# Anime Media Manager

## Recognition & Organization Core Architecture v2 (Final Extreme Stable Edition)

------------------------------------------------------------------------

# 1. Design Goals

Build a media recognition engine that is:

-   Directory-driven
-   Unified Movie/TV competitive identification
-   Multi-stage fallback capable
-   Context-aware (TV → Movie fallback supported)
-   Fully transactional with rollback safety
-   Idempotent (safe to rescan)
-   API-efficient
-   Long-term stable

------------------------------------------------------------------------

# 2. Global Pipeline Overview

    Input: root_path
            ↓
    [1] Recursive Directory Scan
            ↓
    Collect directories that directly contain video files
            ↓
    Filter already processed / unchanged directories
            ↓
    For each directory:
        ↓
        [2] Structure Parsing (local only)
        ↓
        [3] Candidate Title Generation
        ↓
        [4] Unified Movie + TV Competitive Search
        ↓
        Score Ranking
        ↓
        Score insufficient?
            ├─ Yes → Fallback Retry
            └─ No  → Type Decision
                    ↓
            [5] Context-aware Target Resolution
                    ↓
            [6] Season Decision (TV only)
                    ↓
            [7] Target Path Construction
                    ↓
            [8] Transactional Execution (Link + Metadata)
                    ↓
            Success?
                ├─ Yes → SUCCESS
                └─ No  → Rollback

------------------------------------------------------------------------

# 3. Stage 1 --- Directory-Driven Scan

Rules:

-   Recursively traverse all subdirectories.
-   If a directory directly contains at least one video file, include
    it.
-   Do NOT check child directories for eligibility.
-   Do NOT elevate parent directories.
-   No API calls in this stage.

Supported video formats:

-   mp4
-   mkv
-   avi
-   mov
-   flv

Optimization:

-   Skip directories marked SUCCESS and unchanged.
-   Directory signature = (file_count, latest_mtime)

------------------------------------------------------------------------

# 4. Stage 2 --- Structure Parsing (Pure Local)

Extract:

-   raw_name
-   cleaned_name
-   main_title
-   subtitle
-   season_hint
-   episode_hint
-   year_hint
-   special_hint

No API calls here.

------------------------------------------------------------------------

# 5. Stage 3 --- Candidate Title Generation

Priority order:

1.  Full cleaned_name
2.  main_title + subtitle
3.  main_title
4.  main_title without subtitle
5.  main_title without year
6.  main_title without Special/Extra/Final

Used for fallback retries.

------------------------------------------------------------------------

# 6. Stage 4 --- Unified Competitive Search (Core)

For each candidate:

Call both:

-   /search/movie
-   /search/tv

Merge all results into a unified candidate pool.

Candidate fields:

-   media_type
-   tmdb_id
-   title
-   score
-   popularity
-   vote_count

------------------------------------------------------------------------

# 7. Final Scoring Formula

score = title_similarity \* 0.6 + year_bonus \* 0.1 + popularity_weight
\* 0.15 + vote_weight \* 0.15

Confidence levels:

-   ≥ 0.85 High
-   0.7--0.85 Medium
-   0.6--0.7 Low
-   \< 0.6 Fail

------------------------------------------------------------------------

# 8. Fallback Strategy

If best score \< 0.7:

Retry in order:

1.  Remove subtitle
2.  Remove year
3.  Remove Special/Extra/Final
4.  Use only main_title

Maximum two fallback rounds.

If still \< 0.6 → FAILED

------------------------------------------------------------------------

# 9. Type Decision & Context-aware Targeting

Definitions:

scan_context_type = source group type (tv or movie) recognized_type =
search result type

Decision matrix:

  scan_context   recognized   target_root
  -------------- ------------ --------------
  tv             tv           target_tv
  tv             movie        target_movie
  movie          movie        target_movie
  movie          tv           target_tv

Important:

-   scan_context never changes.
-   Only affects destination path.
-   TV directory recognizing movie is valid fallback.

------------------------------------------------------------------------

# 10. Season Decision (TV only)

If season_hint exists: If TMDB season exists → use it Else → default to
1 Else: Default to 1

Special → Season 0

------------------------------------------------------------------------

# 11. Renaming Standards

TV:

Show Name (Year) \[tmdbid=xxx\]/ Season 01/ Show Name - S01E01.mkv

Movie:

Movie Name (Year) \[tmdbid=xxx\]/ Movie Name (Year) - 1080p.mkv

------------------------------------------------------------------------

# 12. Transactional Execution & Rollback Mechanism

All operations must be atomic.

Execution order:

1.  Create temporary target directory
2.  Create hard links
3.  Generate NFO / poster
4.  Validate all outputs
5.  Mark SUCCESS

OperationLog tracks:

-   created_dirs
-   created_links
-   created_files

If any error occurs:

Rollback procedure:

1.  Delete created_files
2.  Delete created_links
3.  Delete empty directories (reverse order)
4.  Update database status to FAILED

Guarantees:

-   No half-finished directory
-   No orphan links
-   No media pollution

------------------------------------------------------------------------

# 13. State Machine

States:

-   SCANNED
-   PARSED
-   IDENTIFIED
-   LOW_CONFIDENCE
-   LINKING
-   SCRAPING
-   SUCCESS
-   FAILED

Rules:

-   Only SUCCESS marks directory processed.
-   FAILED allows future retry.
-   LOW_CONFIDENCE allows manual confirmation.

------------------------------------------------------------------------

# 14. API Protection Strategy

-   Max 1 full identification per directory
-   Max 2 fallback rounds
-   LRU cache enabled
-   Rate limit: 3 req/sec
-   Do not cache failures permanently

------------------------------------------------------------------------

# 15. System Stability Pillars

System stability depends on four pillars:

1.  Directory-driven scanning
2.  Unified competitive scoring
3.  Controlled fallback mechanism
4.  Full transactional rollback

If these four remain intact, long-term stability is guaranteed.

------------------------------------------------------------------------

# 16. Final Outcome

This architecture ensures:

-   High recognition accuracy
-   Automatic TV/Movie correction
-   Minimal API overhead
-   Safe rescanning
-   No data corruption
-   Fully recoverable failure handling
-   Production-level robustness

This document defines the final stable core of Anime Media Manager
recognition engine.
