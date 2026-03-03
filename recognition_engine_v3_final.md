# Anime Media Manager

## Recognition Engine Architecture v3 (Extreme Stable + Full Anime Regex Edition)

------------------------------------------------------------------------

# 1. Architecture Goals (v3 Upgrade)

Recognition Engine v3 introduces:

-   Full layered Anime Regex system
-   Type hint extraction (TV / Movie priority)
-   Unified competitive TMDB scoring
-   Multi-round fallback strategy
-   Context-aware target routing
-   Season intelligent resolution
-   Fully transactional execution
-   Strong rollback safety
-   Idempotent rescan capability
-   API protection and caching layer

v3 is the finalized industrial-grade recognition core.

------------------------------------------------------------------------

# 2. End-to-End Pipeline Overview

Input: root_path

1.  Directory-driven recursive scan
2.  Filter unchanged / processed directories
3.  For each video directory:
    -   Structure Parsing (Regex Layered)
    -   Candidate Title Generation
    -   Unified Movie + TV Competitive Search
    -   Confidence Evaluation
    -   Fallback Retry (if needed)
    -   Context-aware Type Decision
    -   Season Resolution (TV only)
    -   Target Path Construction
    -   Transactional Execution (Hardlink + Metadata)
    -   Commit or Rollback

------------------------------------------------------------------------

# 3. Layered Anime Regex System (Core Upgrade)

## 3.1 Preprocess Layer (Mandatory)

Remove: - Fansub tags \[\] - Technical info () - Resolution / encoding
fields - Normalize separators (. \_ → space)

Output: Clean structured string.

------------------------------------------------------------------------

## 3.2 Strong Structural Patterns (High Priority)

### Pattern A --- SxxEyy

Title S02E05 Title S2E5

Extract: - title - season - episode Type hint: TV (strong)

------------------------------------------------------------------------

### Pattern B --- Season Only

Title S02 Title 2nd Season Title 第2季

Extract: - title - season Type hint: TV (strong)

------------------------------------------------------------------------

### Pattern C --- Roman Numerals

Title II Title III Title IV Title V

Roman → Season mapping: II=2, III=3, IV=4, V=5

Type hint: TV

------------------------------------------------------------------------

### Pattern D --- Final Season

Title Final Season Title The Final Season Part 2

Extract: - title - part (optional)

Season determined later via TMDB max season.

Type hint: TV

------------------------------------------------------------------------

## 3.3 Medium Priority Patterns

### Pattern E --- Part Structure

Title Part 2

Extract part only. Does NOT define season. No forced type.

------------------------------------------------------------------------

### Pattern F --- Movie Subtitle Pattern

Title -Subtitle- Title - Ordinal Scale -

Extract: - title - subtitle

If: - No season - No episode - Hyphen subtitle exists

Type hint: Movie (priority)

------------------------------------------------------------------------

### Pattern G --- Episode Only

Title - 01 Title 01

Extract: - episode Weak TV hint.

------------------------------------------------------------------------

## 3.4 Fallback Case

If no pattern matched:

title = cleaned_name season = None episode = None type_hint = None

------------------------------------------------------------------------

# 4. Type Hint Priority

Priority order:

1.  SxxEyy → TV
2.  Season keywords → TV
3.  Roman numerals → TV
4.  Final Season → TV
5.  Movie keywords / Subtitle pattern → Movie priority
6.  Default → Unified competition decides

Type hint only influences search priority, not final decision.

------------------------------------------------------------------------

# 5. Unified Competitive Search Engine

For each candidate title:

Call both: - /search/movie - /search/tv

Merge into unified candidate pool.

Candidate structure:

-   media_type
-   tmdb_id
-   title
-   popularity
-   vote_count
-   similarity_score
-   final_score

------------------------------------------------------------------------

# 6. Final Scoring Formula

final_score = similarity_score \* 0.6 + year_bonus \* 0.1 +
popularity_weight \* 0.15 + vote_weight \* 0.15

Confidence levels:

≥ 0.85 High 0.7--0.85 Medium 0.6--0.7 Low \< 0.6 Fail

------------------------------------------------------------------------

# 7. Multi-Round Fallback Strategy

If best_score \< 0.7:

Retry sequence:

1.  Remove subtitle
2.  Remove year
3.  Remove Special/Extra/Final
4.  Use main_title only

Max two fallback rounds.

If still \< 0.6 → FAILED

------------------------------------------------------------------------

# 8. Context-Aware Target Routing

scan_context_type = source group type recognized_type = final selected
type

Decision Matrix:

tv + tv → target_tv tv + movie → target_movie movie + movie →
target_movie movie + tv → target_tv

Scan context never changes.

------------------------------------------------------------------------

# 9. Season Resolution (TV Only)

If season_hint exists: If TMDB contains season → use it Else → default 1
Else: default 1

Special → Season 0

Final Season → use max season from TMDB

------------------------------------------------------------------------

# 10. Target Structure Rules

TV:

Show Name (Year) \[tmdbid=xxx\]/ Season 01/ Show Name - S01E01.mkv

Movie:

Movie Name (Year) \[tmdbid=xxx\]/ Movie Name (Year) - 1080p.mkv

------------------------------------------------------------------------

# 11. Transactional Execution Model

All operations must be atomic.

Execution steps:

1.  Create temp directory
2.  Create hard links
3.  Generate NFO / artwork
4.  Validate outputs
5.  Mark SUCCESS

OperationLog tracks: - created_dirs - created_links - created_files

------------------------------------------------------------------------

# 12. Rollback Strategy

If any step fails:

1.  Delete created_files
2.  Delete created_links
3.  Delete empty directories (reverse order)
4.  Update status to FAILED

Guarantees:

-   No half-built directories
-   No orphan links
-   No media corruption

------------------------------------------------------------------------

# 13. State Machine

SCANNED → PARSED → IDENTIFIED → LOW_CONFIDENCE → LINKING → SCRAPING →
SUCCESS / FAILED

Only SUCCESS marks processed.

FAILED allows retry.

------------------------------------------------------------------------

# 14. API Protection & Caching

-   1 identification per directory
-   Max 2 fallback rounds
-   LRU cache
-   3 req/sec limit
-   Do not permanently cache failures

------------------------------------------------------------------------

# 15. Stability Pillars (v3 Final)

1.  Directory-driven scan
2.  Layered Anime Regex engine
3.  Unified competitive scoring
4.  Controlled fallback retries
5.  Transactional execution
6.  Strong rollback system

v3 ensures industrial-grade reliability for long-term NAS deployment.

------------------------------------------------------------------------

# 16. Conclusion

Recognition Engine v3:

-   Handles \>95% anime naming patterns
-   Correctly differentiates TV vs Movie
-   Supports Roman, Final, Part, Chinese season naming
-   Minimizes API overhead
-   Fully recoverable failure model
-   Idempotent and safe for repeated scans

This document defines the final production-ready recognition core.
