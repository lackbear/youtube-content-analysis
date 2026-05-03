# Source-200-channels prompt

The prompt used (with Claude.ai or any web-search-capable AI) to bulk-source the
initial 200-channel catalogue for chapter 6. Output: a CSV block with
`handle,niche,tier` rows that `scripts/discover.py` (chapter 6 commit 3) will
validate against the YouTube API and merge into `competitors.csv`.

Run it 6 times — one per `(niche, tier)` bucket of ~30 channels — for higher
quality than asking for all 200 in a single response.

---

## The prompt

````markdown
# Task: source YouTube channels for a content-analysis data pipeline

I'm building a portfolio data-engineering project that ingests YouTube competitor channels daily, builds a Bronze→Silver→Gold medallion via dbt+DuckDB, and tracks growth over time. I need you to suggest **200 YouTube channels** total, across 2 niches and 3 tiers. Use web search — channel handles change, sub counts drift, channels get deleted; verify recency before suggesting.

## Required distribution

| Niche | Tier | Subscriber range | Count |
|---|---|---|---|
| AI & Automation | Top | ≥ 500k | 15 |
| AI & Automation | Micro | 100k – 500k | 70 |
| AI & Automation | New | < 100k OR channel < 12 months old | 15 |
| Health & Longevity | Top | ≥ 500k | 15 |
| Health & Longevity | Micro | 100k – 500k | 70 |
| Health & Longevity | New | < 100k OR channel < 12 months old | 15 |
| **Total** | | | **200** |

## Niche definitions (positive + negative)

**AI & Automation** — IN: LLM/Claude tutorials, AI tools demos, agents, prompt engineering, AI-adjacent dev productivity, AI commentary/news, automation workflows (n8n, Zapier-style), MLops engineering. OUT: pure ML research lectures with no application angle, broad CS education without AI focus, generic "tech news" channels.

**Health & Longevity** — IN: longevity science, biohacking experiments, sleep / circadian biology, nutrition science, anti-aging research, evidence-based fitness optimisation, supplement deep-dives, peer-reviewed-paper breakdowns. OUT: generic fitness influencers, body-building motivation, broad medical news, supplement-pushing affiliate channels with no original analysis.

## Quality criteria

- **Active in the last 6 months** — verify a recent upload.
- **English-speaking primary audience** unless the channel is so niche-significant that the language barrier is worth it (rare).
- **Original content** — avoid clickbait factories, AI-slop content farms, channels that obviously buy subscribers.
- **Clear topical fit** — channels that frequently produce content matching the niche, not "they did one AI video three years ago".
- **For "New" tier specifically** — bias toward channels with strong recent growth or high signal-per-video, not just any small channel.

## Already-tracked channels (do NOT suggest these)

```
SiimLand
Physionic
DrBradStanfield
jamesbruton
BreakingTaps
BenFelixCSI
TheSwedishInvestor
ShashankKalanithi
DataVidhya
CodeWithYu
Electronoobs
nikodembartnik
```

## Output format

Output a single CSV block I can paste directly into a file. **No commentary, no markdown headers, no "here are…" preamble.** Just the CSV.

- Header row: `handle,niche,tier`
- One channel per line. Total 201 lines (header + 200 channels).
- `handle`: the YouTube `@handle` without the `@` prefix. Example: `LexFridman`.
- `niche`: exactly `AI_Automation` or `Health_Longevity` (underscore, no spaces).
- `tier`: exactly `top`, `micro`, or `new`.

Example of the format expected:

```csv
handle,niche,tier
LexFridman,AI_Automation,top
TwoMinutePapers,AI_Automation,top
PeterAttiaMD,Health_Longevity,top
```

## Anti-patterns

- Don't pad with low-quality suggestions to hit 200. If you can only confidently provide ~150 high-quality channels, output 150 + a `# need <N> more in <niche>/<tier>` comment line — I'd rather curate manually than ingest noise.
- Don't repeat handles across rows. Each channel goes in exactly one bucket.
- Don't suggest a channel without verifying it exists and is active.
- Don't suggest super-mainstream creators who only tangentially touch the niche (e.g. don't propose MrBeast under "AI" because he made one AI video).

## Suggested run strategy

If 200 in a single response stretches your context, do this in 6 separate runs of ~30 channels each — one per (niche, tier) bucket. Use the same output format for each batch; I'll concatenate. Quality > quantity.
````

---

## After running the prompt

1. Concatenate the 6 batches' CSV outputs into a single `data/curator/candidates_input.csv` (project-root path).
2. `scripts/discover.py` (chapter 6 commit 3) reads the file, validates each handle against `youtube.channels.list`, dedups against `competitors.csv` (active or inactive), and writes `data/curator/candidates.csv` with the resolved `channel_id` + verified subscriber count + `status=pending`.
3. Manual review of `candidates.csv` — mark each row `accepted` or `rejected`.
4. Accepted rows get appended to `competitors.csv` with `active=true` and `niche` matching the prompt's bucket. Legacy channels age out as the curator queue replaces stale ones.
