# Media OCR and Classification Notes

## 1. What was implemented

This project now supports low-cost, rule-first media analysis with selective OCR fallback:

- XLSX extraction reliability improvements (namespace and regex fallback)
- Rule-based file-purpose inference in `media.html`
  - inventory classification (wine / ingredients / packaging / consumables)
- Selective PDF OCR fallback in `line-webhook` shared extractor
  - OCR is called only when normal PDF text extraction is too short
  - OCR is skipped for large files
  - in-memory dedupe cache avoids repeated OCR on same payload

Implemented mainly in:

- `supabase/functions/_shared/line_media_content_preview.ts`
- `supabase/functions/admin-api/index.ts`
- `media.html`

Additionally, file-purpose analysis now performs **rule-based category classification** during display,
so extracted text is grouped into practical restaurant-operation categories.

## 1.1 Cost-optimization goal (important)

This implementation is intentionally designed to **avoid unnecessary running cost**.

Primary policy:

- use deterministic extraction/rules first (no AI by default)
- call OCR only when normal extraction is insufficient
- avoid duplicate OCR calls with cache
- route OCR provider by file size for better cost-efficiency

In short, OCR/AI are fallback paths, not the default path.

## 1.2 Rule-based category classification (analysis behavior)

During analysis display (`media.html`), extracted text is categorized by keyword scoring.
This means the system is not just showing raw text; it also classifies likely document type.

Current main categories:

- Labor / employment documents (アルバイト・労働条件・労務)
- Shift / attendance documents (シフト・勤怠)
- Cost / inventory documents (原価・棚卸)
- Recipe / prep documents (レシピ・仕込み)
- Billing / payment documents (請求・支払)
- Sales / budget-vs-actual documents (売上・予実)
- Ordering / delivery documents (発注・納品)
- Hygiene / HACCP documents (衛生・HACCP)
- Manual / operation procedure documents (マニュアル・業務手順)

Classification approach:

- keyword hit count per category
- highest score wins
- high-confidence label when score >= 2
- conservative fallback labels when confidence is low

This is deterministic, explainable, and low-cost (no AI inference required).

## 2. OCR behavior (cost-aware)

OCR is **disabled by default**.

It runs only when:

1. file is PDF
2. normal extracted text is below threshold (`OCR_PDF_MIN_TRIGGER_CHARS`, currently 24 chars)
3. `LINE_MEDIA_OCR_MODE=ocr_space`
4. `LINE_MEDIA_OCR_SPACE_API_KEY` exists
5. file size is below max bytes (`LINE_MEDIA_OCR_PDF_MAX_BYTES`, default 2MB)

Current production mode can be `hybrid_size`:

- PDF <= 1MB: OCR.Space
- PDF > 1MB: Azure Document Intelligence

This split is also for cost and practicality:

- keep small files on lightweight OCR path
- use Azure for larger files where OCR.Space free-key size limits are restrictive

## 2.1 OCR provider currently used

Current implementation uses **OCR.Space API** as optional fallback:

- Endpoint: `https://api.ocr.space/parse/image`
- Method: `POST` (multipart/form-data)
- Request fields:
  - `apikey`: from `LINE_MEDIA_OCR_SPACE_API_KEY`
  - `language`: `jpn`
  - `isOverlayRequired`: `false`
  - `OCREngine`: `2`
  - `file`: original PDF bytes
- Response handling:
  - reads `ParsedResults[].ParsedText`
  - joins per-page text and normalizes before saving to `content_preview`

Notes:

- OCR runs only for low-text PDFs (not for all files).
- If OCR API fails, the flow falls back to the original extracted text (no hard failure).
- Overall objective is to minimize paid OCR/API usage while preserving usability.

## 3. Required Supabase secrets

Set the following on production project:

- `LINE_MEDIA_OCR_MODE` = `ocr_space`
- `LINE_MEDIA_OCR_SPACE_API_KEY` = `<your OCR.Space API key>`

Optional:

- `LINE_MEDIA_OCR_PDF_MAX_BYTES` = `2097152` (or your preferred value)

## 4. How to configure secrets

Run from repository root:

```bash
supabase secrets set LINE_MEDIA_OCR_MODE=ocr_space
supabase secrets set LINE_MEDIA_OCR_SPACE_API_KEY='YOUR_KEY_HERE'
supabase secrets set LINE_MEDIA_OCR_PDF_MAX_BYTES=2097152
```

Then deploy function:

```bash
supabase functions deploy line-webhook --yes
```

## 5. How to verify secrets are present

```bash
supabase secrets list
```

You should see the secret names in the list (values are not displayed).

## 6. Current status

From latest `supabase secrets list`, OCR secrets are now present:

- `LINE_MEDIA_OCR_MODE`
- `LINE_MEDIA_OCR_SPACE_API_KEY`
- `LINE_MEDIA_OCR_PDF_MAX_BYTES`

Current production behavior: OCR fallback is enabled when the trigger conditions are met.
