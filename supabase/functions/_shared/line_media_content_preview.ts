/**
 * Extract short text previews from LINE-saved file attachments (PDF, DOCX, XLSX, plain text).
 * Logic aligned with admin-api document extraction, with tighter limits for webhook runtime.
 */
import JSZip from "https://esm.sh/jszip@3.10.1"

const INTERNAL_MAX_CHARS = 60_000
const PDF_MAX_PAGES = 40
const DB_PREVIEW_MAX_CHARS = 2000
const TEXT_BINARY_RATIO_MAX = 0.08
const PDFJS_MODULE_URL = "https://esm.sh/pdfjs-dist@4.10.38/build/pdf.mjs"
const OCR_PDF_MIN_TRIGGER_CHARS = 24
const OCR_PDF_MAX_BYTES_DEFAULT = 2 * 1024 * 1024
const OCR_SPACE_MAX_BYTES_DEFAULT = 1 * 1024 * 1024
const OCR_CACHE_MAX_ENTRIES = 120
const AZURE_DOCINTEL_API_VERSION = "2024-11-30"
const AZURE_POLL_INTERVAL_MS = 1200
const AZURE_POLL_MAX_ATTEMPTS = 12

export const DOCX_MIME = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
export const XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

/** OOXML SpreadsheetML — prefer explicit NS; some parsers omit default NS URI or treat `*` oddly, so we fall back. */
const SPREADSHEETML_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"

function getSheetElementsLive(root: Document | Element, localName: string): HTMLCollectionOf<Element> {
  const byNs = root.getElementsByTagNameNS(SPREADSHEETML_NS, localName)
  if (byNs.length > 0) return byNs
  const byStar = root.getElementsByTagNameNS("*", localName)
  if (byStar.length > 0) return byStar
  return root.getElementsByTagName(localName) as HTMLCollectionOf<Element>
}

const ARCHIVE_MAX_XML_ENTRIES = 120
const ARCHIVE_MAX_ENTRIES = 400
const ARCHIVE_TOTAL_UNCOMPRESSED_MAX_BYTES = 80 * 1024 * 1024
const ARCHIVE_SINGLE_ENTRY_MAX_BYTES = 24 * 1024 * 1024
const ARCHIVE_MAX_COMPRESSION_RATIO = 40
const ARCHIVE_ENTRY_MAX_BYTES = 8 * 1024 * 1024

type ExtractMime = "application/pdf" | typeof DOCX_MIME | typeof XLSX_MIME | "text/plain"

type PdfJsTextItem = { str?: unknown }
type PdfJsTextContent = { items?: unknown }
type PdfJsPage = {
  getTextContent: () => Promise<PdfJsTextContent>
  cleanup?: () => void
}
type PdfJsDocument = {
  numPages: number
  getPage: (pageNumber: number) => Promise<PdfJsPage>
  cleanup?: () => void
  destroy?: () => void | Promise<void>
}
type PdfJsLoadingTask = {
  promise: Promise<PdfJsDocument>
  destroy?: () => void | Promise<void>
}
type PdfJsModule = {
  getDocument: (source: Record<string, unknown>) => PdfJsLoadingTask
}
type OfficeZipEntry = {
  name: string
  dir: boolean
  async: (type: "string") => Promise<string>
}

let cachedPdfJs: Promise<PdfJsModule | null> | null = null
const ocrPreviewCache = new Map<string, string>()

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null
}

function extractExt(fileName: string): string {
  const safe = String(fileName ?? "").trim()
  const idx = safe.lastIndexOf(".")
  if (idx < 0 || idx === safe.length - 1) return ""
  return safe.slice(idx + 1).toLowerCase().replace(/[^a-z0-9]/g, "")
}

function hasPdfMagicHeader(bytes: Uint8Array): boolean {
  if (bytes.length < 5) return false
  return bytes[0] === 0x25 && bytes[1] === 0x50 && bytes[2] === 0x44 && bytes[3] === 0x46 && bytes[4] === 0x2d
}

function hasZipMagicHeader(bytes: Uint8Array): boolean {
  if (bytes.length < 4) return false
  if (bytes[0] !== 0x50 || bytes[1] !== 0x4b) return false
  const marker = bytes[2] * 256 + bytes[3]
  return marker === 0x0304 || marker === 0x0506 || marker === 0x0708
}

function looksLikeBinaryTextPayload(bytes: Uint8Array): boolean {
  if (bytes.length === 0) return false
  const sampleSize = Math.min(bytes.length, 4096)
  let binaryCount = 0
  for (let i = 0; i < sampleSize; i += 1) {
    const value = bytes[i]
    const isAllowedControl = value === 9 || value === 10 || value === 13
    const isTextByte = value >= 32 && value <= 126
    const isMultiByteLead = value >= 0x80
    if (!isAllowedControl && !isTextByte && !isMultiByteLead) binaryCount += 1
  }
  return (binaryCount / sampleSize) > TEXT_BINARY_RATIO_MAX
}

function tryDecodeUtf8(bytes: Uint8Array): string {
  try {
    return new TextDecoder("utf-8", { fatal: false }).decode(bytes)
  } catch {
    return ""
  }
}

function normalizeExtractedText(value: string): string {
  const normalized = String(value ?? "")
    .replace(/\u0000/g, " ")
    .replace(/\r\n?/g, "\n")
    .trim()
  if (!normalized) return ""
  if (normalized.length <= INTERNAL_MAX_CHARS) return normalized
  return normalized.slice(0, INTERNAL_MAX_CHARS).trimEnd()
}

function parsePositiveIntEnv(value: string | undefined, fallback: number): number {
  const n = Number(value ?? "")
  if (!Number.isFinite(n) || n <= 0) return fallback
  return Math.floor(n)
}

function fnv1a32Hex(bytes: Uint8Array): string {
  let hash = 0x811c9dc5
  for (let i = 0; i < bytes.length; i += 1) {
    hash ^= bytes[i]
    hash = Math.imul(hash, 0x01000193)
  }
  return (hash >>> 0).toString(16).padStart(8, "0")
}

function getOcrCacheKey(bytes: Uint8Array, fileName: string): string {
  const len = bytes.length
  if (len <= 4096) return `${len}:${fnv1a32Hex(bytes)}:${fileName}`
  const head = bytes.slice(0, 2048)
  const tail = bytes.slice(len - 2048)
  const merged = new Uint8Array(head.length + tail.length)
  merged.set(head, 0)
  merged.set(tail, head.length)
  return `${len}:${fnv1a32Hex(merged)}:${fileName}`
}

function setOcrCache(cacheKey: string, text: string): void {
  if (!cacheKey || !text) return
  if (ocrPreviewCache.has(cacheKey)) ocrPreviewCache.delete(cacheKey)
  ocrPreviewCache.set(cacheKey, text)
  if (ocrPreviewCache.size <= OCR_CACHE_MAX_ENTRIES) return
  const oldestKey = ocrPreviewCache.keys().next().value
  if (typeof oldestKey === "string") ocrPreviewCache.delete(oldestKey)
}

async function tryExtractPdfTextViaOcrSpace(bytes: Uint8Array, originalFileName: string): Promise<string> {
  const apiKey = String(Deno.env.get("LINE_MEDIA_OCR_SPACE_API_KEY") ?? "").trim()
  if (!apiKey) return ""

  const maxBytes = parsePositiveIntEnv(Deno.env.get("LINE_MEDIA_OCR_PDF_MAX_BYTES") ?? undefined, OCR_PDF_MAX_BYTES_DEFAULT)
  if (bytes.length <= 0 || bytes.length > maxBytes) return ""

  const cacheKey = getOcrCacheKey(bytes, originalFileName)
  const cached = ocrPreviewCache.get(cacheKey)
  if (cached) return cached

  const safeFileName = String(originalFileName || "line-file.pdf").trim() || "line-file.pdf"
  const body = new FormData()
  body.append("apikey", apiKey)
  body.append("language", "jpn")
  body.append("isOverlayRequired", "false")
  body.append("OCREngine", "2")
  body.append("file", new Blob([bytes], { type: "application/pdf" }), safeFileName)

  try {
    const res = await fetch("https://api.ocr.space/parse/image", { method: "POST", body })
    if (!res.ok) {
      console.warn(`line_media_content_preview: OCR.Space HTTP ${res.status}`)
      return ""
    }
    const payload = await res.json().catch(() => null)
    if (!isRecord(payload)) return ""
    const parsedResults = Array.isArray(payload.ParsedResults) ? payload.ParsedResults : []
    const joined = parsedResults
      .map((entry) => {
        if (!isRecord(entry)) return ""
        const text = entry.ParsedText
        return typeof text === "string" ? text : ""
      })
      .join("\n")
    const normalized = normalizeExtractedText(joined)
    if (!normalized) return ""
    setOcrCache(cacheKey, normalized)
    return normalized
  } catch (error) {
    console.error("line_media_content_preview: OCR.Space failed", error)
    return ""
  }
}

function sanitizeAzureEndpoint(raw: string): string {
  return String(raw ?? "").trim().replace(/\/+$/, "")
}

async function sleepMs(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms))
}

function readAzureAnalyzeText(payload: unknown): string {
  if (!isRecord(payload)) return ""
  const analyzeResult = isRecord(payload.analyzeResult) ? payload.analyzeResult : null
  if (!analyzeResult) return ""
  const content = analyzeResult.content
  if (typeof content === "string" && content.trim()) return content

  const pages = Array.isArray(analyzeResult.pages) ? analyzeResult.pages : []
  const lines: string[] = []
  for (const page of pages) {
    if (!isRecord(page)) continue
    const lineRows = Array.isArray(page.lines) ? page.lines : []
    for (const row of lineRows) {
      if (!isRecord(row)) continue
      const text = row.content
      if (typeof text === "string" && text.trim()) lines.push(text)
    }
  }
  return lines.join("\n")
}

async function tryExtractPdfTextViaAzureDocIntel(bytes: Uint8Array, originalFileName: string): Promise<string> {
  const endpoint = sanitizeAzureEndpoint(Deno.env.get("LINE_MEDIA_AZURE_DOCINTEL_ENDPOINT") ?? "")
  const apiKey = String(Deno.env.get("LINE_MEDIA_AZURE_DOCINTEL_KEY") ?? "").trim()
  if (!endpoint || !apiKey) return ""

  const maxBytes = parsePositiveIntEnv(Deno.env.get("LINE_MEDIA_AZURE_DOCINTEL_MAX_BYTES") ?? undefined, 20 * 1024 * 1024)
  if (bytes.length <= 0 || bytes.length > maxBytes) return ""

  const cacheKey = `az:${getOcrCacheKey(bytes, originalFileName)}`
  const cached = ocrPreviewCache.get(cacheKey)
  if (cached) return cached

  const analyzeUrl = `${endpoint}/documentintelligence/documentModels/prebuilt-read:analyze?api-version=${AZURE_DOCINTEL_API_VERSION}`
  try {
    const startRes = await fetch(analyzeUrl, {
      method: "POST",
      headers: {
        "Ocp-Apim-Subscription-Key": apiKey,
        "Content-Type": "application/pdf",
      },
      body: bytes,
    })
    if (!startRes.ok) {
      const bodyText = await startRes.text().catch(() => "")
      console.warn(`line_media_content_preview: Azure analyze start failed (${startRes.status}) ${bodyText.slice(0, 300)}`)
      return ""
    }

    const operationLocation = String(startRes.headers.get("operation-location") ?? "").trim()
    if (!operationLocation) {
      console.warn("line_media_content_preview: Azure operation-location missing")
      return ""
    }

    for (let attempt = 0; attempt < AZURE_POLL_MAX_ATTEMPTS; attempt += 1) {
      if (attempt > 0) await sleepMs(AZURE_POLL_INTERVAL_MS)
      const pollRes = await fetch(operationLocation, {
        method: "GET",
        headers: {
          "Ocp-Apim-Subscription-Key": apiKey,
        },
      })
      if (!pollRes.ok) {
        const bodyText = await pollRes.text().catch(() => "")
        console.warn(`line_media_content_preview: Azure poll failed (${pollRes.status}) ${bodyText.slice(0, 300)}`)
        return ""
      }
      const payload = await pollRes.json().catch(() => null)
      if (!isRecord(payload)) return ""
      const status = String(payload.status ?? "").toLowerCase()
      if (status === "running" || status === "notstarted") continue
      if (status !== "succeeded") {
        console.warn(`line_media_content_preview: Azure analyze status=${status}`)
        return ""
      }
      const rawText = readAzureAnalyzeText(payload)
      const normalized = normalizeExtractedText(rawText)
      if (!normalized) return ""
      setOcrCache(cacheKey, normalized)
      return normalized
    }
    console.warn("line_media_content_preview: Azure analyze polling timeout")
    return ""
  } catch (error) {
    console.error("line_media_content_preview: Azure OCR failed", error)
    return ""
  }
}

async function maybeExtractPdfTextWithOcrFallback(
  bytes: Uint8Array,
  originalFileName: string,
  extractedPdfText: string,
): Promise<string> {
  const current = normalizeExtractedText(extractedPdfText)
  if (current.length >= OCR_PDF_MIN_TRIGGER_CHARS) return current
  const mode = String(Deno.env.get("LINE_MEDIA_OCR_MODE") ?? "").trim().toLowerCase()
  if (!mode) return current

  if (mode === "ocr_space") {
    const ocrText = await tryExtractPdfTextViaOcrSpace(bytes, originalFileName)
    return ocrText || current
  }
  if (mode === "azure_docintel") {
    const ocrText = await tryExtractPdfTextViaAzureDocIntel(bytes, originalFileName)
    return ocrText || current
  }
  if (mode === "hybrid_size") {
    const ocrSpaceMaxBytes = parsePositiveIntEnv(
      Deno.env.get("LINE_MEDIA_OCR_SPACE_MAX_BYTES") ?? undefined,
      OCR_SPACE_MAX_BYTES_DEFAULT,
    )
    if (bytes.length <= ocrSpaceMaxBytes) {
      const smallText = await tryExtractPdfTextViaOcrSpace(bytes, originalFileName)
      return smallText || current
    }
    const largeText = await tryExtractPdfTextViaAzureDocIntel(bytes, originalFileName)
    return largeText || current
  }
  return current
}

/** Single-line preview safe for DB / LINE (max DB_PREVIEW_MAX_CHARS). */
export function clipPreviewForStorage(text: string): string {
  const singleLine = String(text ?? "")
    .replace(/\s+/g, " ")
    .trim()
  if (!singleLine) return ""
  if (singleLine.length <= DB_PREVIEW_MAX_CHARS) return singleLine
  return `${singleLine.slice(0, DB_PREVIEW_MAX_CHARS - 1).trimEnd()}…`
}

export function resolveLineFileExtractMime(
  contentType: string,
  fileName: string,
  bytes: Uint8Array,
): ExtractMime | null {
  const ct = String(contentType ?? "").split(";")[0].trim().toLowerCase()
  const ext = extractExt(fileName)

  const textishExt = new Set(["txt", "csv", "tsv", "md", "log", "json", "xml", "html", "htm"])
  if (
    ct === "text/plain" ||
    ct === "text/csv" ||
    ct === "text/tab-separated-values" ||
    ct === "application/json" ||
    ct === "text/markdown" ||
    textishExt.has(ext)
  ) {
    return "text/plain"
  }

  if (ct === "application/pdf" || ext === "pdf") {
    if (hasPdfMagicHeader(bytes)) return "application/pdf"
    return null
  }

  /** Excel OOXML（xlsx / xlsm。LINE 側の Content-Type 揺れも含む） */
  const spreadsheetishCt =
    ct === XLSX_MIME ||
    ct.includes("spreadsheetml.sheet") ||
    ct.includes("ms-excel.sheet.macroenabled")

  if (spreadsheetishCt || ext === "xlsx" || ext === "xlsm") {
    if (hasZipMagicHeader(bytes)) return XLSX_MIME
    return null
  }

  if (ct === DOCX_MIME || ext === "docx" || ext === "docm") {
    if (!hasZipMagicHeader(bytes)) return null
    return DOCX_MIME
  }

  if (ext === "pdf" && hasPdfMagicHeader(bytes)) return "application/pdf"
  if ((ext === "docx" || ext === "docm") && hasZipMagicHeader(bytes)) return DOCX_MIME
  if ((ext === "xlsx" || ext === "xlsm") && hasZipMagicHeader(bytes)) return XLSX_MIME

  return null
}

/**
 * LINE などが `application/octet-stream` かつファイル名に拡張子が無い場合でも、
 * OOXML ZIP の典型エントリで Excel / Word を判定する。
 */
async function sniffOpenXmlOfficeKind(
  bytes: Uint8Array,
): Promise<typeof DOCX_MIME | typeof XLSX_MIME | null> {
  if (!hasZipMagicHeader(bytes)) return null
  try {
    const zip = await JSZip.loadAsync(bytes, { checkCRC32: false, createFolders: false })
    if (zip.file("xl/workbook.xml")) return XLSX_MIME
    if (zip.file("word/document.xml")) return DOCX_MIME
  } catch {
    return null
  }
  return null
}

async function inspectOfficeArchiveSafety(
  bytes: Uint8Array,
  mimeType: typeof DOCX_MIME | typeof XLSX_MIME,
): Promise<string | null> {
  let zip: JSZip
  try {
    zip = await JSZip.loadAsync(bytes, { checkCRC32: false, createFolders: false })
  } catch {
    return "Failed to parse Office archive."
  }

  const entries = Object.values(zip.files).filter((entry) => !entry.dir)
  if (entries.length === 0) return "Office archive has no files."
  if (entries.length > ARCHIVE_MAX_ENTRIES) return "Office archive has too many entries."

  let totalUncompressed = 0
  let totalCompressed = 0
  for (const entry of entries) {
    const entryName = String(entry.name || "")
    if (!entryName || entryName.startsWith("/") || entryName.includes("../") || entryName.includes("..\\")) {
      return "Office archive contains unsafe entry paths."
    }
    const uncompressedSize = Number((entry as any)?._data?.uncompressedSize ?? 0)
    const compressedSize = Number((entry as any)?._data?.compressedSize ?? 0)
    if (Number.isFinite(uncompressedSize) && uncompressedSize > ARCHIVE_SINGLE_ENTRY_MAX_BYTES) {
      return "Office archive entry exceeds allowed size."
    }
    if (Number.isFinite(uncompressedSize) && uncompressedSize > 0) {
      totalUncompressed += uncompressedSize
      if (totalUncompressed > ARCHIVE_TOTAL_UNCOMPRESSED_MAX_BYTES) {
        return "Office archive exceeds uncompressed size limit."
      }
    }
    if (Number.isFinite(compressedSize) && compressedSize > 0) totalCompressed += compressedSize
  }

  if (totalCompressed > 0) {
    const compressionRatio = totalUncompressed / totalCompressed
    if (compressionRatio > ARCHIVE_MAX_COMPRESSION_RATIO) {
      return "Office archive compression ratio is too high."
    }
  }

  if (!zip.file("[Content_Types].xml")) return "Office archive is missing required metadata."
  if (mimeType === DOCX_MIME && !zip.file("word/document.xml")) {
    return "DOCX payload is missing word/document.xml."
  }
  if (mimeType === XLSX_MIME && !zip.file("xl/workbook.xml")) {
    return "XLSX payload is missing xl/workbook.xml."
  }
  return null
}

async function validatePayload(bytes: Uint8Array, mime: ExtractMime): Promise<string | null> {
  if (mime === "application/pdf" && !hasPdfMagicHeader(bytes)) return "Invalid PDF payload."
  if (mime === "text/plain" && looksLikeBinaryTextPayload(bytes)) {
    return "Text file appears to contain binary data."
  }
  if (mime === DOCX_MIME || mime === XLSX_MIME) {
    if (!hasZipMagicHeader(bytes)) return "Office file must be a valid ZIP container."
    return await inspectOfficeArchiveSafety(bytes, mime)
  }
  return null
}

async function loadPdfJsModule(): Promise<PdfJsModule | null> {
  if (!cachedPdfJs) {
    cachedPdfJs = import(PDFJS_MODULE_URL)
      .then((mod) => {
        if (isRecord(mod) && typeof mod.getDocument === "function") return mod as unknown as PdfJsModule
        console.error("line_media_content_preview: pdfjs getDocument missing")
        return null
      })
      .catch((e) => {
        console.error("line_media_content_preview: pdfjs load failed", e)
        return null
      })
  }
  return await cachedPdfJs
}

async function extractPdfText(bytes: Uint8Array): Promise<string> {
  const pdfjs = await loadPdfJsModule()
  if (!pdfjs) return ""

  let loadingTask: PdfJsLoadingTask | null = null
  let pdfDocument: PdfJsDocument | null = null
  try {
    loadingTask = pdfjs.getDocument({
      data: bytes,
      disableWorker: true,
      useSystemFonts: false,
      isEvalSupported: false,
      stopAtErrors: false,
    })
    pdfDocument = await loadingTask.promise
    const numPages = Number(pdfDocument.numPages || 0)
    if (!Number.isFinite(numPages) || numPages <= 0) return ""

    const pagesToRead = Math.min(numPages, PDF_MAX_PAGES)
    const chunks: string[] = []
    let extractedChars = 0

    for (let pageNumber = 1; pageNumber <= pagesToRead; pageNumber += 1) {
      let page: PdfJsPage | null = null
      try {
        page = await pdfDocument.getPage(pageNumber)
        const textContent = await page.getTextContent()
        const rawItems = Array.isArray(textContent?.items) ? textContent.items : []
        const pageText = rawItems
          .map((item) => {
            if (!isRecord(item)) return ""
            const strValue = (item as PdfJsTextItem).str
            return typeof strValue === "string" ? strValue : ""
          })
          .join(" ")
          .replace(/\s+/g, " ")
          .trim()
        if (!pageText) continue
        const remaining = INTERNAL_MAX_CHARS - extractedChars
        if (remaining <= 0) break
        const clipped = pageText.length > remaining ? pageText.slice(0, remaining) : pageText
        chunks.push(clipped)
        extractedChars += clipped.length + 1
        if (extractedChars >= INTERNAL_MAX_CHARS) break
      } catch (pageError) {
        console.error(`line_media_content_preview: PDF page ${pageNumber}`, pageError)
      } finally {
        try {
          page?.cleanup?.()
        } catch {
          /* no-op */
        }
      }
    }
    return normalizeExtractedText(chunks.join("\n"))
  } catch (error) {
    console.error("line_media_content_preview: extractPdfText", error)
    return ""
  } finally {
    try {
      pdfDocument?.cleanup?.()
    } catch {
      /* no-op */
    }
    try {
      await pdfDocument?.destroy?.()
    } catch {
      /* no-op */
    }
    try {
      await loadingTask?.destroy?.()
    } catch {
      /* no-op */
    }
  }
}

async function loadOfficeZip(bytes: Uint8Array): Promise<JSZip | null> {
  try {
    return await JSZip.loadAsync(bytes, { checkCRC32: false, createFolders: false })
  } catch (error) {
    console.error("line_media_content_preview: loadOfficeZip", error)
    return null
  }
}

function parseXmlDocument(xml: string): Document | null {
  if (!xml) return null
  try {
    const doc = new DOMParser().parseFromString(xml, "application/xml")
    const parserErrors = doc.getElementsByTagName("parsererror")
    if (parserErrors && parserErrors.length > 0) return null
    return doc
  } catch {
    return null
  }
}

function getOfficeXmlEntries(zip: JSZip, predicate: (entryName: string) => boolean): OfficeZipEntry[] {
  return Object.values(zip.files)
    .filter((entry) => !entry.dir && predicate(entry.name))
    .slice(0, ARCHIVE_MAX_XML_ENTRIES) as unknown as OfficeZipEntry[]
}

async function readOfficeXmlEntry(entry: OfficeZipEntry): Promise<string> {
  try {
    const uncompressedSize = Number((entry as any)?._data?.uncompressedSize ?? 0)
    if (Number.isFinite(uncompressedSize) && uncompressedSize > ARCHIVE_ENTRY_MAX_BYTES) {
      return ""
    }
    const raw = await entry.async("string")
    if (!raw) return ""
    if (raw.length > ARCHIVE_ENTRY_MAX_BYTES) return raw.slice(0, ARCHIVE_ENTRY_MAX_BYTES)
    return raw
  } catch (error) {
    console.error(`line_media_content_preview: readOfficeXml ${entry.name}`, error)
    return ""
  }
}

function appendChunkWithinLimit(chunks: string[], chunk: string, remainingChars: number): number {
  if (!Number.isFinite(remainingChars) || remainingChars <= 0) return 0
  const text = String(chunk ?? "")
  if (!text || !/\S/.test(text)) return remainingChars
  const clipped = text.length > remainingChars ? text.slice(0, remainingChars) : text
  chunks.push(clipped)
  return remainingChars - clipped.length - 1
}

function appendWordXmlNodeText(node: Node, out: string[]): void {
  if (node.nodeType === Node.TEXT_NODE) {
    const value = node.nodeValue
    if (value) out.push(value)
    return
  }
  if (node.nodeType !== Node.ELEMENT_NODE) return
  const el = node as Element
  const local = String(el.localName || "").toLowerCase()
  if (local === "t") {
    const text = el.textContent || ""
    if (text) out.push(text)
    return
  }
  if (local === "tab") {
    out.push("\t")
    return
  }
  if (local === "br" || local === "cr") {
    out.push("\n")
    return
  }
  for (const child of Array.from(el.childNodes)) appendWordXmlNodeText(child, out)
  if (local === "p" || local === "tr") out.push("\n")
  else if (local === "tc") out.push("\t")
}

function extractWordXmlText(xml: string): string {
  const doc = parseXmlDocument(xml)
  if (!doc || !doc.documentElement) return ""
  const out: string[] = []
  appendWordXmlNodeText(doc.documentElement, out)
  return out.join("")
}

function compareWordXmlEntry(a: string, b: string): number {
  const rank = (entryName: string): number => {
    if (entryName === "word/document.xml") return 0
    if (entryName.startsWith("word/header")) return 1
    if (entryName.startsWith("word/footer")) return 2
    if (entryName.startsWith("word/footnotes")) return 3
    if (entryName.startsWith("word/endnotes")) return 4
    return 9
  }
  const diff = rank(a) - rank(b)
  return diff !== 0 ? diff : a.localeCompare(b)
}

async function extractDocxText(bytes: Uint8Array): Promise<string> {
  const zip = await loadOfficeZip(bytes)
  if (!zip) return ""
  const entries = getOfficeXmlEntries(zip, (entryName) =>
    entryName.startsWith("word/") &&
    entryName.endsWith(".xml") &&
    !entryName.includes("/_rels/"),
  ).sort((a, b) => compareWordXmlEntry(a.name, b.name))

  const chunks: string[] = []
  let remainingChars = INTERNAL_MAX_CHARS
  for (const entry of entries) {
    if (remainingChars <= 0) break
    const xml = await readOfficeXmlEntry(entry)
    if (!xml) continue
    const text = extractWordXmlText(xml)
    if (!text) continue
    remainingChars = appendChunkWithinLimit(chunks, text, remainingChars)
  }
  return normalizeExtractedText(chunks.join("\n"))
}

function collectTextNodes(root: Element, localName: string): string[] {
  const nodes = getSheetElementsLive(root, localName)
  const out: string[] = []
  for (let i = 0; i < nodes.length; i += 1) {
    const value = nodes.item(i)?.textContent ?? ""
    if (value) out.push(value)
  }
  return out
}

function parseXlsxSharedStrings(xml: string): string[] {
  const doc = parseXmlDocument(xml)
  if (!doc || !doc.documentElement) return []
  const siNodes = getSheetElementsLive(doc, "si")
  const out: string[] = []
  for (let i = 0; i < siNodes.length; i += 1) {
    const si = siNodes.item(i)
    if (!si) {
      out.push("")
      continue
    }
    out.push(collectTextNodes(si, "t").join(""))
  }
  return out
}

function getDirectChildElementsByLocalName(root: Element, localName: string): Element[] {
  const out: Element[] = []
  const nodes = root.childNodes
  for (let i = 0; i < nodes.length; i += 1) {
    const child = nodes.item(i)
    if (!child) continue
    if (child.nodeType !== Node.ELEMENT_NODE) continue
    const el = child as Element
    if ((el.localName || "").toLowerCase() === localName) out.push(el)
  }
  return out
}

function getFirstSpreadsheetDescendant(root: Element, localName: string): Element | null {
  const byNs = root.getElementsByTagNameNS(SPREADSHEETML_NS, localName)
  if (byNs.length > 0) return byNs.item(0)
  const byStar = root.getElementsByTagNameNS("*", localName)
  if (byStar.length > 0) return byStar.item(0)
  const byTag = root.getElementsByTagName(localName)
  return byTag.length > 0 ? byTag.item(0) : null
}

function columnNameToIndex(columnName: string): number | null {
  const normalized = String(columnName || "").trim().toUpperCase()
  if (!normalized || !/^[A-Z]+$/.test(normalized)) return null
  let value = 0
  for (let i = 0; i < normalized.length; i += 1) {
    value = value * 26 + (normalized.charCodeAt(i) - 64)
  }
  return value - 1
}

function getColumnIndexFromCellRef(cellRef: string): number | null {
  const match = String(cellRef || "").toUpperCase().match(/^([A-Z]+)\d+$/)
  if (!match) return null
  return columnNameToIndex(match[1])
}

function extractXlsxCellText(cell: Element, sharedStrings: string[]): string {
  const cellType = String(cell.getAttribute("t") || "").trim().toLowerCase()
  const valueEl = getFirstSpreadsheetDescendant(cell, "v")
  const rawValue = String(valueEl?.textContent ?? "").trim()
  if (cellType === "s") {
    const idx = Number(rawValue)
    if (Number.isInteger(idx) && idx >= 0 && idx < sharedStrings.length) return String(sharedStrings[idx] || "")
    return ""
  }
  if (cellType === "inlineStr") {
    const inlineEl = getFirstSpreadsheetDescendant(cell, "is")
    if (!inlineEl) return ""
    return collectTextNodes(inlineEl, "t").join("")
  }
  if (cellType === "b") {
    if (rawValue === "1") return "TRUE"
    if (rawValue === "0") return "FALSE"
  }
  if (rawValue) return rawValue
  const formulaEl = getFirstSpreadsheetDescendant(cell, "f")
  const formula = String(formulaEl?.textContent ?? "").trim()
  return formula ? `=${formula}` : ""
}

function extractXlsxSheetText(xml: string, sharedStrings: string[]): string {
  const doc = parseXmlDocument(xml)
  if (!doc || !doc.documentElement) return ""
  const rowNodes = getSheetElementsLive(doc, "row")
  const lines: string[] = []
  for (let i = 0; i < rowNodes.length; i += 1) {
    const row = rowNodes.item(i)
    if (!row) continue
    const cellNodes = getDirectChildElementsByLocalName(row, "c")
    if (cellNodes.length === 0) continue
    const cols: string[] = []
    let nextCol = 0
    for (const cell of cellNodes) {
      const ref = String(cell.getAttribute("r") || "")
      const indexedCol = getColumnIndexFromCellRef(ref)
      const col = indexedCol == null ? nextCol : indexedCol
      while (nextCol < col) {
        cols.push("")
        nextCol += 1
      }
      cols.push(extractXlsxCellText(cell, sharedStrings))
      nextCol = col + 1
    }
    while (cols.length > 0 && !String(cols[cols.length - 1] || "").trim()) cols.pop()
    if (cols.length === 0) continue
    lines.push(cols.join("\t"))
  }
  return lines.join("\n")
}

function compareXlsxWorksheetEntry(a: string, b: string): number {
  const parse = (name: string): number => {
    const match = name.match(/sheet(\d+)\.xml$/)
    if (!match) return Number.POSITIVE_INFINITY
    return Number(match[1])
  }
  const diff = parse(a) - parse(b)
  return Number.isFinite(diff) && diff !== 0 ? diff : a.localeCompare(b)
}

function decodeXmlEntities(value: string): string {
  return String(value ?? "")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, "\"")
    .replace(/&#39;/g, "'")
}

function stripXmlTags(value: string): string {
  return decodeXmlEntities(String(value ?? "").replace(/<[^>]+>/g, ""))
}

function parseXlsxSharedStringsByRegex(xml: string): string[] {
  const out: string[] = []
  const siRe = /<si\b[^>]*>([\s\S]*?)<\/si>/gi
  let siMatch: RegExpExecArray | null
  while ((siMatch = siRe.exec(xml)) !== null) {
    const siBody = String(siMatch[1] ?? "")
    const tRe = /<t\b[^>]*>([\s\S]*?)<\/t>/gi
    let tMatch: RegExpExecArray | null
    const pieces: string[] = []
    while ((tMatch = tRe.exec(siBody)) !== null) {
      pieces.push(stripXmlTags(String(tMatch[1] ?? "")))
    }
    out.push(pieces.join(""))
  }
  return out
}

function parseXlsxSheetTextByRegex(xml: string, sharedStrings: string[]): string {
  const lines: string[] = []
  const rowRe = /<row\b[^>]*>([\s\S]*?)<\/row>/gi
  let rowMatch: RegExpExecArray | null

  while ((rowMatch = rowRe.exec(xml)) !== null) {
    const rowBody = String(rowMatch[1] ?? "")
    const cols: string[] = []
    let nextCol = 0
    const cellRe = /<c\b([^>]*)>([\s\S]*?)<\/c>/gi
    let cellMatch: RegExpExecArray | null

    while ((cellMatch = cellRe.exec(rowBody)) !== null) {
      const attr = String(cellMatch[1] ?? "")
      const body = String(cellMatch[2] ?? "")
      const refMatch = /\br="([A-Z]+\d+)"/i.exec(attr)
      const idx = refMatch ? getColumnIndexFromCellRef(refMatch[1]) : null
      const col = idx == null ? nextCol : idx
      while (nextCol < col) {
        cols.push("")
        nextCol += 1
      }

      const typeMatch = /\bt="([^"]+)"/i.exec(attr)
      const cellType = String(typeMatch?.[1] ?? "").trim().toLowerCase()
      let text = ""
      if (cellType === "s") {
        const vMatch = /<v\b[^>]*>([\s\S]*?)<\/v>/i.exec(body)
        const sharedIdx = Number(String(vMatch?.[1] ?? "").trim())
        if (Number.isInteger(sharedIdx) && sharedIdx >= 0 && sharedIdx < sharedStrings.length) {
          text = String(sharedStrings[sharedIdx] ?? "")
        }
      } else if (cellType === "inlineStr") {
        const tRe = /<t\b[^>]*>([\s\S]*?)<\/t>/gi
        let tMatch: RegExpExecArray | null
        const pieces: string[] = []
        while ((tMatch = tRe.exec(body)) !== null) {
          pieces.push(stripXmlTags(String(tMatch[1] ?? "")))
        }
        text = pieces.join("")
      } else {
        const vMatch = /<v\b[^>]*>([\s\S]*?)<\/v>/i.exec(body)
        const rawValue = stripXmlTags(String(vMatch?.[1] ?? "")).trim()
        if (rawValue) {
          text = rawValue
        } else {
          const fMatch = /<f\b[^>]*>([\s\S]*?)<\/f>/i.exec(body)
          const formula = stripXmlTags(String(fMatch?.[1] ?? "")).trim()
          if (formula) text = `=${formula}`
        }
      }
      cols.push(text)
      nextCol = col + 1
    }

    while (cols.length > 0 && !String(cols[cols.length - 1] ?? "").trim()) cols.pop()
    if (cols.length > 0) lines.push(cols.join("\t"))
  }
  return lines.join("\n")
}

async function extractXlsxText(bytes: Uint8Array): Promise<string> {
  const zip = await loadOfficeZip(bytes)
  if (!zip) return ""
  const sharedEntry = zip.file("xl/sharedStrings.xml") as unknown as OfficeZipEntry | null
  const sharedStrings = sharedEntry ? parseXlsxSharedStrings(await readOfficeXmlEntry(sharedEntry)) : []
  const worksheetEntries = getOfficeXmlEntries(zip, (entryName) =>
    entryName.startsWith("xl/worksheets/") &&
    entryName.endsWith(".xml") &&
    !entryName.includes("/_rels/"),
  ).sort((a, b) => compareXlsxWorksheetEntry(a.name, b.name))

  const chunks: string[] = []
  let remainingChars = INTERNAL_MAX_CHARS
  for (const entry of worksheetEntries) {
    if (remainingChars <= 0) break
    const xml = await readOfficeXmlEntry(entry)
    if (!xml) continue
    const text = extractXlsxSheetText(xml, sharedStrings)
    if (!text) continue
    remainingChars = appendChunkWithinLimit(chunks, text, remainingChars)
  }
  const domText = normalizeExtractedText(chunks.join("\n"))
  if (domText) return domText

  // Fallback for runtime-specific DOMParser namespace quirks on some XLSX payloads.
  const sharedXml = sharedEntry ? await readOfficeXmlEntry(sharedEntry) : ""
  const sharedByRegex = sharedXml ? parseXlsxSharedStringsByRegex(sharedXml) : []
  const regexChunks: string[] = []
  let remainingRegexChars = INTERNAL_MAX_CHARS
  for (const entry of worksheetEntries) {
    if (remainingRegexChars <= 0) break
    const xml = await readOfficeXmlEntry(entry)
    if (!xml) continue
    const text = parseXlsxSheetTextByRegex(xml, sharedByRegex)
    if (!text) continue
    remainingRegexChars = appendChunkWithinLimit(regexChunks, text, remainingRegexChars)
  }
  return normalizeExtractedText(regexChunks.join("\n"))
}

async function extractByMime(bytes: Uint8Array, mime: ExtractMime, originalFileName: string): Promise<string> {
  if (mime === "text/plain") return normalizeExtractedText(tryDecodeUtf8(bytes))
  if (mime === "application/pdf") {
    const extracted = await extractPdfText(bytes)
    return await maybeExtractPdfTextWithOcrFallback(bytes, originalFileName, extracted)
  }
  if (mime === DOCX_MIME) return await extractDocxText(bytes)
  if (mime === XLSX_MIME) return await extractXlsxText(bytes)
  return ""
}

/**
 * Returns a short single-line preview for DB storage, or null if unsupported / empty / invalid.
 */
export async function extractLineMediaFileContentPreview(
  bytes: Uint8Array,
  contentType: string,
  originalFileName: string,
): Promise<string | null> {
  let mime = resolveLineFileExtractMime(contentType, originalFileName, bytes)
  if (!mime) {
    mime = await sniffOpenXmlOfficeKind(bytes)
  }
  if (!mime) return null

  const err = await validatePayload(bytes, mime)
  if (err) {
    console.warn(`line_media_content_preview: skip (${originalFileName}): ${err}`)
    return null
  }

  try {
    const raw = await extractByMime(bytes, mime, originalFileName)
    if (!raw) return null
    const clipped = clipPreviewForStorage(raw)
    return clipped || null
  } catch (e) {
    console.error("line_media_content_preview: extract failed", e)
    return null
  }
}
