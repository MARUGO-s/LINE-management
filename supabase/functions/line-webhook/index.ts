import "jsr:@supabase/functions-js/edge-runtime.d.ts"
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2.44.0'

type CalendarListScope =
  | 'today'
  | 'tomorrow'
  | 'week'
  | 'next_week'
  | 'date'
  | 'month'
  | 'next_month'
  | 'year_month'
  | 'year'
  | 'upcoming_30d'

type CalendarCommand =
  | {
      kind: 'create'
      date: string
      time: string
      durationMin: number
      title: string
    }
  | {
      kind: 'list'
      scope: CalendarListScope
      date?: string
      month?: number
      year?: number
      keyword?: string
    }

type CalendarCreateCommand = Extract<CalendarCommand, { kind: 'create' }>

type CalendarCommandParseResult = {
  matched: boolean
  command: CalendarCommand | null
  error: string | null
}

type CalendarEnv = {
  calendarId: string
  serviceAccountEmail: string
  serviceAccountPrivateKey: string
  timezone: string
}

type CalendarEnvState =
  | { ok: true; env: CalendarEnv }
  | { ok: false; missing: string[] }

type AiCalendarIntent = {
  shouldCreate: boolean
  confidence: number
  title: string
  date: string
  time: string
  durationMin: number
  reason: string
}

type PendingCalendarConfirmation = {
  storage: 'pending_table' | 'line_messages'
  id: string
  conversation_key: string
  title: string
  date: string
  time: string
  duration_min: number
  confidence: number
  reason: string | null
  expires_at: string
}

type AiListIntent = {
  scope: CalendarListScope
  date?: string
  month?: number
  year?: number
  keyword?: string
  confidence: number
}

type GoogleCalendarEvent = {
  id?: string
  summary?: string
  description?: string
  location?: string
  attendees?: Array<{ email?: string; displayName?: string }>
  start?: { date?: string; dateTime?: string }
  end?: { date?: string; dateTime?: string }
}

type CalendarCreateResult =
  | { ok: true; summary: string; startDate: Date; endDate: Date }
  | { ok: false; error: string }

type MessageRetentionDays = 60 | 120 | 180

type MessageSearchScope = 'current_room' | 'all_rooms'

type MessageSearchCommand = {
  kind: 'search_messages'
  keyword: string
  days: MessageRetentionDays
  scope: MessageSearchScope
}

type MessageSearchParseResult = {
  matched: boolean
  command: MessageSearchCommand | null
  error: string | null
}

type AiMessageSearchIntent = {
  shouldSearch: boolean
  keyword: string
  days: MessageRetentionDays
  scope: MessageSearchScope
  confidence: number
}

type SearchMessageRow = {
  room_id: string
  room_label?: string | null
  content: string
  created_at: string
  user_id: string | null
}

type StorableLineMediaType = 'image' | 'video' | 'audio' | 'file'

const JST_OFFSET_MS = 9 * 60 * 60 * 1000
const DEFAULT_DURATION_MIN = 60
const MAX_DURATION_MIN = 720
const CALENDAR_SCOPE = 'https://www.googleapis.com/auth/calendar'
const AI_MIN_CONFIDENCE = 0.82
const AI_CONFIRMATION_MIN_CONFIDENCE = 0.68
const AI_LIST_MIN_CONFIDENCE = 0.72
const AI_MESSAGE_SEARCH_MIN_CONFIDENCE = 0.72
const AI_AUTO_CREATE_MAX_EVENTS = 5
const PAST_EVENT_GRACE_MS = 5 * 60 * 1000
const PENDING_CONFIRMATION_TTL_MIN = 30
const CALENDAR_PENDING_TABLE = 'calendar_pending_confirmations'
const LEGACY_PENDING_PREFIX = '[[CAL_PENDING]]'
const LEGACY_PENDING_DONE_PREFIX = '[[CAL_PENDING_DONE]]'
const DEFAULT_MESSAGE_RETENTION_DAYS: MessageRetentionDays = 60
const SEARCH_MAX_FETCH_ROWS = 800
const SEARCH_MAX_SUMMARY_ROWS = 120
const SEARCH_MAX_PREVIEW_ROWS = 5
const SEARCH_AI_SUMMARY_MAX_HITS = 80
const LINE_MEDIA_BUCKET = 'line-media'
const LINE_MEDIA_ABSOLUTE_MAX_BYTES = 20 * 1024 * 1024
const LINE_MEDIA_TOTAL_CAP_BYTES = 500 * 1024 * 1024
const DEFAULT_MEDIA_UPLOAD_MAX_MB = 10
const MAX_MEDIA_UPLOAD_MAX_MB = 20
const STORABLE_LINE_MEDIA_TYPES = new Set<StorableLineMediaType>(['image', 'video', 'audio', 'file'])
const KEYWORD_SYNONYM_GROUPS = [
  ['ミーティング', 'meeting', 'mtg', '会議', '打ち合わせ', '打合せ', '商談'],
  ['試飲会', '試飲', 'テイスティング', 'tasting'],
  ['予定', 'スケジュール', 'schedule'],
  ['いくら', '幾ら', '値段', '価格', '金額', '料金', '費用', '円', 'yen'],
] as const

Deno.serve(async (req) => {
  if (req.method !== 'POST') {
    return new Response('Method not allowed', { status: 405 })
  }

  try {
    const rawBody = await req.text()
    const signature = req.headers.get('x-line-signature')
    const lineChannelSecret = Deno.env.get('LINE_CHANNEL_SECRET')

    // LINE Signature Verification
    // In production, if channel secret exists, signature must be present and valid.
    if (lineChannelSecret) {
      if (!signature) {
        console.error('Missing LINE signature header')
        return new Response('Forbidden', { status: 403 })
      }

      const encoder = new TextEncoder()
      const key = await crypto.subtle.importKey(
        'raw',
        encoder.encode(lineChannelSecret),
        { name: 'HMAC', hash: 'SHA-256' },
        false,
        ['sign']
      )
      const signatureBuffer = await crypto.subtle.sign(
        'HMAC',
        key,
        encoder.encode(rawBody)
      )
      // Convert buffer to Base64
      const hashArray = Array.from(new Uint8Array(signatureBuffer))
      const hashString = String.fromCharCode.apply(null, hashArray)
      const hashBase64 = btoa(hashString)

      if (hashBase64 !== signature) {
        console.error('Invalid LINE signature')
        return new Response('Forbidden', { status: 403 })
      }
    }

    const body = JSON.parse(rawBody)
    const events = body.events || []

    // Initialize Supabase Client
    const supabaseUrl = Deno.env.get('SUPABASE_URL') ?? ''
    const supabaseKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') ?? ''
    const supabase = createClient(supabaseUrl, supabaseKey)
    const lineAccessToken = Deno.env.get('LINE_CHANNEL_ACCESS_TOKEN') ?? ''
    const groqApiKey = Deno.env.get('GROQ_API_KEY') ?? ''
    const aiAutoCreateEnabled = parseBooleanEnv(Deno.env.get('CALENDAR_AI_AUTO_CREATE_ENABLED'))
    const calendarEnvState = loadCalendarEnv()
    const roomNameSyncDone = new Set<string>()
    const roomMessageSearchEnabledCache = new Map<string, boolean>()
    const messageRetentionDays = await loadMessageRetentionDays(supabase)
    const mediaUploadMaxBytes = await loadMediaUploadMaxBytes(supabase)

    for (const event of events) {
      if (event.type !== 'message') continue

      // Determine room/group ID or user ID as fallback
      const source = event.source || {}
      const roomId = String(source.groupId || source.roomId || source.userId || 'unknown')
      const userId = source.userId ? String(source.userId) : null
      const replyToken = String(event.replyToken ?? '')
      let aiAutoCreateReply: string | null = null

      if (!roomNameSyncDone.has(roomId)) {
        roomNameSyncDone.add(roomId)
        await syncRoomDisplayNameIfMissing(supabase, lineAccessToken, source, roomId)
      }

      if (event.message?.type === 'text') {
        const text = String(event.message.text ?? '').trim()
        if (calendarEnvState.ok) {
          const confirmationReply = await tryHandlePendingCalendarConfirmation(
            text,
            supabase,
            calendarEnvState.env,
            roomId,
            userId,
          )
          if (confirmationReply) {
            if (!lineAccessToken) {
              console.error('LINE_CHANNEL_ACCESS_TOKEN is missing. Cannot reply pending confirmation.')
              continue
            }
            if (!replyToken) {
              console.error('Missing replyToken for pending confirmation.')
              continue
            }
            const replyResult = await replyLineMessage(replyToken, confirmationReply, lineAccessToken)
            if (!replyResult.ok) {
              console.error('Failed to reply pending confirmation:', replyResult.error)
            }
            continue
          }
        }

        const messageSearchParse = parseMessageSearchCommand(text, messageRetentionDays)
        let messageSearchCommand: MessageSearchCommand | null = null
        let messageSearchError: string | null = null

        if (messageSearchParse.matched) {
          messageSearchCommand = messageSearchParse.command
          messageSearchError = messageSearchParse.error
        } else if (!!groqApiKey && looksLikeMessageSearchQuestion(text)) {
          const aiSearchIntent = await extractMessageSearchIntentWithGroq(text, messageRetentionDays, groqApiKey)
          if (aiSearchIntent && isAcceptableAiMessageSearchIntent(aiSearchIntent)) {
            messageSearchCommand = {
              kind: 'search_messages',
              keyword: aiSearchIntent.keyword,
              days: aiSearchIntent.days,
              scope: aiSearchIntent.scope,
            }
          }
        }

        if (messageSearchCommand || messageSearchError) {
          const messageSearchEnabled = await loadRoomMessageSearchEnabled(
            supabase,
            roomId,
            roomMessageSearchEnabledCache,
          )
          if (!messageSearchEnabled) {
            continue
          }

          const replyMessage = await buildMessageSearchReply(
            messageSearchCommand,
            messageSearchError,
            supabase,
            roomId,
            messageRetentionDays,
            groqApiKey,
          )

          if (!lineAccessToken) {
            console.error('LINE_CHANNEL_ACCESS_TOKEN is missing. Cannot reply message search.')
            continue
          }
          if (!replyToken) {
            console.error('Missing replyToken for message search.')
            continue
          }
          const replyResult = await replyLineMessage(replyToken, replyMessage, lineAccessToken)
          if (!replyResult.ok) {
            console.error('Failed to reply message search:', replyResult.error)
          }
          continue
        }

        const commandParse = parseCalendarCommand(text)
        if (commandParse.matched) {
          const replyMessage = await buildCalendarReplyMessage(
            commandParse,
            calendarEnvState,
            roomId,
            userId,
          )

          if (!lineAccessToken) {
            console.error('LINE_CHANNEL_ACCESS_TOKEN is missing. Cannot reply to command.')
            continue
          }
          if (!replyToken) {
            console.error('Missing replyToken for calendar command.')
            continue
          }

          const replyResult = await replyLineMessage(replyToken, replyMessage, lineAccessToken)
          if (!replyResult.ok) {
            console.error('Failed to reply calendar command:', replyResult.error)
          }
          continue
        }

        if (!commandParse.matched && calendarEnvState.ok && !!groqApiKey && looksLikeCalendarListQuestion(text)) {
          const aiListIntent = await extractCalendarListIntentWithGroq(
            text,
            calendarEnvState.env.timezone,
            groqApiKey,
          )
          if (aiListIntent && isAcceptableAiListIntent(aiListIntent)) {
            const aiCommand: Extract<CalendarCommand, { kind: 'list' }> = {
              kind: 'list',
              scope: aiListIntent.scope,
              ...(aiListIntent.date ? { date: aiListIntent.date } : {}),
              ...(typeof aiListIntent.month === 'number' ? { month: aiListIntent.month } : {}),
              ...(typeof aiListIntent.year === 'number' ? { year: aiListIntent.year } : {}),
              ...(aiListIntent.keyword ? { keyword: aiListIntent.keyword } : {}),
            }
            const replyMessage = await listCalendarEventsReply(aiCommand, calendarEnvState.env)

            if (!lineAccessToken) {
              console.error('LINE_CHANNEL_ACCESS_TOKEN is missing. Cannot reply AI list intent.')
              continue
            }
            if (!replyToken) {
              console.error('Missing replyToken for AI list intent.')
              continue
            }
            const replyResult = await replyLineMessage(replyToken, replyMessage, lineAccessToken)
            if (!replyResult.ok) {
              console.error('Failed to reply AI list intent:', replyResult.error)
            }
            continue
          }
        }

        if (aiAutoCreateEnabled && calendarEnvState.ok) {
          const ruleCommands = extractCalendarCommandsFromText(text)
          if (ruleCommands.length > 0) {
            aiAutoCreateReply = await autoCreateCalendarEventsFromCommands(
              ruleCommands,
              calendarEnvState.env,
              roomId,
              userId,
              'ルール抽出',
            )
          } else if (!!groqApiKey && looksLikeCalendarCandidate(text)) {
            const aiIntent = await extractCalendarIntentWithGroq(
              text,
              calendarEnvState.env.timezone,
              groqApiKey,
            )
            if (aiIntent && isHighConfidenceAiCalendarIntent(aiIntent)) {
              const reply = await createCalendarEventReply(
                {
                  kind: 'create',
                  date: aiIntent.date,
                  time: aiIntent.time,
                  durationMin: aiIntent.durationMin,
                  title: aiIntent.title,
                },
                calendarEnvState.env,
                roomId,
                userId,
              )
              aiAutoCreateReply = `AI判断で予定を自動登録しました（信頼度 ${Math.round(aiIntent.confidence * 100)}%）。\n${reply}`
            } else if (aiIntent && isConfirmableAiCalendarIntent(aiIntent)) {
              const pendingSaved = await savePendingCalendarConfirmation(
                supabase,
                roomId,
                userId,
                text,
                aiIntent,
              )
              if (pendingSaved) {
                aiAutoCreateReply = buildPendingCalendarConfirmationPrompt(aiIntent, calendarEnvState.env.timezone)
              } else {
                aiAutoCreateReply = '予定候補を解釈しましたが、確認待ちの保存に失敗しました。もう一度送ってください。'
              }
            }
          }
        }
      }

      // Parse content based on message type
      const content = toStoredMessageContent(event.message)
      // Save message to target database
      const { data: savedMessage, error } = await supabase
        .from('line_messages')
        .insert({
          room_id: roomId,
          user_id: userId,
          content,
          processed: false,
        })
        .select('id')
        .single()

      if (error) {
        console.error('Failed to insert message:', error)
      } else {
        console.log(`Saved message from ${roomId}: ${content.substring(0, 30)}...`)
        const savedMessageId = String(savedMessage?.id ?? '').trim()
        if (savedMessageId) {
          await trySaveLineMediaContent(
            supabase,
            lineAccessToken,
            event.message,
            savedMessageId,
            roomId,
            userId,
            mediaUploadMaxBytes,
          )
        }
      }

      if (aiAutoCreateReply) {
        if (!lineAccessToken) {
          console.error('LINE_CHANNEL_ACCESS_TOKEN is missing. Cannot reply AI auto-create result.')
          continue
        }
        if (!replyToken) {
          console.error('Missing replyToken for AI auto-create result.')
          continue
        }
        const replyResult = await replyLineMessage(replyToken, aiAutoCreateReply, lineAccessToken)
        if (!replyResult.ok) {
          console.error('Failed to reply AI auto-create result:', replyResult.error)
        }
      }
    }

    return new Response(JSON.stringify({ status: 'ok' }), {
      headers: { 'Content-Type': 'application/json' },
      status: 200,
    })

  } catch (err) {
    console.error('Error processing webhook:', err)
    return new Response('Internal Server Error', { status: 500 })
  }
})

function toStoredMessageContent(message: any): string {
  if (!message || typeof message !== 'object') {
    return '【不明なメッセージが送信されました】'
  }

  const mediaTag = buildLineMediaTag(message?.id)

  if (message.type === 'text') {
    return String(message.text ?? '')
  }
  if (message.type === 'image') {
    return `【画像が送信されました】${mediaTag}`
  }
  if (message.type === 'video') {
    return `【動画が送信されました】${mediaTag}`
  }
  if (message.type === 'file') {
    return `【ファイルが送信されました: ${message.fileName || '名称不明'}】${mediaTag}`
  }
  if (message.type === 'audio') {
    return `【ボイスメッセージが送信されました】${mediaTag}`
  }
  if (message.type === 'location') {
    return `【位置情報が送信されました: ${message.title || ''}】`
  }
  if (message.type === 'sticker') {
    return '【スタンプが送信されました】'
  }
  return `【その他のメディア (${message.type}) が送信されました】`
}

function buildLineMediaTag(lineMessageId: unknown): string {
  const id = String(lineMessageId ?? '').trim()
  if (!id) return ''
  return ` [[MEDIA:${id}]]`
}

async function trySaveLineMediaContent(
  supabase: ReturnType<typeof createClient>,
  lineAccessToken: string,
  message: any,
  lineMessageRowId: string,
  roomId: string,
  userId: string | null,
  mediaUploadMaxBytes: number,
): Promise<void> {
  const mediaType = normalizeStorableLineMediaType(message?.type)
  if (!mediaType) return

  const lineMessageId = String(message?.id ?? '').trim()
  if (!lineMessageId) {
    console.warn('Skip media save: LINE message ID is missing.')
    return
  }
  if (!lineAccessToken) {
    console.warn(`Skip media save: LINE_CHANNEL_ACCESS_TOKEN is missing (type=${mediaType}, lineMessageId=${lineMessageId}).`)
    return
  }

  const { data: existing, error: existingError } = await supabase
    .from('line_message_media')
    .select('id')
    .eq('line_message_id', lineMessageId)
    .maybeSingle()

  if (existingError) {
    console.error('Failed to inspect existing media metadata:', existingError)
    return
  }
  if (existing?.id != null) {
    return
  }

  const contentFetch = await fetchLineMessageBinary(lineMessageId, lineAccessToken, mediaUploadMaxBytes)
  if (!contentFetch.ok) {
    console.error(`Failed to fetch media content from LINE (lineMessageId=${lineMessageId}):`, contentFetch.error)
    return
  }

  const fileSizeBytes = contentFetch.bytes.byteLength
  if (fileSizeBytes <= 0) {
    console.warn(`Skip media save: empty payload (lineMessageId=${lineMessageId}).`)
    return
  }
  if (fileSizeBytes >= mediaUploadMaxBytes) {
    console.warn(
      `Skip media save: payload too large (${fileSizeBytes} bytes, limit(<)=${mediaUploadMaxBytes}, lineMessageId=${lineMessageId}).`,
    )
    return
  }

  const usageBefore = await loadLineMediaUsageTotals(supabase)
  if (!usageBefore.ok) {
    console.error(`Skip media save: failed to inspect total media usage (lineMessageId=${lineMessageId}): ${usageBefore.error}`)
    return
  }
  if (usageBefore.totalBytes + fileSizeBytes > LINE_MEDIA_TOTAL_CAP_BYTES) {
    console.warn(
      `Skip media save: total media cap exceeded (${usageBefore.totalBytes} + ${fileSizeBytes} > ${LINE_MEDIA_TOTAL_CAP_BYTES}, lineMessageId=${lineMessageId}).`,
    )
    return
  }

  const extension = resolveMediaExtension(mediaType, contentFetch.contentType, String(message?.fileName ?? ''))
  const originalFileName = resolveOriginalFileName(
    mediaType,
    String(message?.fileName ?? ''),
    lineMessageId,
    extension,
  )
  const storagePath = buildMediaStoragePath(roomId, mediaType, lineMessageId, extension)
  const uploadResult = await supabase
    .storage
    .from(LINE_MEDIA_BUCKET)
    .upload(storagePath, contentFetch.bytes, {
      contentType: contentFetch.contentType || undefined,
      upsert: false,
    })

  if (uploadResult.error) {
    console.error(`Failed to upload media to storage (lineMessageId=${lineMessageId}):`, uploadResult.error)
    return
  }

  const { error: insertError } = await supabase.from('line_message_media').insert({
    message_id: lineMessageRowId,
    line_message_id: lineMessageId,
    room_id: roomId,
    user_id: userId,
    media_type: mediaType,
    storage_bucket: LINE_MEDIA_BUCKET,
    storage_path: storagePath,
    original_file_name: originalFileName,
    mime_type: contentFetch.contentType || null,
    file_size_bytes: fileSizeBytes,
  })

  if (insertError) {
    const code = String((insertError as any)?.code ?? '')
    if (code === '23505') return
    console.error(`Failed to insert media metadata (lineMessageId=${lineMessageId}):`, insertError)
    return
  }

  const usageAfter = await loadLineMediaUsageTotals(supabase)
  if (usageAfter.ok && usageAfter.totalBytes > LINE_MEDIA_TOTAL_CAP_BYTES) {
    console.warn(
      `Rollback media save: total media usage exceeded cap after insert (${usageAfter.totalBytes} > ${LINE_MEDIA_TOTAL_CAP_BYTES}, lineMessageId=${lineMessageId}).`,
    )
    const removeFileRes = await supabase.storage.from(LINE_MEDIA_BUCKET).remove([storagePath])
    if (removeFileRes.error) {
      console.error(`Failed to rollback storage file for lineMessageId=${lineMessageId}:`, removeFileRes.error)
    }
    const rollbackMetaRes = await supabase
      .from('line_message_media')
      .delete()
      .eq('line_message_id', lineMessageId)
    if (rollbackMetaRes.error) {
      console.error(`Failed to rollback media metadata for lineMessageId=${lineMessageId}:`, rollbackMetaRes.error)
    }
    return
  }

  console.log(`Saved media content (${mediaType}) for room=${roomId}, lineMessageId=${lineMessageId}`)
}

async function loadLineMediaUsageTotals(
  supabase: ReturnType<typeof createClient>,
): Promise<{ ok: true; totalBytes: number; totalFiles: number } | { ok: false; error: string }> {
  try {
    const { data, error } = await supabase.rpc('get_line_media_usage_stats', {
      filter_room_id: null,
      filter_media_type: null,
    })
    if (error) {
      return { ok: false, error: error.message }
    }
    const row = Array.isArray(data) ? data[0] : null
    const totalBytes = toNonNegativeInt((row as any)?.total_bytes)
    const totalFiles = toNonNegativeInt((row as any)?.total_files)
    return { ok: true, totalBytes, totalFiles }
  } catch (err) {
    return {
      ok: false,
      error: err instanceof Error ? err.message : String(err),
    }
  }
}

async function loadMediaUploadMaxBytes(
  supabase: ReturnType<typeof createClient>,
): Promise<number> {
  try {
    const { data, error } = await supabase
      .from('summary_settings')
      .select('media_upload_max_mb')
      .eq('id', 1)
      .maybeSingle()
    if (error) {
      console.error('Failed to load media_upload_max_mb:', error.message)
      return DEFAULT_MEDIA_UPLOAD_MAX_MB * 1024 * 1024
    }
    const maxMb = normalizeMediaUploadMaxMb(data?.media_upload_max_mb)
    return maxMb * 1024 * 1024
  } catch (error) {
    console.error('Unexpected error while loading media_upload_max_mb:', error)
    return DEFAULT_MEDIA_UPLOAD_MAX_MB * 1024 * 1024
  }
}

function normalizeMediaUploadMaxMb(value: unknown): number {
  const mb = Number(value)
  if (!Number.isInteger(mb)) return DEFAULT_MEDIA_UPLOAD_MAX_MB
  if (mb < 1) return 1
  if (mb > MAX_MEDIA_UPLOAD_MAX_MB) return MAX_MEDIA_UPLOAD_MAX_MB
  return mb
}

function toNonNegativeInt(value: unknown): number {
  const n = Number(value)
  if (!Number.isFinite(n) || n < 0) return 0
  return Math.floor(n)
}

function normalizeStorableLineMediaType(value: unknown): StorableLineMediaType | null {
  const normalized = String(value ?? '').trim().toLowerCase()
  if (!normalized) return null
  if (!STORABLE_LINE_MEDIA_TYPES.has(normalized as StorableLineMediaType)) return null
  return normalized as StorableLineMediaType
}

async function fetchLineMessageBinary(
  lineMessageId: string,
  lineAccessToken: string,
  mediaUploadMaxBytes: number,
): Promise<{ ok: true; bytes: Uint8Array; contentType: string } | { ok: false; error: string }> {
  try {
    const response = await fetch(
      `https://api-data.line.me/v2/bot/message/${encodeURIComponent(lineMessageId)}/content`,
      {
        method: 'GET',
        headers: {
          'Authorization': `Bearer ${lineAccessToken}`,
        },
      },
    )

    if (!response.ok) {
      const errorText = await response.text()
      return { ok: false, error: `LINE content API ${response.status}: ${errorText}` }
    }

    const lengthHeader = Number(response.headers.get('content-length'))
    if (Number.isFinite(lengthHeader) && lengthHeader > LINE_MEDIA_ABSOLUTE_MAX_BYTES) {
      return {
        ok: false,
        error: `content too large: ${lengthHeader} bytes (absolute limit ${LINE_MEDIA_ABSOLUTE_MAX_BYTES} bytes)`,
      }
    }

    if (Number.isFinite(lengthHeader) && lengthHeader >= mediaUploadMaxBytes) {
      return {
        ok: false,
        error: `content too large: ${lengthHeader} bytes (limit is under ${mediaUploadMaxBytes} bytes)`,
      }
    }

    const contentTypeRaw = String(response.headers.get('content-type') ?? '')
    const contentType = contentTypeRaw.split(';')[0].trim().toLowerCase()
    const arrayBuffer = await response.arrayBuffer()
    return { ok: true, bytes: new Uint8Array(arrayBuffer), contentType }
  } catch (err) {
    return {
      ok: false,
      error: err instanceof Error ? err.message : String(err),
    }
  }
}

function resolveMediaExtension(
  mediaType: StorableLineMediaType,
  contentType: string,
  rawFileName: string,
): string {
  const fromName = extractExtension(rawFileName)
  if (fromName) return fromName

  const normalizedContentType = String(contentType ?? '').toLowerCase()
  if (normalizedContentType.includes('jpeg') || normalizedContentType.includes('jpg')) return 'jpg'
  if (normalizedContentType.includes('png')) return 'png'
  if (normalizedContentType.includes('gif')) return 'gif'
  if (normalizedContentType.includes('webp')) return 'webp'
  if (normalizedContentType.includes('mp4')) return 'mp4'
  if (normalizedContentType.includes('quicktime')) return 'mov'
  if (normalizedContentType.includes('mpeg')) return 'mp3'
  if (normalizedContentType.includes('wav')) return 'wav'
  if (normalizedContentType.includes('aac')) return 'aac'
  if (normalizedContentType.includes('pdf')) return 'pdf'
  if (normalizedContentType.includes('zip')) return 'zip'
  if (normalizedContentType.includes('json')) return 'json'
  if (normalizedContentType.includes('csv')) return 'csv'
  if (normalizedContentType.includes('plain')) return 'txt'

  if (mediaType === 'image') return 'jpg'
  if (mediaType === 'video') return 'mp4'
  if (mediaType === 'audio') return 'm4a'
  return 'bin'
}

function resolveOriginalFileName(
  mediaType: StorableLineMediaType,
  rawFileName: string,
  lineMessageId: string,
  extension: string,
): string {
  const cleaned = sanitizeFileName(rawFileName)
  if (cleaned) return cleaned
  const base = mediaType === 'image'
    ? 'image'
    : mediaType === 'video'
      ? 'video'
      : mediaType === 'audio'
        ? 'audio'
        : 'file'
  return `${base}-${lineMessageId}.${extension}`
}

function buildMediaStoragePath(
  roomId: string,
  mediaType: StorableLineMediaType,
  lineMessageId: string,
  extension: string,
): string {
  const now = new Date()
  const y = now.getUTCFullYear()
  const m = String(now.getUTCMonth() + 1).padStart(2, '0')
  const d = String(now.getUTCDate()).padStart(2, '0')
  const roomKey = sanitizeStoragePathSegment(roomId || 'unknown-room')
  const ext = extension.replace(/[^a-zA-Z0-9]/g, '').toLowerCase() || 'bin'
  return `${y}/${m}/${d}/${roomKey}/${mediaType}/${lineMessageId}.${ext}`
}

function sanitizeStoragePathSegment(value: string): string {
  const cleaned = String(value ?? '')
    .trim()
    .replace(/[^a-zA-Z0-9._-]/g, '_')
  if (!cleaned) return 'unknown'
  return cleaned.length > 120 ? cleaned.slice(0, 120) : cleaned
}

function sanitizeFileName(value: string): string {
  const normalized = String(value ?? '').trim()
  if (!normalized) return ''
  const safe = normalized
    .replace(/[\/\\?%*:|"<>]/g, '_')
    .replace(/\s+/g, ' ')
    .trim()
  return safe.length > 180 ? safe.slice(0, 180) : safe
}

function extractExtension(fileName: string): string {
  const normalized = sanitizeFileName(fileName)
  if (!normalized) return ''
  const idx = normalized.lastIndexOf('.')
  if (idx < 0 || idx === normalized.length - 1) return ''
  const ext = normalized.slice(idx + 1).toLowerCase().replace(/[^a-z0-9]/g, '')
  if (!ext) return ''
  return ext.length > 12 ? ext.slice(0, 12) : ext
}

function parseBooleanEnv(value: string | null): boolean {
  const v = String(value ?? '').trim().toLowerCase()
  return v === '1' || v === 'true' || v === 'yes' || v === 'on'
}

async function syncRoomDisplayNameIfMissing(
  supabase: ReturnType<typeof createClient>,
  lineAccessToken: string,
  source: any,
  roomId: string,
): Promise<void> {
  const normalizedRoomId = String(roomId ?? '').trim()
  if (!normalizedRoomId || normalizedRoomId === 'unknown') return

  const sourceType = String(source?.type ?? '').trim().toLowerCase()
  if (!sourceType || (sourceType !== 'group' && sourceType !== 'room' && sourceType !== 'user')) {
    return
  }

  const { data: existing, error: existingError } = await supabase
    .from('room_summary_settings')
    .select('room_id, room_name')
    .eq('room_id', normalizedRoomId)
    .maybeSingle()

  if (existingError) {
    console.error(`Failed to inspect room_summary_settings for ${normalizedRoomId}:`, existingError.message)
    return
  }

  const existingName = String(existing?.room_name ?? '').trim()
  if (existingName && existingName !== normalizedRoomId) return
  if (!lineAccessToken) return

  const fetchedName = await fetchLineConversationDisplayName(source, lineAccessToken)
  if (!fetchedName) return

  const updatedAt = new Date().toISOString()
  if (existing?.room_id) {
    const { error: updateError } = await supabase
      .from('room_summary_settings')
      .update({
        room_name: fetchedName,
        updated_at: updatedAt,
      })
      .eq('room_id', normalizedRoomId)

    if (updateError) {
      console.error(`Failed to update room_name for ${normalizedRoomId}:`, updateError.message)
      return
    }
    console.log(`Auto-set room_name for ${normalizedRoomId}: ${fetchedName}`)
    return
  }

  const { error: insertError } = await supabase
    .from('room_summary_settings')
    .insert({
      room_id: normalizedRoomId,
      room_name: fetchedName,
      updated_at: updatedAt,
    })

  if (insertError) {
    const code = String((insertError as any)?.code ?? '')
    if (code !== '23505') {
      console.error(`Failed to insert room setting for ${normalizedRoomId}:`, insertError.message)
    }
    return
  }

  console.log(`Auto-created room setting for ${normalizedRoomId}: ${fetchedName}`)
}

async function fetchLineConversationDisplayName(source: any, lineAccessToken: string): Promise<string | null> {
  const sourceType = String(source?.type ?? '').trim().toLowerCase()
  if (sourceType === 'group') {
    const groupId = String(source?.groupId ?? '').trim()
    if (!groupId) return null
    const summary = await fetchLineJson(
      `https://api.line.me/v2/bot/group/${encodeURIComponent(groupId)}/summary`,
      lineAccessToken,
    )
    return normalizeDisplayName(summary?.groupName)
  }

  if (sourceType === 'room') {
    const roomId = String(source?.roomId ?? '').trim()
    if (!roomId) return null
    const summary = await fetchLineJson(
      `https://api.line.me/v2/bot/room/${encodeURIComponent(roomId)}/summary`,
      lineAccessToken,
    )
    return normalizeDisplayName(summary?.roomName)
  }

  if (sourceType === 'user') {
    const userId = String(source?.userId ?? '').trim()
    if (!userId) return null
    const profile = await fetchLineJson(
      `https://api.line.me/v2/bot/profile/${encodeURIComponent(userId)}`,
      lineAccessToken,
    )
    return normalizeDisplayName(profile?.displayName)
  }

  return null
}

async function fetchLineJson(url: string, lineAccessToken: string): Promise<any | null> {
  try {
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${lineAccessToken}`,
      },
    })
    if (!response.ok) {
      const errorText = await response.text()
      console.warn(`LINE API request failed (${response.status}) ${url}: ${errorText}`)
      return null
    }
    return await response.json()
  } catch (error) {
    console.error(`LINE API request error for ${url}:`, error)
    return null
  }
}

function normalizeDisplayName(value: unknown): string | null {
  const normalized = String(value ?? '').replace(/\u3000/g, ' ').replace(/\s+/g, ' ').trim()
  return normalized || null
}

async function loadMessageRetentionDays(
  supabase: ReturnType<typeof createClient>,
): Promise<MessageRetentionDays> {
  try {
    const { data, error } = await supabase
      .from('summary_settings')
      .select('message_retention_days')
      .eq('id', 1)
      .maybeSingle()
    if (error) {
      console.error('Failed to load message_retention_days:', error.message)
      return DEFAULT_MESSAGE_RETENTION_DAYS
    }
    return normalizeMessageRetentionDays(data?.message_retention_days)
  } catch (err) {
    console.error('Unexpected error while loading message_retention_days:', err)
    return DEFAULT_MESSAGE_RETENTION_DAYS
  }
}

function normalizeMessageRetentionDays(value: unknown): MessageRetentionDays {
  const days = Number(value)
  if (days === 60 || days === 120 || days === 180) return days
  return DEFAULT_MESSAGE_RETENTION_DAYS
}

async function loadRoomMessageSearchEnabled(
  supabase: ReturnType<typeof createClient>,
  roomId: string,
  cache: Map<string, boolean>,
): Promise<boolean> {
  const normalizedRoomId = String(roomId ?? '').trim()
  if (!normalizedRoomId || normalizedRoomId === 'unknown') return true
  if (cache.has(normalizedRoomId)) return cache.get(normalizedRoomId) !== false

  try {
    const { data, error } = await supabase
      .from('room_summary_settings')
      .select('message_search_enabled')
      .eq('room_id', normalizedRoomId)
      .maybeSingle()

    if (error) {
      console.error(`Failed to load message_search_enabled for ${normalizedRoomId}:`, error.message)
      cache.set(normalizedRoomId, true)
      return true
    }

    const enabled = data?.message_search_enabled !== false
    cache.set(normalizedRoomId, enabled)
    return enabled
  } catch (err) {
    console.error(`Unexpected error while loading message_search_enabled for ${normalizedRoomId}:`, err)
    cache.set(normalizedRoomId, true)
    return true
  }
}

function parseMessageSearchCommand(rawText: string, defaultDays: MessageRetentionDays): MessageSearchParseResult {
  const text = normalizeSpaces(rawText)
  if (!text) return { matched: false, command: null, error: null }

  const compact = normalizeForRuleParsing(text).replace(/\s+/g, '')
  const hasExplicitPrefix = /^(会話|トーク|履歴|チャット)(検索|要約|確認)/.test(compact)
  const hasConversationHint = /(会話|トーク|履歴|チャット|メッセージ|発言|ルーム|グループ|他ルーム|他のルーム|別ルーム|全ルーム)/.test(compact)
  const hasQueryIntent = /(検索|探し|探して|探す|要約|教えて|見せて|みせて|確認|表示|表示して|出して|だして|知りたい)/.test(compact)
  if (!hasExplicitPrefix && !(hasConversationHint && hasQueryIntent)) {
    return { matched: false, command: null, error: null }
  }

  const requestedDays = detectMessageSearchDays(compact) ?? defaultDays
  const scope = detectMessageSearchScope(compact)
  const keyword = extractMessageSearchKeyword(text)
  if (!keyword) {
    return {
      matched: true,
      command: null,
      error: [
        '会話検索のキーワードを指定してください。',
        '例: 会話検索 試飲会',
        '例: 会話検索 120日 発注',
      ].join('\n'),
    }
  }

  return {
    matched: true,
    command: {
      kind: 'search_messages',
      keyword,
      days: requestedDays,
      scope,
    },
    error: null,
  }
}

function detectMessageSearchDays(compactText: string): MessageRetentionDays | null {
  if (/(180日|半年|6ヶ月|6か月|六ヶ月)/.test(compactText)) return 180
  if (/(120日|4ヶ月|4か月|四ヶ月)/.test(compactText)) return 120
  if (/(60日|2ヶ月|2か月|二ヶ月)/.test(compactText)) return 60
  return null
}

function detectMessageSearchScope(compactText: string): MessageSearchScope {
  if (/(全ルーム|他ルーム|他のルーム|別ルーム|別のルーム|全グループ|他グループ|別グループ|別のグループ)/.test(compactText)) {
    return 'all_rooms'
  }
  return 'current_room'
}

function extractMessageSearchKeyword(rawText: string): string {
  const stripped = normalizeForRuleParsing(rawText)
    .replace(/(180日|120日|60日|半年|6ヶ月|6か月|六ヶ月|4ヶ月|4か月|四ヶ月|2ヶ月|2か月|二ヶ月)/g, ' ')
    .replace(/(過去|最近|直近|以内|分|間)/g, ' ')
    .replace(/(会話|トーク|履歴|チャット|メッセージ|発言|ルーム|グループ|全ルーム|他ルーム|他のルーム|別ルーム|別のルーム)/g, ' ')
    .replace(/(検索|探し|探して|探す|要約|まとめ|教えて|見せて|みせて|確認|表示|表示して|出して|だして|知りたい|記述|言及)/g, ' ')
    .replace(/(ありますか|あるか|あります|ある|でしたか|ですか|ますか|でしょうか|だったっけ|だっけ|っけ|かな|です|ます)/g, ' ')
    .replace(/(を|は|が|に|で|の|から|だけ|について|して|ください|下さい|お願いします|お願い|とか|って|こと|もの|やつ)/g, ' ')
    .replace(/[?？!！。．、,]/g, ' ')
  return normalizeKeywordForFilter(stripped)
}

function looksLikeMessageSearchQuestion(text: string): boolean {
  const compact = normalizeForRuleParsing(text).replace(/\s+/g, '')
  if (!compact) return false
  if (/^予定(?:確認|一覧|報告|登録|追加)/.test(compact)) return false

  const hasSearchIntent = /(検索|探し|探して|探す|教えて|表示|表示して|見せて|みせて|確認|知りたい|ありますか|あるか|あります|ある|記述|言及|話してた|言ってた)/.test(compact)
  if (!hasSearchIntent) return false

  const hasConversationHint = /(会話|トーク|履歴|チャット|メッセージ|発言|ルーム|グループ|他ルーム|他のルーム|別ルーム|別のルーム|全ルーム)/.test(compact)
  return hasConversationHint
}

async function extractMessageSearchIntentWithGroq(
  text: string,
  defaultDays: MessageRetentionDays,
  groqApiKey: string,
): Promise<AiMessageSearchIntent | null> {
  try {
    const response = await fetch('https://api.groq.com/openai/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${groqApiKey}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        model: 'llama-3.3-70b-versatile',
        temperature: 0,
        messages: [
          {
            role: 'system',
            content: [
              'あなたはLINE会話検索コマンド抽出用のJSON抽出器です。',
              '会話・履歴・メッセージ検索の意図がある場合のみ should_search=true にしてください。',
              '予定照会（予定・会議などのカレンダー検索）は should_search=false にしてください。',
              'JSONのみ返してください。説明文やコードブロックは禁止です。',
              `days は 60/120/180 のいずれか。未指定時は ${defaultDays}。`,
              'scope は current_room または all_rooms。',
              '「他のルーム」「全ルーム」「別グループ」等の意図がある場合は all_rooms。',
              'keyword は検索に使う短い語句のみ。',
              '返却JSONスキーマ:',
              '{"should_search":boolean,"keyword":string,"days":60|120|180,"scope":"current_room|all_rooms","confidence":number(0-1)}',
            ].join('\n'),
          },
          { role: 'user', content: text },
        ],
      }),
    })

    if (!response.ok) {
      const err = await response.text()
      console.error('Groq message-search extraction failed:', response.status, err)
      return null
    }

    const json = await response.json()
    const content = String(json?.choices?.[0]?.message?.content ?? '').trim()
    if (!content) return null

    const extracted = parseFirstJsonObject(content)
    if (!extracted || typeof extracted !== 'object') return null

    const raw = extracted as Record<string, unknown>
    const shouldSearch = Boolean(raw.should_search ?? raw.shouldSearch ?? false)
    const keyword = normalizeKeywordForFilter(String(raw.keyword ?? ''))
    const confidenceNum = Number(raw.confidence ?? 0)
    const confidence = Number.isFinite(confidenceNum)
      ? Math.max(0, Math.min(1, confidenceNum))
      : 0

    const rawDays = Number(raw.days)
    const days = rawDays === 60 || rawDays === 120 || rawDays === 180
      ? rawDays
      : defaultDays

    const scope = normalizeAiMessageSearchScope(String(raw.scope ?? ''))

    return {
      shouldSearch,
      keyword,
      days,
      scope,
      confidence,
    }
  } catch (err) {
    console.error('Failed to extract message search intent with Groq:', err)
    return null
  }
}

function normalizeAiMessageSearchScope(raw: string): MessageSearchScope {
  const value = String(raw ?? '').trim().toLowerCase()
  if (value === 'all_rooms' || value === 'all' || value === 'global' || value === 'cross_room') {
    return 'all_rooms'
  }
  return 'current_room'
}

function isAcceptableAiMessageSearchIntent(intent: AiMessageSearchIntent): boolean {
  if (!intent.shouldSearch) return false
  if (intent.confidence < AI_MESSAGE_SEARCH_MIN_CONFIDENCE) return false
  if (!intent.keyword) return false
  return true
}

async function buildMessageSearchReply(
  command: MessageSearchCommand | null,
  parseError: string | null,
  supabase: ReturnType<typeof createClient>,
  roomId: string,
  configuredRetentionDays: MessageRetentionDays,
  groqApiKey: string,
): Promise<string> {
  if (parseError) return parseError
  if (!command) return '会話検索の意図を解釈できませんでした。'

  const effectiveDays = command.days > configuredRetentionDays ? configuredRetentionDays : command.days
  const sinceIso = new Date(Date.now() - effectiveDays * 24 * 60 * 60 * 1000).toISOString()

  let query = supabase
    .from('line_messages')
    .select('room_id, content, created_at, user_id')
    .gte('created_at', sinceIso)
    .order('created_at', { ascending: false })
    .limit(SEARCH_MAX_FETCH_ROWS)

  if (command.scope !== 'all_rooms') {
    query = query.eq('room_id', roomId)
  }

  const { data, error } = await query

  if (error) {
    return `会話検索に失敗しました。${error.message}`
  }

  const rows: SearchMessageRow[] = Array.isArray(data)
    ? data.map((row: any) => ({
        room_id: String(row?.room_id ?? ''),
        content: String(row?.content ?? ''),
        created_at: String(row?.created_at ?? ''),
        user_id: row?.user_id == null ? null : String(row.user_id),
      }))
    : []

  const hitsRaw = rows.filter((row) => messageMatchesKeyword(row.content, command.keyword))
  if (hitsRaw.length === 0) {
    const lines = [`「${command.keyword}」に一致する会話はありません（過去${effectiveDays}日）`]
    if (effectiveDays !== command.days) {
      lines.push(`※保持期間設定が${configuredRetentionDays}日のため、検索範囲を調整しました。`)
    }
    return lines.join('\n')
  }

  const roomLabels = command.scope === 'all_rooms'
    ? await loadRoomLabelsForHits(supabase, hitsRaw)
    : new Map<string, string>()
  const hits = hitsRaw.map((row) => ({
    ...row,
    room_label: command.scope === 'all_rooms'
      ? (roomLabels.get(row.room_id) ?? row.room_id)
      : null,
  }))

  const shouldSummarize = !!groqApiKey && hits.length <= SEARCH_AI_SUMMARY_MAX_HITS
  const summary = await summarizeMessageSearchHitsWithGroq(
    shouldSummarize ? hits.slice(0, SEARCH_MAX_SUMMARY_ROWS) : [],
    command.keyword,
    effectiveDays,
    groqApiKey,
  )

  const previewRows = hits.slice(0, SEARCH_MAX_PREVIEW_ROWS)
  const scopeLabel = command.scope === 'all_rooms' ? '全ルーム横断' : 'このルーム'
  const lines: string[] = [
    `会話検索結果（${scopeLabel} / 過去${effectiveDays}日 / キーワード: ${command.keyword}）`,
    `一致: ${hits.length}件`,
  ]
  if (effectiveDays !== command.days) {
    lines.push(`※保持期間設定が${configuredRetentionDays}日のため、検索範囲を調整しました。`)
  }
  if (rows.length >= SEARCH_MAX_FETCH_ROWS) {
    lines.push(`※検索対象が多いため、新しい順で先頭${SEARCH_MAX_FETCH_ROWS}件を対象にしています。`)
  }
  if (summary) {
    lines.push('')
    lines.push('要約:')
    lines.push(summary)
  } else if (!!groqApiKey && hits.length > SEARCH_AI_SUMMARY_MAX_HITS) {
    lines.push(`※一致件数が多いため、AI要約は省略しています（${SEARCH_AI_SUMMARY_MAX_HITS}件超）。`)
  }
  lines.push('')
  lines.push('一致メッセージ（新しい順）:')
  for (let i = 0; i < previewRows.length; i += 1) {
    lines.push(`${i + 1}. ${formatMessageSearchPreview(previewRows[i])}`)
  }
  if (hits.length > previewRows.length) {
    lines.push(`…ほか ${hits.length - previewRows.length}件`)
  }
  return lines.join('\n')
}

async function loadRoomLabelsForHits(
  supabase: ReturnType<typeof createClient>,
  rows: SearchMessageRow[],
): Promise<Map<string, string>> {
  const roomIds = Array.from(new Set(rows.map((row) => String(row.room_id ?? '').trim()).filter((id) => id.length > 0)))
  if (roomIds.length === 0) return new Map<string, string>()

  const { data, error } = await supabase
    .from('room_summary_settings')
    .select('room_id, room_name')
    .in('room_id', roomIds)

  if (error) {
    console.error('Failed to load room labels for message search:', error.message)
    return new Map<string, string>()
  }

  const map = new Map<string, string>()
  for (const row of Array.isArray(data) ? data : []) {
    const id = String((row as any)?.room_id ?? '').trim()
    if (!id) continue
    const name = normalizeInlineText(String((row as any)?.room_name ?? ''))
    map.set(id, name || id)
  }
  return map
}

function formatMessageSearchPreview(row: SearchMessageRow): string {
  const date = formatSearchDateTime(row.created_at)
  const content = normalizeInlineText(String(row.content ?? ''))
  const compact = content.length > 90 ? `${content.slice(0, 90)}...` : (content || '（内容なし）')
  const roomPrefix = row.room_label ? `[${row.room_label}] ` : ''
  return `${roomPrefix}${date} ${compact}`
}

function formatSearchDateTime(iso: string): string {
  const date = new Date(iso)
  if (Number.isNaN(date.getTime())) return '(時刻不明)'
  return new Intl.DateTimeFormat('ja-JP', {
    timeZone: 'Asia/Tokyo',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).format(date)
}

async function summarizeMessageSearchHitsWithGroq(
  rows: SearchMessageRow[],
  keyword: string,
  days: number,
  groqApiKey: string,
): Promise<string | null> {
  if (!groqApiKey || rows.length === 0) return null

  try {
    const chronologicalRows = [...rows].reverse()
    const transcript = chronologicalRows
      .map((row, index) => {
        const content = normalizeInlineText(String(row.content ?? '')).replace(/\s+/g, ' ')
        const short = content.length > 180 ? `${content.slice(0, 180)}...` : content
        const room = row.room_label ? ` [${row.room_label}]` : ''
        return `[${index + 1}] ${formatSearchDateTime(row.created_at)}${room} ${short}`
      })
      .join('\n')

    const response = await fetch('https://api.groq.com/openai/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${groqApiKey}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        model: 'llama-3.3-70b-versatile',
        temperature: 0.2,
        messages: [
          {
            role: 'system',
            content: [
              'あなたはLINE会話検索結果の要約アシスタントです。',
              '入力はキーワード一致した発言のみです。',
              '日本語で、3〜5行で簡潔に要点を要約してください。',
              '日時や依頼事項、結論があれば必ず含めてください。',
            ].join('\n'),
          },
          {
            role: 'user',
            content: [
              `検索キーワード: ${keyword}`,
              `検索範囲: 過去${days}日`,
              '以下を要約してください:',
              transcript,
            ].join('\n\n'),
          },
        ],
      }),
    })

    if (!response.ok) {
      const err = await response.text()
      console.error('Groq message-search summary failed:', response.status, err)
      return null
    }

    const json = await response.json()
    const content = normalizeInlineText(String(json?.choices?.[0]?.message?.content ?? ''))
    if (!content) return null
    return content.length > 900 ? `${content.slice(0, 900)}...` : content
  } catch (err) {
    console.error('Failed to summarize message search hits with Groq:', err)
    return null
  }
}

function messageMatchesKeyword(content: string, keyword: string): boolean {
  return keywordMatchesHaystacks(keyword, [String(content ?? '')])
}

function looksLikeCalendarCandidate(text: string): boolean {
  const normalized = normalizeForRuleParsing(text).toLowerCase()
  const hasDateHint = /(\d{4}[\/.\-]\d{1,2}[\/.\-]\d{1,2}|\d{4}年\d{1,2}月\d{1,2}日|\d{1,2}[\/.\-]\d{1,2}|\d{1,2}日|今日|明日|明後日|来週|今週)/.test(normalized)
  const hasTimeHint = /(\d{1,2}:\d{2}|\d{1,2}時(\d{1,2}分)?)/.test(normalized)
  const hasIntentWord = /(予定|会議|打ち合わせ|ミーティング|mtg|予約|アポ|面談|訪問|来店|ランチ|ディナー)/.test(normalized)
  return (hasDateHint && hasTimeHint) || (hasIntentWord && (hasDateHint || hasTimeHint))
}

function looksLikeCalendarListQuestion(text: string): boolean {
  const compact = normalizeForRuleParsing(text).replace(/\s+/g, '')
  if (!compact) return false

  const hasQuestionIntent = /(いつ|何件|ありますか|あります|ある|教えて|見せて|みせて|知りたい|一覧|確認|どれ|どこ|空き|空いて|表示|表示して|出して|だして|見たい|確認したい)/.test(compact)
  if (!hasQuestionIntent) return false

  const hasCalendarHint =
    /(\d{4}[\/.\-]\d{1,2}|\d{4}年\d{1,2}月|\d{1,2}月|今日|明日|今週|来週|今月|来月|今後|これから|予定|会議|打ち合わせ|打合せ|ミーティング|mtg|meeting|予約|アポ|面談|イベント)/.test(compact)
  return hasCalendarHint
}

async function extractCalendarListIntentWithGroq(
  text: string,
  timezone: string,
  groqApiKey: string,
): Promise<AiListIntent | null> {
  try {
    const now = new Date()
    const nowText = new Intl.DateTimeFormat('ja-JP', {
      timeZone: timezone,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
    }).format(now)

    const response = await fetch('https://api.groq.com/openai/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${groqApiKey}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        model: 'llama-3.3-70b-versatile',
        temperature: 0,
        messages: [
          {
            role: 'system',
            content: [
              'あなたはカレンダー予定検索用のJSON抽出器です。',
              `現在時刻は ${nowText} (${timezone})。`,
              '予定登録ではなく、予定照会（検索）として解釈できるときだけ高い confidence を返してください。',
              'JSONのみ返してください。説明文やコードブロックは禁止です。',
              'scope は次のいずれか: today, tomorrow, week, next_week, date, month, next_month, year_month, year, upcoming_30d',
              'date は YYYY-MM-DD、month は 1-12、year は西暦4桁。',
              'keyword は任意。名詞句のみ（例: ミーティング）。不要なら空文字。',
              '範囲指定が曖昧な照会は upcoming_30d を使ってください。',
              '返却JSONスキーマ:',
              '{"scope":string,"date":"YYYY-MM-DD|optional","month":number|optional,"year":number|optional,"keyword":string|optional,"confidence":number(0-1)}',
            ].join('\n'),
          },
          { role: 'user', content: text },
        ],
      }),
    })

    if (!response.ok) {
      const err = await response.text()
      console.error('Groq list extraction failed:', response.status, err)
      return null
    }

    const json = await response.json()
    const content = String(json?.choices?.[0]?.message?.content ?? '').trim()
    if (!content) return null

    const extracted = parseFirstJsonObject(content)
    if (!extracted || typeof extracted !== 'object') return null

    const raw = extracted as Record<string, unknown>
    const scopeRaw = String(raw.scope ?? '').trim()
    const scope = normalizeAiListScope(scopeRaw)
    if (!scope) return null

    const confidenceNum = Number(raw.confidence ?? 0)
    const confidence = Number.isFinite(confidenceNum)
      ? Math.max(0, Math.min(1, confidenceNum))
      : 0

    const dateRaw = String(raw.date ?? '').trim()
    const date = isValidDate(dateRaw) ? dateRaw : undefined

    const monthRaw = Number(raw.month)
    const month = Number.isFinite(monthRaw) ? Math.round(monthRaw) : undefined

    const yearRaw = Number(raw.year)
    const year = Number.isFinite(yearRaw) ? Math.round(yearRaw) : undefined

    const keywordRaw = normalizeKeywordForFilter(String(raw.keyword ?? ''))
    const keyword = keywordRaw || undefined

    const intent: AiListIntent = {
      scope,
      confidence,
      ...(date ? { date } : {}),
      ...(typeof month === 'number' ? { month } : {}),
      ...(typeof year === 'number' ? { year } : {}),
      ...(keyword ? { keyword } : {}),
    }
    return intent
  } catch (err) {
    console.error('Failed to extract calendar list intent with Groq:', err)
    return null
  }
}

function normalizeAiListScope(raw: string): CalendarListScope | null {
  const value = String(raw ?? '').trim().toLowerCase()
  if (value === 'today') return 'today'
  if (value === 'tomorrow') return 'tomorrow'
  if (value === 'week' || value === 'this_week') return 'week'
  if (value === 'next_week') return 'next_week'
  if (value === 'date') return 'date'
  if (value === 'month' || value === 'this_month') return 'month'
  if (value === 'next_month') return 'next_month'
  if (value === 'year_month' || value === 'ym') return 'year_month'
  if (value === 'year') return 'year'
  if (value === 'upcoming_30d' || value === 'upcoming' || value === 'next_30_days') return 'upcoming_30d'
  return null
}

async function extractCalendarIntentWithGroq(
  text: string,
  timezone: string,
  groqApiKey: string,
): Promise<AiCalendarIntent | null> {
  try {
    const now = new Date()
    const nowText = new Intl.DateTimeFormat('ja-JP', {
      timeZone: timezone,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
    }).format(now)

    const response = await fetch('https://api.groq.com/openai/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${groqApiKey}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        model: 'llama-3.3-70b-versatile',
        temperature: 0.1,
        messages: [
          {
            role: 'system',
            content: [
              'あなたは予定抽出専用のJSON抽出器です。',
              'ユーザー文から「カレンダーに予定登録すべき明確な意図」がある場合のみ should_create=true にしてください。',
              '日時が曖昧・未指定なら should_create=false。',
              `現在時刻は ${nowText} (${timezone})。相対表現（今日/明日/来週）を絶対日付に変換してください。`,
              '「18日の18時」のように月が未指定で日付だけある場合は、現在月（現在年）として解釈してください。',
              'JSONのみ返してください。説明文やコードブロックは不要です。',
              '返却JSONスキーマ:',
              '{"should_create":boolean,"confidence":number(0-1),"title":string,"date":"YYYY-MM-DD","time":"HH:mm","duration_min":number,"reason":string}',
              'duration_min が不明なら 60 としてください。',
            ].join('\n'),
          },
          { role: 'user', content: text },
        ],
      }),
    })

    if (!response.ok) {
      const err = await response.text()
      console.error('Groq extraction failed:', response.status, err)
      return null
    }

    const json = await response.json()
    const content = String(json?.choices?.[0]?.message?.content ?? '').trim()
    if (!content) return null

    const extracted = parseFirstJsonObject(content)
    if (!extracted || typeof extracted !== 'object') return null

    const raw = extracted as Record<string, unknown>
    const shouldCreate = Boolean(raw.should_create ?? raw.shouldCreate ?? false)
    const confidenceNum = Number(raw.confidence ?? 0)
    const confidence = Number.isFinite(confidenceNum)
      ? Math.max(0, Math.min(1, confidenceNum))
      : 0
    const title = String(raw.title ?? '').trim()
    const date = String(raw.date ?? '').trim()
    const time = String(raw.time ?? '').trim()
    const durationRaw = Number(raw.duration_min ?? raw.durationMin ?? DEFAULT_DURATION_MIN)
    const durationMin = Number.isFinite(durationRaw) ? Math.round(durationRaw) : DEFAULT_DURATION_MIN
    const reason = String(raw.reason ?? '').trim()

    return { shouldCreate, confidence, title, date, time, durationMin, reason }
  } catch (err) {
    console.error('Failed to extract calendar intent with Groq:', err)
    return null
  }
}

function parseFirstJsonObject(raw: string): unknown | null {
  const trimmed = raw
    .replace(/^```json\s*/i, '')
    .replace(/^```/, '')
    .replace(/```$/, '')
    .trim()
  try {
    return JSON.parse(trimmed)
  } catch {
    const start = trimmed.indexOf('{')
    const end = trimmed.lastIndexOf('}')
    if (start >= 0 && end > start) {
      const candidate = trimmed.slice(start, end + 1)
      try {
        return JSON.parse(candidate)
      } catch {
        return null
      }
    }
    return null
  }
}

function isValidAiCalendarIntent(intent: AiCalendarIntent): boolean {
  if (!intent.shouldCreate) return false
  if (!intent.title) return false
  if (!isValidDate(intent.date)) return false
  if (!isValidTime(intent.time)) return false
  if (!Number.isInteger(intent.durationMin) || intent.durationMin <= 0) return false
  if (intent.durationMin > MAX_DURATION_MIN) return false
  return true
}

function isHighConfidenceAiCalendarIntent(intent: AiCalendarIntent): boolean {
  return isValidAiCalendarIntent(intent) && intent.confidence >= AI_MIN_CONFIDENCE
}

function isConfirmableAiCalendarIntent(intent: AiCalendarIntent): boolean {
  return isValidAiCalendarIntent(intent) && intent.confidence >= AI_CONFIRMATION_MIN_CONFIDENCE
}

function buildConversationKey(roomId: string, userId: string | null): string {
  return `${roomId}::${userId ?? 'unknown'}`
}

function normalizeConfirmationDecision(rawText: string): 'yes' | 'no' | null {
  const compact = normalizeForRuleParsing(rawText)
    .toLowerCase()
    .replace(/\s+/g, '')
    .replace(/[。．.!！?？、,]/g, '')
  if (!compact) return null

  if (/^(はい|ok|okay|yes|y|登録|登録して|お願いします|おねがいします|お願い|おねがい)$/.test(compact)) {
    return 'yes'
  }
  if (/^(いいえ|no|n|キャンセル|中止|やめる|不要|登録しない|しない)$/.test(compact)) {
    return 'no'
  }
  return null
}

function isMissingPendingTableError(error: any): boolean {
  const code = String(error?.code ?? '')
  if (code === '42P01') return true
  const text = `${String(error?.message ?? '')} ${String(error?.details ?? '')}`.toLowerCase()
  return text.includes('calendar_pending_confirmations') && (text.includes('does not exist') || text.includes('relation'))
}

function encodeLegacyPendingContent(payload: {
  conversationKey: string
  title: string
  date: string
  time: string
  durationMin: number
  confidence: number
  reason: string | null
  expiresAt: string
}): string {
  return `${LEGACY_PENDING_PREFIX}${JSON.stringify(payload)}`
}

function decodeLegacyPendingContent(content: string): null | {
  conversationKey: string
  title: string
  date: string
  time: string
  durationMin: number
  confidence: number
  reason: string | null
  expiresAt: string
} {
  if (!content.startsWith(LEGACY_PENDING_PREFIX)) return null
  const jsonText = content.slice(LEGACY_PENDING_PREFIX.length)
  const parsed = parseFirstJsonObject(jsonText)
  if (!parsed || typeof parsed !== 'object') return null
  const raw = parsed as Record<string, unknown>
  const conversationKey = String(raw.conversationKey ?? '').trim()
  const title = String(raw.title ?? '').trim()
  const date = String(raw.date ?? '').trim()
  const time = String(raw.time ?? '').trim()
  const durationRaw = Number(raw.durationMin ?? raw.duration_min ?? DEFAULT_DURATION_MIN)
  const confidenceRaw = Number(raw.confidence ?? 0)
  const reasonRaw = String(raw.reason ?? '').trim()
  const expiresAt = String(raw.expiresAt ?? raw.expires_at ?? '').trim()

  if (!conversationKey || !title || !isValidDate(date) || !isValidTime(time) || !expiresAt) return null
  const durationMin = Number.isFinite(durationRaw) ? Math.round(durationRaw) : DEFAULT_DURATION_MIN
  if (!Number.isInteger(durationMin) || durationMin <= 0 || durationMin > MAX_DURATION_MIN) return null
  const confidence = Number.isFinite(confidenceRaw)
    ? Math.max(0, Math.min(1, confidenceRaw))
    : 0

  return {
    conversationKey,
    title,
    date,
    time,
    durationMin,
    confidence,
    reason: reasonRaw || null,
    expiresAt,
  }
}

async function fetchPendingCalendarConfirmation(
  supabase: any,
  roomId: string,
  userId: string | null,
): Promise<PendingCalendarConfirmation | null> {
  const conversationKey = buildConversationKey(roomId, userId)
  const { data, error } = await supabase
    .from(CALENDAR_PENDING_TABLE)
    .select('id, conversation_key, title, date, time, duration_min, confidence, reason, expires_at')
    .eq('conversation_key', conversationKey)
    .eq('status', 'pending')
    .order('created_at', { ascending: false })
    .limit(1)
    .maybeSingle()

  if (error) {
    if (!isMissingPendingTableError(error)) {
      console.error('Failed to fetch pending confirmation:', error)
      return null
    }

    const query = supabase
      .from('line_messages')
      .select('id, content')
      .eq('room_id', roomId)
      .eq('processed', true)
      .like('content', `${LEGACY_PENDING_PREFIX}%`)
      .order('created_at', { ascending: false })
      .limit(5)
    const scopedQuery = userId ? query.eq('user_id', userId) : query.is('user_id', null)
    const { data: fallbackRows, error: fallbackError } = await scopedQuery
    if (fallbackError) {
      console.error('Failed to fetch legacy pending confirmation:', fallbackError)
      return null
    }
    const rows = Array.isArray(fallbackRows) ? fallbackRows : []
    for (const row of rows) {
      const content = String(row?.content ?? '')
      const decoded = decodeLegacyPendingContent(content)
      if (!decoded) continue
      if (decoded.conversationKey !== conversationKey) continue
      return {
        storage: 'line_messages',
        id: String(row?.id ?? ''),
        conversation_key: decoded.conversationKey,
        title: decoded.title,
        date: decoded.date,
        time: decoded.time,
        duration_min: decoded.durationMin,
        confidence: decoded.confidence,
        reason: decoded.reason,
        expires_at: decoded.expiresAt,
      }
    }
    return null
  }
  if (!data) return null
  return {
    storage: 'pending_table',
    id: String(data.id),
    conversation_key: String(data.conversation_key),
    title: String(data.title),
    date: String(data.date),
    time: String(data.time),
    duration_min: Number(data.duration_min),
    confidence: Number(data.confidence),
    reason: data.reason ? String(data.reason) : null,
    expires_at: String(data.expires_at),
  }
}

async function resolvePendingCalendarConfirmation(
  supabase: any,
  pending: PendingCalendarConfirmation,
  status: 'confirmed' | 'cancelled' | 'expired' | 'superseded',
): Promise<void> {
  if (pending.storage === 'pending_table') {
    const { error } = await supabase
      .from(CALENDAR_PENDING_TABLE)
      .update({
        status,
        resolved_at: new Date().toISOString(),
      })
      .eq('id', Number(pending.id))
      .eq('status', 'pending')
    if (error) {
      console.error('Failed to resolve pending confirmation:', error)
    }
    return
  }

  const legacyPayload = {
    conversationKey: pending.conversation_key,
    title: pending.title,
    date: pending.date,
    time: pending.time,
    durationMin: pending.duration_min,
    confidence: pending.confidence,
    reason: pending.reason ?? null,
    expiresAt: pending.expires_at,
  }
  const legacyDoneContent = `${LEGACY_PENDING_DONE_PREFIX}${status}:${JSON.stringify(legacyPayload)}`
  const { error } = await supabase
    .from('line_messages')
    .update({ content: legacyDoneContent })
    .eq('id', pending.id)
    .eq('processed', true)
  if (error) {
    console.error('Failed to resolve legacy pending confirmation:', error)
  }
}

async function savePendingCalendarConfirmation(
  supabase: any,
  roomId: string,
  userId: string | null,
  sourceText: string,
  intent: AiCalendarIntent,
): Promise<boolean> {
  const conversationKey = buildConversationKey(roomId, userId)
  const nowIso = new Date().toISOString()
  const expiresAt = new Date(Date.now() + PENDING_CONFIRMATION_TTL_MIN * 60 * 1000).toISOString()

  const { error: supersedeError } = await supabase
    .from(CALENDAR_PENDING_TABLE)
    .update({
      status: 'superseded',
      resolved_at: nowIso,
    })
    .eq('conversation_key', conversationKey)
    .eq('status', 'pending')
  if (supersedeError && !isMissingPendingTableError(supersedeError)) {
    console.error('Failed to supersede pending confirmation:', supersedeError)
  }

  const { error: insertError } = await supabase
    .from(CALENDAR_PENDING_TABLE)
    .insert({
      conversation_key: conversationKey,
      room_id: roomId,
      user_id: userId,
      source_text: sourceText,
      title: cleanCalendarTitle(intent.title),
      date: intent.date,
      time: intent.time,
      duration_min: intent.durationMin,
      confidence: intent.confidence,
      reason: intent.reason || null,
      status: 'pending',
      expires_at: expiresAt,
    })

  if (insertError) {
    if (!isMissingPendingTableError(insertError)) {
      console.error('Failed to save pending confirmation:', insertError)
      return false
    }
    const payloadContent = encodeLegacyPendingContent({
      conversationKey,
      title: cleanCalendarTitle(intent.title),
      date: intent.date,
      time: intent.time,
      durationMin: intent.durationMin,
      confidence: intent.confidence,
      reason: intent.reason || null,
      expiresAt,
    })
    const { error: fallbackInsertError } = await supabase
      .from('line_messages')
      .insert({
        room_id: roomId,
        user_id: userId,
        content: payloadContent,
        processed: true,
      })
    if (fallbackInsertError) {
      console.error('Failed to save legacy pending confirmation:', fallbackInsertError)
      return false
    }
  }
  return true
}

function buildPendingCalendarConfirmationPrompt(
  intent: AiCalendarIntent,
  timezone: string,
): string {
  const title = cleanCalendarTitle(intent.title)
  const lines = [
    '予定候補を見つけました。登録しますか？',
  ]
  const start = parseJstDateTime(intent.date, intent.time)
  if (start) {
    lines.push(formatDateOnlyForLine(start, timezone))
    lines.push(formatTimeOnlyForLine(start, timezone))
  } else {
    lines.push(intent.date)
    lines.push(intent.time)
  }
  lines.push(title)
  lines.push('')
  lines.push('「はい」で登録 / 「いいえ」でキャンセル')
  return lines.join('\n')
}

async function tryHandlePendingCalendarConfirmation(
  text: string,
  supabase: any,
  env: CalendarEnv,
  roomId: string,
  userId: string | null,
): Promise<string | null> {
  const decision = normalizeConfirmationDecision(text)
  if (!decision) return null

  const pending = await fetchPendingCalendarConfirmation(supabase, roomId, userId)
  if (!pending) return null

  const expireAtMs = new Date(pending.expires_at).getTime()
  if (!Number.isFinite(expireAtMs) || Date.now() >= expireAtMs) {
    await resolvePendingCalendarConfirmation(supabase, pending, 'expired')
    return '確認待ちの予定が期限切れです。予定文をもう一度送ってください。'
  }

  if (decision === 'no') {
    await resolvePendingCalendarConfirmation(supabase, pending, 'cancelled')
    return '予定登録をキャンセルしました。'
  }

  const command: CalendarCreateCommand = {
    kind: 'create',
    date: pending.date,
    time: pending.time,
    durationMin: pending.duration_min,
    title: pending.title,
  }
  const result = await createCalendarEvent(command, env, roomId, userId)
  if (!result.ok) {
    return `予定登録に失敗しました。${result.error}\n再試行する場合は「はい」、中止する場合は「いいえ」を送ってください。`
  }

  await resolvePendingCalendarConfirmation(supabase, pending, 'confirmed')
  return [
    '確認済みの予定を登録しました。',
    formatDateOnlyForLine(result.startDate, env.timezone),
    formatTimeOnlyForLine(result.startDate, env.timezone),
    cleanCalendarTitle(result.summary),
  ].join('\n')
}

function isAcceptableAiListIntent(intent: AiListIntent): boolean {
  if (intent.confidence < AI_LIST_MIN_CONFIDENCE) return false

  if (intent.scope === 'date') {
    return !!intent.date && isValidDate(intent.date)
  }

  if (intent.scope === 'month') {
    if (typeof intent.month === 'undefined') return true
    return Number.isInteger(intent.month) && intent.month >= 1 && intent.month <= 12
  }

  if (intent.scope === 'year_month') {
    if (!Number.isInteger(intent.year) || !Number.isInteger(intent.month)) return false
    return intent.month >= 1 && intent.month <= 12 && intent.year >= 1900 && intent.year <= 2300
  }

  if (intent.scope === 'year') {
    return Number.isInteger(intent.year) && intent.year >= 1900 && intent.year <= 2300
  }

  return true
}

function extractCalendarCommandsFromText(rawText: string): CalendarCreateCommand[] {
  const normalized = normalizeForRuleParsing(rawText)
  const baseDate = new Date()
  const lines = normalized
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0)

  const commands: CalendarCreateCommand[] = []
  let currentHeading = ''

  for (const line of lines) {
    const headingMatch = line.match(/^【(.+?)】$/)
    if (headingMatch) {
      currentHeading = cleanCalendarTitle(headingMatch[1])
      continue
    }

    const slot = parseDateTimeSlotFromLine(line, baseDate)
    if (!slot) continue

    const title = currentHeading || inferTitleFromLine(line) || '予定'
    commands.push({
      kind: 'create',
      title,
      date: slot.date,
      time: slot.time,
      durationMin: slot.durationMin,
    })
  }

  if (commands.length === 0) {
    const fallbackSlot = parseDateTimeSlotFromLine(normalized, baseDate)
    if (fallbackSlot) {
      const firstHeading = normalized.match(/【([^】]+)】/)
      const fallbackTitle = firstHeading ? cleanCalendarTitle(firstHeading[1]) : '予定'
      commands.push({
        kind: 'create',
        title: fallbackTitle,
        date: fallbackSlot.date,
        time: fallbackSlot.time,
        durationMin: fallbackSlot.durationMin,
      })
    }
  }

  const deduped: CalendarCreateCommand[] = []
  const seen = new Set<string>()
  for (const cmd of commands) {
    const key = `${cmd.date}|${cmd.time}|${cmd.durationMin}|${cmd.title.toLowerCase()}`
    if (seen.has(key)) continue
    seen.add(key)
    deduped.push(cmd)
  }
  return deduped
}

function parseDateTimeSlotFromLine(line: string, baseDate = new Date()): { date: string; time: string; durationMin: number } | null {
  const normalized = normalizeForRuleParsing(line)
  const { year: currentYear, month: currentMonth } = getJstYearMonth(baseDate)

  const rangeRegex = /(?:日時\s*[::]\s*)?(\d{4})[\/.\-年](\d{1,2})[\/.\-月](\d{1,2})(?:日)?(?:\s*[（(][^）)]*[）)])?\s*(\d{1,2}):(\d{2})\s*[-~〜～‐‑‒–—―ー－](\d{1,2}):(\d{2})/
  const range = normalized.match(rangeRegex)
  if (range) {
    const year = Number(range[1])
    const month = Number(range[2])
    const day = Number(range[3])
    const startHour = Number(range[4])
    const startMin = Number(range[5])
    const endHour = Number(range[6])
    const endMin = Number(range[7])

    return buildSlotFromDateAndTime(
      year,
      month,
      day,
      { hour: startHour, minute: startMin },
      { hour: endHour, minute: endMin },
    )
  }

  const singleRegex = /(?:日時\s*[::]\s*)?(\d{4})[\/.\-年](\d{1,2})[\/.\-月](\d{1,2})(?:日)?(?:\s*[（(][^）)]*[）)])?\s*(\d{1,2}):(\d{2})/
  const single = normalized.match(singleRegex)
  if (single) {
    const year = Number(single[1])
    const month = Number(single[2])
    const day = Number(single[3])
    const hour = Number(single[4])
    const min = Number(single[5])

    return buildSlotFromDateAndTime(
      year,
      month,
      day,
      { hour, minute: min },
      null,
    )
  }

  const monthDayRangeRegex = /(\d{1,2})月(\d{1,2})日(?:\s*[（(][^）)]*[）)])?\s*([0-9]{1,2}(?::[0-9]{2}|時(?:\s*[0-9]{1,2}分?)?))\s*(?:から|[-~〜～‐‑‒–—―ー－])\s*([0-9]{1,2}(?::[0-9]{2}|時(?:\s*[0-9]{1,2}分?)?))/
  const monthDayRange = normalized.match(monthDayRangeRegex)
  if (monthDayRange) {
    const month = Number(monthDayRange[1])
    const day = Number(monthDayRange[2])
    const start = parseFlexibleTimeToken(monthDayRange[3])
    const end = parseFlexibleTimeToken(monthDayRange[4])
    if (!start || !end) return null
    return buildSlotFromDateAndTime(currentYear, month, day, start, end)
  }

  const monthDaySingleRegex = /(\d{1,2})月(\d{1,2})日(?:\s*[（(][^）)]*[）)])?\s*(?:の|に)?\s*([0-9]{1,2}(?::[0-9]{2}|時(?:\s*[0-9]{1,2}分?)?))(?:\s*から)?/
  const monthDaySingle = normalized.match(monthDaySingleRegex)
  if (monthDaySingle) {
    const month = Number(monthDaySingle[1])
    const day = Number(monthDaySingle[2])
    const start = parseFlexibleTimeToken(monthDaySingle[3])
    if (!start) return null
    return buildSlotFromDateAndTime(currentYear, month, day, start, null)
  }

  const dayRangeRegex = /(?:^|[^\d])(\d{1,2})日(?:\s*[（(][^）)]*[）)])?\s*(?:の|に)?\s*([0-9]{1,2}(?::[0-9]{2}|時(?:\s*[0-9]{1,2}分?)?))\s*(?:から|[-~〜～‐‑‒–—―ー－])\s*([0-9]{1,2}(?::[0-9]{2}|時(?:\s*[0-9]{1,2}分?)?))/
  const dayRange = normalized.match(dayRangeRegex)
  if (dayRange) {
    const day = Number(dayRange[1])
    const start = parseFlexibleTimeToken(dayRange[2])
    const end = parseFlexibleTimeToken(dayRange[3])
    if (!start || !end) return null
    return buildSlotFromDateAndTime(currentYear, currentMonth, day, start, end)
  }

  const daySingleRegex = /(?:^|[^\d])(\d{1,2})日(?:\s*[（(][^）)]*[）)])?\s*(?:の|に)?\s*([0-9]{1,2}(?::[0-9]{2}|時(?:\s*[0-9]{1,2}分?)?))(?:\s*から)?/
  const daySingle = normalized.match(daySingleRegex)
  if (daySingle) {
    const day = Number(daySingle[1])
    const start = parseFlexibleTimeToken(daySingle[2])
    if (!start) return null
    return buildSlotFromDateAndTime(currentYear, currentMonth, day, start, null)
  }

  return null
}

function parseFlexibleTimeToken(raw: string): { hour: number; minute: number } | null {
  const token = normalizeForRuleParsing(raw).replace(/\s+/g, '')
  if (!token) return null

  let m = token.match(/^(\d{1,2}):(\d{2})$/)
  if (m) {
    const hour = Number(m[1])
    const minute = Number(m[2])
    if (!Number.isInteger(hour) || !Number.isInteger(minute)) return null
    if (hour < 0 || hour > 23 || minute < 0 || minute > 59) return null
    return { hour, minute }
  }

  m = token.match(/^(\d{1,2})時(?:(\d{1,2})分?)?$/)
  if (m) {
    const hour = Number(m[1])
    const minute = m[2] ? Number(m[2]) : 0
    if (!Number.isInteger(hour) || !Number.isInteger(minute)) return null
    if (hour < 0 || hour > 23 || minute < 0 || minute > 59) return null
    return { hour, minute }
  }

  return null
}

function buildSlotFromDateAndTime(
  year: number,
  month: number,
  day: number,
  start: { hour: number; minute: number },
  end: { hour: number; minute: number } | null,
): { date: string; time: string; durationMin: number } | null {
  const date = `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`
  const time = `${String(start.hour).padStart(2, '0')}:${String(start.minute).padStart(2, '0')}`
  if (!isValidDate(date) || !isValidTime(time)) return null

  if (!end) {
    return { date, time, durationMin: DEFAULT_DURATION_MIN }
  }

  const durationMin = computeDurationMinutes(start.hour, start.minute, end.hour, end.minute)
  return {
    date,
    time,
    durationMin: Math.max(1, Math.min(MAX_DURATION_MIN, durationMin)),
  }
}

function computeDurationMinutes(
  startHour: number,
  startMin: number,
  endHour: number,
  endMin: number,
): number {
  const start = startHour * 60 + startMin
  let end = endHour * 60 + endMin
  if (end <= start) end += 24 * 60
  const diff = end - start
  if (!Number.isFinite(diff) || diff <= 0) return DEFAULT_DURATION_MIN
  return diff
}

function inferTitleFromLine(line: string): string | null {
  const normalized = normalizeForRuleParsing(line)
  const stripped = normalized
    .replace(
      /(?:日時\s*[::]\s*)?(?:\d{4}[\/.\-年]\d{1,2}[\/.\-月]\d{1,2}日?)(?:\s*[（(][^）)]*[）)])?\s*\d{1,2}(?::\d{2}|時(?:\s*\d{1,2}分?)?)(?:\s*(?:から|[-~〜～‐‑‒–—―ー－])\s*\d{1,2}(?::\d{2}|時(?:\s*\d{1,2}分?)?))?/g,
      ' ',
    )
    .replace(
      /(?:今月|来月|再来月|先月|今日|明日|明後日|本日|当日)?(?:の)?\s*\d{1,2}月\d{1,2}日(?:\s*[（(][^）)]*[）)])?\s*\d{1,2}(?::\d{2}|時(?:\s*\d{1,2}分?)?)(?:\s*(?:から|[-~〜～‐‑‒–—―ー－])\s*\d{1,2}(?::\d{2}|時(?:\s*\d{1,2}分?)?))?/g,
      ' ',
    )
    .replace(
      /(?:今月|来月|再来月|先月|今日|明日|明後日|本日|当日)?(?:の)?\s*\d{1,2}日(?:\s*[（(][^）)]*[）)])?\s*\d{1,2}(?::\d{2}|時(?:\s*\d{1,2}分?)?)(?:\s*(?:から|[-~〜～‐‑‒–—―ー－])\s*\d{1,2}(?::\d{2}|時(?:\s*\d{1,2}分?)?))?/g,
      ' ',
    )
    .replace(/(?:^|[\s、,])(?:今月|来月|再来月|先月|今週|来週|再来週|今日|明日|明後日|本日|当日)(?:の)?/g, ' ')
    .replace(/(?:^|[\s、,])(?:\d{1,2}日|(?:\d{1,2}月\d{1,2}日))(?:$|[\s、,])/g, ' ')
    .replace(/(?:^|[\s、,])\d{1,2}(?::\d{2}|時(?:\s*\d{1,2}分?)?)(?:$|[\s、,])/g, ' ')
    .replace(/(?:^|[\s、,])(?:から|まで|開始|終了)(?:$|[\s、,])/g, ' ')
    .replace(/(?:です|ます|でした|ました|ですか|でしょうか)\s*$/g, ' ')
    .replace(/[。．.!！?？]+$/g, ' ')
    .replace(/^[\s:：\-]+/, '')
    .replace(/\s+/g, ' ')
    .trim()
  if (!stripped) return null
  return cleanCalendarTitle(stripped)
}

function cleanCalendarTitle(raw: string): string {
  const cleaned = normalizeSpaces(raw)
    .replace(/[【】]/g, '')
    .replace(/^[\s、,。．:：\-]+/, '')
    .replace(/[\s、,。．]+$/g, '')
    .trim()
  if (!cleaned) return '予定'
  return cleaned.length > 120 ? cleaned.slice(0, 120) : cleaned
}

function normalizeForRuleParsing(text: string): string {
  return normalizeSpaces(text)
    .replace(/[！-～]/g, (ch) => String.fromCharCode(ch.charCodeAt(0) - 0xFEE0))
    .replace(/[‐‑‒–—―－〜～]/g, '-')
    .replace(/[\t\r]+/g, ' ')
}

async function autoCreateCalendarEventsFromCommands(
  commands: CalendarCreateCommand[],
  env: CalendarEnv,
  roomId: string,
  userId: string | null,
  sourceLabel: string,
): Promise<string | null> {
  if (commands.length === 0) return null

  const validFutureCommands: CalendarCreateCommand[] = []
  let pastCount = 0
  const now = Date.now()

  for (const cmd of commands) {
    const start = parseJstDateTime(cmd.date, cmd.time)
    if (!start) continue
    if (start.getTime() < now - PAST_EVENT_GRACE_MS) {
      pastCount += 1
      continue
    }
    validFutureCommands.push(cmd)
  }

  if (validFutureCommands.length === 0) {
    if (pastCount > 0) {
      return `${sourceLabel}で予定候補を検出しましたが、すべて過去日時のため登録しませんでした。`
    }
    return null
  }

  const targets = validFutureCommands.slice(0, AI_AUTO_CREATE_MAX_EVENTS)
  const accessToken = await fetchGoogleAccessToken(env)
  const successes: Array<{ summary: string; startDate: Date }> = []
  const failures: string[] = []

  for (const cmd of targets) {
    const result = await createCalendarEvent(cmd, env, roomId, userId, accessToken)
    if (result.ok) {
      successes.push({ summary: result.summary, startDate: result.startDate })
    } else {
      failures.push(result.error)
    }
  }

  if (successes.length === 0) {
    const failureHead = failures[0] || '登録処理に失敗しました。'
    return `${sourceLabel}で予定候補を検出しましたが登録に失敗しました。\n${failureHead}`
  }

  const lines = [`予定を自動登録しました（${successes.length}件）。`]
  const shown = successes.slice(0, 3)
  for (let i = 0; i < shown.length; i += 1) {
    const item = shown[i]
    lines.push(formatDateOnlyForLine(item.startDate, env.timezone))
    lines.push(formatTimeOnlyForLine(item.startDate, env.timezone))
    lines.push(cleanCalendarTitle(item.summary))
    if (i < shown.length - 1) {
      lines.push('')
    }
  }
  if (successes.length > 3) {
    lines.push(`他 ${successes.length - 3} 件`)
  }
  if (pastCount > 0) {
    lines.push(`過去日時 ${pastCount} 件はスキップしました。`)
  }
  if (validFutureCommands.length > AI_AUTO_CREATE_MAX_EVENTS) {
    lines.push(`上限のため先頭 ${AI_AUTO_CREATE_MAX_EVENTS} 件のみ登録しました。`)
  }
  if (failures.length > 0) {
    lines.push(`登録失敗 ${failures.length} 件`)
  }
  return lines.join('\n')
}

function parseCalendarCommand(rawText: string): CalendarCommandParseResult {
  const text = normalizeSpaces(rawText)
  if (!text) {
    return { matched: false, command: null, error: null }
  }

  if (text.startsWith('予定登録') || text.startsWith('予定追加')) {
    const body = text.replace(/^予定(?:登録|追加)\s*/, '')
    const m = body.match(/^(\d{4}-\d{2}-\d{2})\s+(\d{1,2}:\d{2})\s+(.+)$/)
    if (!m) {
      return {
        matched: true,
        command: null,
        error: '形式エラーです。\n例: 予定登録 2026-04-07 15:30 60 定例ミーティング',
      }
    }

    const date = m[1]
    const time = m[2]
    let remaining = m[3].trim()
    let durationMin = DEFAULT_DURATION_MIN

    const durationMatch = remaining.match(/^(\d{1,3})\s+(.+)$/)
    if (durationMatch) {
      durationMin = Number(durationMatch[1])
      remaining = durationMatch[2].trim()
    }

    if (!isValidDate(date)) {
      return { matched: true, command: null, error: '日付は YYYY-MM-DD 形式で指定してください。' }
    }
    if (!isValidTime(time)) {
      return { matched: true, command: null, error: '時刻は HH:mm 形式（24時間）で指定してください。' }
    }
    if (!Number.isInteger(durationMin) || durationMin <= 0 || durationMin > MAX_DURATION_MIN) {
      return { matched: true, command: null, error: `所要時間は1〜${MAX_DURATION_MIN}分で指定してください。` }
    }
    if (!remaining) {
      return { matched: true, command: null, error: '予定タイトルを指定してください。' }
    }

    return {
      matched: true,
      command: { kind: 'create', date, time, durationMin, title: remaining },
      error: null,
    }
  }

  if (text.startsWith('予定確認') || text.startsWith('予定一覧') || text.startsWith('予定報告')) {
    const body = text.replace(/^予定(?:確認|一覧|報告)\s*/, '').trim()
    const listCommand = parseCalendarListScope(body)
    if (listCommand) {
      return { matched: true, command: { kind: 'list', ...listCommand }, error: null }
    }

    return {
      matched: true,
      command: null,
      error: [
        '形式エラーです。',
        '例: 予定確認 今日',
        '例: 予定確認 来週',
        '例: 予定確認 2026-04-07',
        '例: 予定確認 4月',
        '例: 予定確認 2026年4月',
        '例: 予定確認 今後',
      ].join('\n'),
    }
  }

  const naturalListCommand = parseNaturalLanguageListQuery(text)
  if (naturalListCommand) {
    return { matched: true, command: { kind: 'list', ...naturalListCommand }, error: null }
  }

  return { matched: false, command: null, error: null }
}

function parseCalendarListScope(bodyRaw: string): Omit<Extract<CalendarCommand, { kind: 'list' }>, 'kind'> | null {
  const body = normalizeForRuleParsing(bodyRaw).replace(/\s+/g, '')
  const canonical = canonicalizeListScopeText(body)
  if (!canonical || canonical === '今日') {
    return { scope: 'today' }
  }
  if (canonical === '明日') {
    return { scope: 'tomorrow' }
  }
  if (canonical === '今週') {
    return { scope: 'week' }
  }
  if (canonical === '来週') {
    return { scope: 'next_week' }
  }
  if (canonical === '今月' || canonical === '当月' || canonical === '今月中') {
    return { scope: 'month' }
  }
  if (canonical === '来月' || canonical === '来月中') {
    return { scope: 'next_month' }
  }
  if (/^(今後|これから|直近|近日|近々|向こう30日|30日以内|1ヶ月|1か月|1ヵ月|一ヶ月)$/.test(canonical)) {
    return { scope: 'upcoming_30d' }
  }
  if (isValidDate(canonical)) {
    return { scope: 'date', date: canonical }
  }

  const ymSlash = canonical.match(/^(\d{4})[\/.-](\d{1,2})$/)
  if (ymSlash) {
    const year = Number(ymSlash[1])
    const month = Number(ymSlash[2])
    if (month >= 1 && month <= 12) {
      return { scope: 'year_month', year, month }
    }
  }

  const ymJa = canonical.match(/^(\d{4})年(\d{1,2})月$/)
  if (ymJa) {
    const year = Number(ymJa[1])
    const month = Number(ymJa[2])
    if (month >= 1 && month <= 12) {
      return { scope: 'year_month', year, month }
    }
  }

  const monthOnly = canonical.match(/^(\d{1,2})月$/)
  if (monthOnly) {
    const month = Number(monthOnly[1])
    if (month >= 1 && month <= 12) {
      return { scope: 'month', month }
    }
  }

  const yearOnly = canonical.match(/^(\d{4})年$/)
  if (yearOnly) {
    const year = Number(yearOnly[1])
    return { scope: 'year', year }
  }

  return null
}

function parseNaturalLanguageListQuery(rawText: string): Omit<Extract<CalendarCommand, { kind: 'list' }>, 'kind'> | null {
  const compact = normalizeForRuleParsing(rawText).replace(/\s+/g, '')
  if (!compact) return null
  if (/^予定(?:確認|一覧|報告)/.test(compact)) return null

  const hasQuestionIntent = /(いつ|何件|ありますか|あります|ある\?|ある？|ある$|教えて|見せて|みせて|知りたい|一覧|確認|どれ|どこ|空き|空いて|表示|表示して|出して|だして|見たい|確認したい)/.test(compact)
  const hasShortListIntent = /(?:今日|明日|今週|来週|今月|来月|当月|今月中|来月中|今後|これから|直近|近日|近々|向こう30日|30日以内|1ヶ月|1か月|1ヵ月|一ヶ月|\d{1,2}月|\d{4}年\d{1,2}月|\d{4}[\/.-]\d{1,2}|\d{4}年)(?:の)?予定(?:一覧|確認|報告)?(?:だけ)?$/.test(compact)
  if (!hasQuestionIntent && !hasShortListIntent) return null

  const hasScheduleHint = /(予定|会議|打ち合わせ|打合せ|ミーティング|meeting|mtg|予約|アポ|面談|イベント)/.test(compact)
  if (!hasScheduleHint) return null

  const scopeToken = detectRangeToken(compact)
  const scope = scopeToken ? parseCalendarListScope(scopeToken) : { scope: 'upcoming_30d' as CalendarListScope }
  if (!scope) return null

  let residue = scopeToken ? compact.replace(scopeToken, '') : compact
  residue = residue
    .replace(/(?:の)?予定(?:一覧|確認|報告)?/g, ' ')
    .replace(/(?:は|を|に|で|が|って|とは)/g, ' ')
    .replace(/(?:いつ|ありますか|あります|ある|教えて|見せて|みせて|知りたい|確認|一覧|表示|表示して|出して|だして|見たい|確認したい|ですか|かな|か)/g, ' ')
    .replace(/(?:今後|これから|直近|近日|近々|向こう30日|30日以内|1ヶ月|1か月|1ヵ月|一ヶ月)/g, ' ')
    .replace(/[?？!！。．、,]/g, ' ')
    .replace(/^の+/, ' ')

  const keyword = normalizeKeywordForFilter(residue)
  return keyword ? { ...scope, keyword } : scope
}

function detectRangeToken(compactText: string): string | null {
  const patterns = [
    /(\d{4}年\d{1,2}月)/,
    /(\d{4}[\/.-]\d{1,2})/,
    /(\d{4}年)/,
    /(今後|これから|直近|近日|近々|向こう30日|30日以内|1ヶ月|1か月|1ヵ月|一ヶ月)/,
    /(今月中|来月中|今月|来月|今週|来週|今日|明日|当月)/,
    /(\d{1,2}月)/,
  ]
  for (const pattern of patterns) {
    const match = compactText.match(pattern)
    if (match && match[1]) return match[1]
  }
  return null
}

function normalizeKeywordForFilter(raw: string): string {
  const cleaned = normalizeSpaces(raw.replace(/\s+/g, ' '))
  const trimmed = cleaned.replace(/^の+/, '').trim()
  if (!trimmed) return ''
  if (trimmed.length > 60) return trimmed.slice(0, 60)
  return trimmed
}

function canonicalizeListScopeText(raw: string): string {
  return raw
    .replace(/(?:の)?予定(?:一覧|確認|報告)?/g, '')
    .replace(/(?:を|は)?(?:教えて|みせて|見せて|表示|表示して|出して|だして|確認して|確認|ください|下さい|お願いします|お願い|知りたい|見たい|確認したい|いつ|ですか|かな|か)/g, '')
    .replace(/[?？!！。．、,]+/g, '')
    .trim()
}

async function buildCalendarReplyMessage(
  parseResult: CalendarCommandParseResult,
  calendarEnvState: CalendarEnvState,
  roomId: string,
  userId: string | null,
): Promise<string> {
  if (parseResult.error) {
    return parseResult.error
  }
  if (!parseResult.command) {
    return 'コマンドを解釈できませんでした。'
  }
  if (!calendarEnvState.ok) {
    return [
      'Googleカレンダー連携が未設定です。',
      `不足: ${calendarEnvState.missing.join(', ')}`,
      '設定後に再実行してください。',
    ].join('\n')
  }

  try {
    if (parseResult.command.kind === 'create') {
      return await createCalendarEventReply(
        parseResult.command,
        calendarEnvState.env,
        roomId,
        userId,
      )
    }
    return await listCalendarEventsReply(parseResult.command, calendarEnvState.env)
  } catch (err) {
    console.error('Calendar command failed:', err)
    return `カレンダー操作に失敗しました。${err instanceof Error ? err.message : String(err)}`
  }
}

function loadCalendarEnv(): CalendarEnvState {
  const calendarId = (Deno.env.get('GOOGLE_CALENDAR_ID') ?? '').trim()
  const serviceAccountEmail = (Deno.env.get('GOOGLE_SERVICE_ACCOUNT_EMAIL') ?? '').trim()
  const serviceAccountPrivateKey = (Deno.env.get('GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY') ?? '').trim()
  const timezone = (Deno.env.get('GOOGLE_CALENDAR_TIMEZONE') ?? 'Asia/Tokyo').trim() || 'Asia/Tokyo'

  const missing: string[] = []
  if (!calendarId) missing.push('GOOGLE_CALENDAR_ID')
  if (!serviceAccountEmail) missing.push('GOOGLE_SERVICE_ACCOUNT_EMAIL')
  if (!serviceAccountPrivateKey) missing.push('GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY')

  if (missing.length > 0) {
    return { ok: false, missing }
  }

  return {
    ok: true,
    env: {
      calendarId,
      serviceAccountEmail,
      serviceAccountPrivateKey: normalizePrivateKey(serviceAccountPrivateKey),
      timezone,
    },
  }
}

async function createCalendarEventReply(
  command: CalendarCreateCommand,
  env: CalendarEnv,
  roomId: string,
  userId: string | null,
): Promise<string> {
  const result = await createCalendarEvent(command, env, roomId, userId)
  if (!result.ok) {
    return `予定登録に失敗しました。${result.error}`
  }
  const startText = formatDateTimeForLine(result.startDate, env.timezone)
  const endText = formatDateTimeForLine(result.endDate, env.timezone)

  return [
    '予定を登録しました。',
    `件名: ${result.summary}`,
    `開始: ${startText}`,
    `終了: ${endText}`,
  ].join('\n')
}

async function createCalendarEvent(
  command: CalendarCreateCommand,
  env: CalendarEnv,
  roomId: string,
  userId: string | null,
  providedAccessToken?: string,
): Promise<CalendarCreateResult> {
  const startDate = parseJstDateTime(command.date, command.time)
  if (!startDate) {
    return { ok: false, error: '日時の解釈に失敗しました。' }
  }
  const endDate = new Date(startDate.getTime() + command.durationMin * 60 * 1000)
  const accessToken = providedAccessToken || await fetchGoogleAccessToken(env)

  const calendarPath = `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(env.calendarId)}/events`
  const response = await fetch(calendarPath, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${accessToken}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      summary: command.title,
      description: `LINE room_id: ${roomId}\nLINE user_id: ${userId ?? 'unknown'}\nsource: line-webhook`,
      start: {
        dateTime: startDate.toISOString(),
        timeZone: env.timezone,
      },
      end: {
        dateTime: endDate.toISOString(),
        timeZone: env.timezone,
      },
    }),
  })

  if (!response.ok) {
    const text = await response.text()
    return { ok: false, error: `Google Calendar API error (${response.status}): ${text}` }
  }

  const created = await response.json()
  const summary = String(created?.summary ?? command.title)
  return { ok: true, summary, startDate, endDate }
}

async function listCalendarEventsReply(
  command: Extract<CalendarCommand, { kind: 'list' }>,
  env: CalendarEnv,
): Promise<string> {
  const range = resolveListRange(command)
  const accessToken = await fetchGoogleAccessToken(env)
  const maxResults = suggestMaxResultsForListScope(command.scope, !!command.keyword)

  const url = new URL(`https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(env.calendarId)}/events`)
  url.searchParams.set('singleEvents', 'true')
  url.searchParams.set('orderBy', 'startTime')
  url.searchParams.set('timeMin', range.start.toISOString())
  url.searchParams.set('timeMax', range.end.toISOString())
  url.searchParams.set('maxResults', String(maxResults))
  url.searchParams.set('timeZone', env.timezone)

  const response = await fetch(url.toString(), {
    method: 'GET',
    headers: {
      'Authorization': `Bearer ${accessToken}`,
      'Content-Type': 'application/json',
    },
  })

  if (!response.ok) {
    const text = await response.text()
    throw new Error(`Google Calendar API error (${response.status}): ${text}`)
  }

  const data = await response.json()
  const allItems: GoogleCalendarEvent[] = Array.isArray(data?.items) ? data.items : []
  const items = command.keyword
    ? allItems.filter((item) => eventMatchesKeyword(item, command.keyword as string))
    : allItems

  if (items.length === 0) {
    if (command.keyword) {
      return `「${command.keyword}」に一致する予定はありません（${range.label}）`
    }
    return `予定はありません（${range.label}）`
  }

  const heading = command.keyword
    ? `予定一覧（${range.label} / キーワード: ${command.keyword}）`
    : `予定一覧（${range.label}）`

  const lines: string[] = [heading]
  for (let i = 0; i < items.length; i += 1) {
    const item = items[i]
    const detail = formatEventDetailBlock(item, env.timezone)
    lines.push(`${i + 1}.`)
    lines.push(`  日付: ${detail.date}`)
    lines.push(`  時間: ${detail.time}`)
    lines.push(`  予定: ${detail.title}`)
    lines.push(`  内容: ${detail.content}`)
    if (i < items.length - 1) {
      lines.push('')
    }
  }
  return lines.join('\n')
}

function resolveListRange(command: Extract<CalendarCommand, { kind: 'list' }>): {
  start: Date
  end: Date
  label: string
} {
  const todayJst = getTodayJstDateString()
  const { year: currentYear, month: currentMonth } = getJstYearMonth()
  if (command.scope === 'today') {
    return { ...dayRangeFromJstDate(todayJst), label: '今日' }
  }
  if (command.scope === 'tomorrow') {
    const tomorrow = addDaysToJstDateString(todayJst, 1)
    return { ...dayRangeFromJstDate(tomorrow), label: '明日' }
  }
  if (command.scope === 'week') {
    const start = dayRangeFromJstDate(todayJst).start
    const end = new Date(start.getTime() + 7 * 24 * 60 * 60 * 1000)
    return { start, end, label: '今週（7日間）' }
  }
  if (command.scope === 'next_week') {
    const thisWeekStart = dayRangeFromJstDate(todayJst).start
    const start = new Date(thisWeekStart.getTime() + 7 * 24 * 60 * 60 * 1000)
    const end = new Date(start.getTime() + 7 * 24 * 60 * 60 * 1000)
    return { start, end, label: '来週（7日間）' }
  }
  if (command.scope === 'upcoming_30d') {
    const start = new Date()
    const end = new Date(start.getTime() + 30 * 24 * 60 * 60 * 1000)
    return { start, end, label: '今後30日' }
  }
  if (command.scope === 'month') {
    const month = command.month ?? currentMonth
    const range = monthRangeFromJstYearMonth(currentYear, month)
    return { ...range, label: `${currentYear}年${month}月` }
  }
  if (command.scope === 'next_month') {
    const shifted = shiftJstYearMonth(currentYear, currentMonth, 1)
    const range = monthRangeFromJstYearMonth(shifted.year, shifted.month)
    return { ...range, label: `${shifted.year}年${shifted.month}月` }
  }
  if (command.scope === 'year_month') {
    const year = command.year ?? currentYear
    const month = command.month ?? currentMonth
    const range = monthRangeFromJstYearMonth(year, month)
    return { ...range, label: `${year}年${month}月` }
  }
  if (command.scope === 'year') {
    const year = command.year ?? currentYear
    const range = yearRangeFromJstYear(year)
    return { ...range, label: `${year}年` }
  }
  const date = command.date ?? todayJst
  return { ...dayRangeFromJstDate(date), label: date }
}

function suggestMaxResultsForListScope(
  scope: Extract<CalendarCommand, { kind: 'list' }>['scope'],
  hasKeyword: boolean,
): number {
  if (scope === 'today' || scope === 'tomorrow' || scope === 'date') return hasKeyword ? 40 : 20
  if (scope === 'week' || scope === 'next_week') return hasKeyword ? 100 : 50
  if (scope === 'upcoming_30d') return hasKeyword ? 250 : 120
  if (scope === 'month' || scope === 'next_month' || scope === 'year_month') return hasKeyword ? 250 : 120
  if (scope === 'year') return hasKeyword ? 500 : 300
  return hasKeyword ? 100 : 50
}

function getTodayJstDateString(base = new Date()): string {
  const jst = new Date(base.getTime() + JST_OFFSET_MS)
  const y = jst.getUTCFullYear()
  const m = String(jst.getUTCMonth() + 1).padStart(2, '0')
  const d = String(jst.getUTCDate()).padStart(2, '0')
  return `${y}-${m}-${d}`
}

function getJstYearMonth(base = new Date()): { year: number; month: number } {
  const jst = new Date(base.getTime() + JST_OFFSET_MS)
  return {
    year: jst.getUTCFullYear(),
    month: jst.getUTCMonth() + 1,
  }
}

function shiftJstYearMonth(year: number, month: number, deltaMonths: number): { year: number; month: number } {
  const shifted = new Date(Date.UTC(year, month - 1 + deltaMonths, 1))
  return {
    year: shifted.getUTCFullYear(),
    month: shifted.getUTCMonth() + 1,
  }
}

function monthRangeFromJstYearMonth(year: number, month: number): { start: Date; end: Date } {
  const safeMonth = Math.max(1, Math.min(12, month))
  const startUtc = new Date(Date.UTC(year, safeMonth - 1, 1, -9, 0, 0, 0))
  const nextMonth = shiftJstYearMonth(year, safeMonth, 1)
  const endUtc = new Date(Date.UTC(nextMonth.year, nextMonth.month - 1, 1, -9, 0, 0, 0))
  return { start: startUtc, end: endUtc }
}

function yearRangeFromJstYear(year: number): { start: Date; end: Date } {
  const startUtc = new Date(Date.UTC(year, 0, 1, -9, 0, 0, 0))
  const endUtc = new Date(Date.UTC(year + 1, 0, 1, -9, 0, 0, 0))
  return { start: startUtc, end: endUtc }
}

function addDaysToJstDateString(date: string, days: number): string {
  const [y, m, d] = date.split('-').map(Number)
  const dt = new Date(Date.UTC(y, m - 1, d + days))
  const yy = dt.getUTCFullYear()
  const mm = String(dt.getUTCMonth() + 1).padStart(2, '0')
  const dd = String(dt.getUTCDate()).padStart(2, '0')
  return `${yy}-${mm}-${dd}`
}

function dayRangeFromJstDate(date: string): { start: Date; end: Date } {
  const [y, m, d] = date.split('-').map(Number)
  const startUtc = new Date(Date.UTC(y, m - 1, d, -9, 0, 0, 0))
  const endUtc = new Date(startUtc.getTime() + 24 * 60 * 60 * 1000)
  return { start: startUtc, end: endUtc }
}

function parseJstDateTime(date: string, time: string): Date | null {
  if (!isValidDate(date) || !isValidTime(time)) return null
  const [y, m, d] = date.split('-').map(Number)
  const [h, min] = time.split(':').map(Number)
  const utcDate = new Date(Date.UTC(y, m - 1, d, h - 9, min, 0, 0))

  // Verify no overflow happened during Date normalization.
  const jstDate = new Date(utcDate.getTime() + JST_OFFSET_MS)
  const checkY = jstDate.getUTCFullYear()
  const checkM = jstDate.getUTCMonth() + 1
  const checkD = jstDate.getUTCDate()
  const checkH = jstDate.getUTCHours()
  const checkMin = jstDate.getUTCMinutes()
  if (checkY !== y || checkM !== m || checkD !== d || checkH !== h || checkMin !== min) {
    return null
  }

  return utcDate
}

function isValidDate(value: string): boolean {
  const m = value.match(/^(\d{4})-(\d{2})-(\d{2})$/)
  if (!m) return false
  const y = Number(m[1])
  const mon = Number(m[2])
  const d = Number(m[3])
  if (mon < 1 || mon > 12 || d < 1 || d > 31) return false
  const dt = new Date(Date.UTC(y, mon - 1, d))
  return dt.getUTCFullYear() === y && (dt.getUTCMonth() + 1) === mon && dt.getUTCDate() === d
}

function isValidTime(value: string): boolean {
  const m = value.match(/^(\d{1,2}):(\d{2})$/)
  if (!m) return false
  const h = Number(m[1])
  const min = Number(m[2])
  return h >= 0 && h <= 23 && min >= 0 && min <= 59
}

function formatDateTimeForLine(date: Date, timezone: string): string {
  return new Intl.DateTimeFormat('ja-JP', {
    timeZone: timezone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).format(date)
}

function formatDateOnlyForLine(date: Date, timezone: string): string {
  return new Intl.DateTimeFormat('ja-JP', {
    timeZone: timezone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(date).replace(/\./g, '/')
}

function formatTimeOnlyForLine(date: Date, timezone: string): string {
  return new Intl.DateTimeFormat('ja-JP', {
    timeZone: timezone,
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).format(date)
}

function eventMatchesKeyword(event: GoogleCalendarEvent, keyword: string): boolean {
  const haystacks: string[] = []
  haystacks.push(String(event.summary ?? ''))
  haystacks.push(String(event.description ?? ''))
  haystacks.push(String(event.location ?? ''))

  if (Array.isArray(event.attendees)) {
    for (const attendee of event.attendees) {
      haystacks.push(String(attendee?.displayName ?? ''))
      haystacks.push(String(attendee?.email ?? ''))
    }
  }
  return keywordMatchesHaystacks(keyword, haystacks)
}

function keywordMatchesHaystacks(keyword: string, haystacks: string[]): boolean {
  const normalizedKeyword = normalizeKeywordForSearch(keyword)
  if (!normalizedKeyword) return true

  const target = normalizeKeywordForSearch(haystacks.join('\n'))
  const compactTarget = compactSearchText(target)
  if (!target && !compactTarget) return false

  const rawTokens = normalizedKeyword
    .replace(/[、,，/／|｜]+/g, ' ')
    .split(/\s+/)
    .filter((token) => token.length > 0)
  if (rawTokens.length === 0) return true
  const filteredTokens = rawTokens.filter((token) => !isIgnorableMessageSearchToken(token))
  const tokens = filteredTokens.length > 0 ? filteredTokens : rawTokens
  return tokens.every((token) => {
    const variants = expandKeywordVariants(token)
    return variants.some((variant) => {
      const normalizedVariant = normalizeKeywordForSearch(variant)
      const compactVariant = compactSearchText(variant)
      if (normalizedVariant && target.includes(normalizedVariant)) return true
      if (compactVariant && compactTarget.includes(compactVariant)) return true
      return false
    })
  })
}

function isIgnorableMessageSearchToken(token: string): boolean {
  const normalized = normalizeKeywordForSearch(token)
  if (!normalized) return true
  if (/^(です|ます|ですか|ますか|すか|でしょうか|か|かな|だっけ|っけ|ありますか|あるか|あります|ある)$/.test(normalized)) {
    return true
  }
  if (/^(こと|もの|やつ|内容|情報|記述|言及|会話|トーク|履歴|メッセージ|発言|ルーム|グループ)$/.test(normalized)) {
    return true
  }
  if (normalized.length <= 1 && /^[ぁ-んー]+$/.test(normalized)) return true
  return false
}

function normalizeKeywordForSearch(raw: string): string {
  return String(raw ?? '')
    .normalize('NFKC')
    .toLowerCase()
    .replace(/\u3000/g, ' ')
    .replace(/[‐‑‒–—―－]/g, '-')
    .replace(/[’'`´]/g, '')
    .replace(/\s+/g, ' ')
    .trim()
}

function compactSearchText(raw: string): string {
  return normalizeKeywordForSearch(raw)
    .replace(/[!！?？。．、,/:：;；"'“”‘’()（）\[\]{}【】<>＜＞\s]/g, '')
    .replace(/[-ー_]/g, '')
}

function expandKeywordVariants(token: string): string[] {
  const base = normalizeKeywordForSearch(token)
  if (!base) return []

  const variants = new Set<string>()
  const compactBase = compactSearchText(base)

  const addVariant = (value: string) => {
    const normalized = normalizeKeywordForSearch(value)
    if (!normalized) return
    variants.add(normalized)
  }

  addVariant(base)
  addVariant(base.replace(/^の+/, ''))
  addVariant(base.replace(/の/g, ''))
  addVariant(base.replace(/[-ー]/g, ''))

  for (const group of KEYWORD_SYNONYM_GROUPS) {
    const normalizedTerms = group.map((term) => normalizeKeywordForSearch(term))
    const compactTerms = normalizedTerms.map((term) => compactSearchText(term))
    const matched = normalizedTerms.includes(base) || compactTerms.includes(compactBase)
    if (!matched) continue
    for (const term of group) {
      addVariant(term)
      addVariant(term.replace(/[-ー]/g, ''))
    }
  }

  return Array.from(variants).filter((v) => v.length > 0)
}

function formatEventDetailBlock(
  event: GoogleCalendarEvent,
  timezone: string,
): { date: string; time: string; title: string; content: string } {
  let date = '(日付不明)'
  let time = '(時間不明)'

  const startDateTime = event.start?.dateTime
  const endDateTime = event.end?.dateTime
  const startDate = event.start?.date

  if (startDateTime) {
    const start = new Date(startDateTime)
    date = formatDateOnlyForLine(start, timezone)
    if (endDateTime) {
      const end = new Date(endDateTime)
      time = `${formatTimeOnlyForLine(start, timezone)}-${formatTimeOnlyForLine(end, timezone)}`
    } else {
      time = formatTimeOnlyForLine(start, timezone)
    }
  } else if (startDate) {
    date = String(startDate).replace(/-/g, '/')
    time = '終日'
  }

  const title = cleanCalendarTitle(String(event.summary ?? '(無題)'))
  const content = formatEventContentForList(event)
  return { date, time, title, content }
}

function formatEventContentForList(event: GoogleCalendarEvent): string {
  const pieces: string[] = []

  const location = normalizeInlineText(String(event.location ?? ''))
  if (location) {
    pieces.push(location)
  }

  const desc = sanitizeEventDescriptionForList(String(event.description ?? ''))
  if (desc) {
    pieces.push(desc)
  }

  if (pieces.length === 0) return '（内容なし）'
  return pieces.join(' / ')
}

function sanitizeEventDescriptionForList(raw: string): string {
  if (!raw) return ''
  const lines = raw
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .filter((line) => !/^(LINE room_id:|LINE user_id:|source:\s*line-webhook)/i.test(line))

  const merged = normalizeInlineText(lines.join(' / '))
  if (!merged) return ''
  if (merged.length > 140) return `${merged.slice(0, 140)}...`
  return merged
}

function normalizeInlineText(raw: string): string {
  return normalizeSpaces(raw).replace(/\s+/g, ' ')
}

function formatEventSchedule(event: GoogleCalendarEvent, timezone: string): string {
  const startDateTime = event.start?.dateTime
  const startDate = event.start?.date
  const endDateTime = event.end?.dateTime

  if (startDateTime) {
    const start = new Date(startDateTime)
    if (endDateTime) {
      const end = new Date(endDateTime)
      const datePart = new Intl.DateTimeFormat('ja-JP', {
        timeZone: timezone,
        month: '2-digit',
        day: '2-digit',
      }).format(start)
      const startHm = new Intl.DateTimeFormat('ja-JP', {
        timeZone: timezone,
        hour: '2-digit',
        minute: '2-digit',
        hour12: false,
      }).format(start)
      const endHm = new Intl.DateTimeFormat('ja-JP', {
        timeZone: timezone,
        hour: '2-digit',
        minute: '2-digit',
        hour12: false,
      }).format(end)
      return `${datePart} ${startHm}-${endHm}`
    }
    return formatDateTimeForLine(start, timezone)
  }

  if (startDate) {
    return `${startDate}（終日）`
  }

  return '(日時不明)'
}

async function fetchGoogleAccessToken(env: CalendarEnv): Promise<string> {
  const nowSec = Math.floor(Date.now() / 1000)
  const header = { alg: 'RS256', typ: 'JWT' }
  const payload = {
    iss: env.serviceAccountEmail,
    scope: CALENDAR_SCOPE,
    aud: 'https://oauth2.googleapis.com/token',
    exp: nowSec + 3600,
    iat: nowSec,
  }

  const unsigned = `${base64UrlEncodeText(JSON.stringify(header))}.${base64UrlEncodeText(JSON.stringify(payload))}`
  const signature = await signRs256(unsigned, env.serviceAccountPrivateKey)
  const assertion = `${unsigned}.${signature}`

  const body = new URLSearchParams()
  body.set('grant_type', 'urn:ietf:params:oauth:grant-type:jwt-bearer')
  body.set('assertion', assertion)

  const response = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: body.toString(),
  })

  if (!response.ok) {
    const text = await response.text()
    throw new Error(`OAuth token request failed (${response.status}): ${text}`)
  }

  const json = await response.json()
  const accessToken = String(json?.access_token ?? '')
  if (!accessToken) {
    throw new Error('OAuth token response missing access_token.')
  }
  return accessToken
}

async function signRs256(data: string, privateKeyPem: string): Promise<string> {
  const keyData = pemToArrayBuffer(privateKeyPem)
  const key = await crypto.subtle.importKey(
    'pkcs8',
    keyData,
    { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' },
    false,
    ['sign'],
  )
  const signature = await crypto.subtle.sign(
    'RSASSA-PKCS1-v1_5',
    key,
    new TextEncoder().encode(data),
  )
  return base64UrlEncodeBytes(new Uint8Array(signature))
}

function normalizePrivateKey(value: string): string {
  return value.replace(/\\n/g, '\n')
}

function pemToArrayBuffer(pem: string): ArrayBuffer {
  const cleaned = pem
    .replace('-----BEGIN PRIVATE KEY-----', '')
    .replace('-----END PRIVATE KEY-----', '')
    .replace(/\s+/g, '')
  const binary = atob(cleaned)
  const bytes = new Uint8Array(binary.length)
  for (let i = 0; i < binary.length; i += 1) {
    bytes[i] = binary.charCodeAt(i)
  }
  return bytes.buffer
}

function base64UrlEncodeText(value: string): string {
  return base64UrlEncodeBytes(new TextEncoder().encode(value))
}

function base64UrlEncodeBytes(value: Uint8Array): string {
  let binary = ''
  for (const b of value) {
    binary += String.fromCharCode(b)
  }
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '')
}

function normalizeSpaces(text: string): string {
  return text.replace(/\u3000/g, ' ').trim()
}

async function replyLineMessage(replyToken: string, text: string, channelAccessToken: string): Promise<{ ok: true } | { ok: false; error: string }> {
  const response = await fetch('https://api.line.me/v2/bot/message/reply', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${channelAccessToken}`,
    },
    body: JSON.stringify({
      replyToken,
      messages: [{ type: 'text', text: text.slice(0, 4900) }],
    }),
  })

  if (!response.ok) {
    const errText = await response.text()
    return { ok: false, error: `LINE reply API error (${response.status}): ${errText}` }
  }
  return { ok: true }
}
