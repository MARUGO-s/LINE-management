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
      location?: string
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
  location?: string
  date: string
  time: string
  durationMin: number
  reason: string
}

type PendingCalendarConfirmation = {
  storage: 'pending_table' | 'line_messages'
  id: string
  conversation_key: string
  source_text?: string | null
  title: string
  location?: string | null
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
  htmlLink?: string
  summary?: string
  description?: string
  location?: string
  attendees?: Array<{ email?: string; displayName?: string }>
  start?: { date?: string; dateTime?: string; timeZone?: string }
  end?: { date?: string; dateTime?: string; timeZone?: string }
}

type CalendarCreateResult =
  | {
      ok: true
      summary: string
      startDate: Date
      endDate: Date
      savedStartRaw?: string
      savedStartTimeZone?: string
      savedEndRaw?: string
      savedEndTimeZone?: string
      eventId?: string
      eventLink?: string
    }
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

type AiPrimaryIntent = 'create_calendar' | 'list_calendar' | 'search_messages' | 'none'

type AiPrimaryIntentResult = {
  intent: AiPrimaryIntent
  confidence: number
  reason: string
}

type RoomReplyPolicy = {
  isEnabled: boolean
  messageSearchEnabled: boolean
  calendarAiAutoCreateEnabled: boolean
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
const AI_PRIMARY_INTENT_MIN_CONFIDENCE = 0.72
const AI_PRIMARY_INTENT_CONFIRM_MIN_CONFIDENCE = 0.60
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
const CALENDAR_CREATE_TIMEZONE = 'Asia/Tokyo'
const STORABLE_LINE_MEDIA_TYPES = new Set<StorableLineMediaType>(['image', 'video', 'audio', 'file'])
const CALENDAR_EVENT_TITLE_KEYWORDS = [
  '試飲会',
  '打ち合わせ',
  '打合せ',
  '会議',
  'ミーティング',
  'meeting',
  'mtg',
  '商談',
  '面談',
  'イベント',
  '予約',
  'アポ',
  'グランドオープン',
  'オープン',
  'ランチ',
  'ディナー',
] as const
const CALENDAR_LOCATION_HINT_KEYWORDS = [
  'marugo',
  'マルゴ',
  'クラウディア',
  '四谷',
  '新宿',
  '新橋',
  '丸の内',
  'オット',
  'こるり',
  'サヴァ',
  'セカンド',
  'ペロタ',
  '東京ドーム',
  'オンライン',
  'google meet',
  'zoom',
  'teams',
  '会議室',
  'ホール',
] as const
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
    const aiAutoCreateEnvRaw = Deno.env.get('CALENDAR_AI_AUTO_CREATE_ENABLED')
    const aiAutoCreateEnabled = aiAutoCreateEnvRaw == null || String(aiAutoCreateEnvRaw).trim() === ''
      ? true
      : parseBooleanEnv(aiAutoCreateEnvRaw)
    const calendarEnvState = loadCalendarEnv()
    const roomNameSyncDone = new Set<string>()
    const roomReplyPolicyCache = new Map<string, RoomReplyPolicy>()
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
        const roomReplyPolicy = await loadRoomReplyPolicy(
          supabase,
          roomId,
          roomReplyPolicyCache,
        )
        const text = String(event.message.text ?? '').trim()
        if (isRoomBotReplyEnabled(roomReplyPolicy)) {
          let forceAiMessageSearch = false
          let forceAiCalendarList = false
          let forceAiCalendarCreate = false

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

          if (!!groqApiKey && text && !isExplicitBotCommandText(text)) {
            const primaryIntent = await extractPrimaryIntentWithGroq(
              text,
              messageRetentionDays,
              calendarEnvState.ok ? calendarEnvState.env.timezone : CALENDAR_CREATE_TIMEZONE,
              groqApiKey,
            )
            if (primaryIntent && primaryIntent.intent !== 'none') {
              if (primaryIntent.confidence >= AI_PRIMARY_INTENT_MIN_CONFIDENCE) {
                forceAiMessageSearch = primaryIntent.intent === 'search_messages'
                forceAiCalendarList = primaryIntent.intent === 'list_calendar'
                forceAiCalendarCreate = primaryIntent.intent === 'create_calendar'
              } else if (primaryIntent.confidence >= AI_PRIMARY_INTENT_CONFIRM_MIN_CONFIDENCE) {
                if (isRoomInteractiveReplyEnabled(roomReplyPolicy)) {
                  const confirmPrompt = buildPrimaryIntentConfirmationPrompt(primaryIntent)
                  if (confirmPrompt) {
                    if (!lineAccessToken) {
                      console.error('LINE_CHANNEL_ACCESS_TOKEN is missing. Cannot reply AI primary-intent confirmation.')
                      continue
                    }
                    if (!replyToken) {
                      console.error('Missing replyToken for AI primary-intent confirmation.')
                      continue
                    }
                    const replyResult = await replyLineMessage(replyToken, confirmPrompt, lineAccessToken)
                    if (!replyResult.ok) {
                      console.error('Failed to reply AI primary-intent confirmation:', replyResult.error)
                    }
                    continue
                  }
                }
              }
            }
          }

          // Fallback: capture explicit single-event announcements even when primary intent misses.
          if (
            !forceAiCalendarCreate &&
            !forceAiCalendarList &&
            !forceAiMessageSearch &&
            calendarEnvState.ok &&
            !!groqApiKey &&
            looksLikeSingleEventAnnouncement(text)
          ) {
            forceAiCalendarCreate = true
          }

          const messageSearchParse = parseMessageSearchCommand(text, messageRetentionDays)
          let messageSearchCommand: MessageSearchCommand | null = null
          let messageSearchError: string | null = null

          if (messageSearchParse.matched) {
            messageSearchCommand = messageSearchParse.command
            messageSearchError = messageSearchParse.error
          } else if (!!groqApiKey && forceAiMessageSearch) {
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
            if (!roomReplyPolicy.messageSearchEnabled) {
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

          if (!commandParse.matched && calendarEnvState.ok && !!groqApiKey && forceAiCalendarList) {
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

          if (calendarEnvState.ok && !!groqApiKey && forceAiCalendarCreate) {
              const aiIntent = await extractCalendarIntentWithGroq(
                text,
                calendarEnvState.env.timezone,
                groqApiKey,
              )
              const resolvedDetails = aiIntent
                ? resolveAiCalendarDetails(text, aiIntent.title, aiIntent.location)
                : null
              const isLikelyMultiEvent = looksLikeMultiEventAnnouncement(text)
              const normalizedAiIntent = aiIntent
                ? {
                    ...aiIntent,
                    title: resolvedDetails?.title ?? aiIntent.title,
                    location: resolvedDetails?.location,
                  }
                : null

              const canAutoCreate =
                aiAutoCreateEnabled &&
                roomReplyPolicy.calendarAiAutoCreateEnabled &&
                normalizedAiIntent &&
                isHighConfidenceAiCalendarIntent(normalizedAiIntent) &&
                resolvedDetails?.titleSource !== 'default' &&
                !isLikelyMultiEvent

              if (canAutoCreate && normalizedAiIntent) {
                const reply = await createCalendarEventReply(
                  {
                    kind: 'create',
                    date: normalizedAiIntent.date,
                    time: normalizedAiIntent.time,
                    durationMin: normalizedAiIntent.durationMin,
                    title: normalizedAiIntent.title,
                    ...(normalizedAiIntent.location ? { location: normalizedAiIntent.location } : {}),
                  },
                  calendarEnvState.env,
                  roomId,
                  userId,
                )
                aiAutoCreateReply = `AI判断で予定を自動登録しました（信頼度 ${Math.round(normalizedAiIntent.confidence * 100)}%）。\n${reply}`
              } else if (normalizedAiIntent && isConfirmableAiCalendarIntent(normalizedAiIntent)) {
                const pendingSaved = await savePendingCalendarConfirmation(
                  supabase,
                  roomId,
                  userId,
                  text,
                  normalizedAiIntent,
                )
                if (pendingSaved) {
                  const basePrompt = buildPendingCalendarConfirmationPrompt(normalizedAiIntent, calendarEnvState.env.timezone)
                  const notices: string[] = []
                  if (!roomReplyPolicy.calendarAiAutoCreateEnabled) {
                    notices.push('このルームの自動登録はOFFなので、確認後に登録します。')
                  } else if (!aiAutoCreateEnabled) {
                    notices.push('自動登録はOFFなので、確認後に登録します。')
                  }
                  if (isLikelyMultiEvent) {
                    notices.push('本文に複数の予定候補があるため、自動登録せず確認後に登録します。')
                  }
                  if (resolvedDetails?.titleSource === 'default') {
                    notices.push('件名を本文から確定できなかったため、確認後に登録します。')
                  }
                  aiAutoCreateReply = notices.length > 0
                    ? `${notices.join('\n')}\n${basePrompt}`
                    : basePrompt
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

async function loadRoomReplyPolicy(
  supabase: ReturnType<typeof createClient>,
  roomId: string,
  cache: Map<string, RoomReplyPolicy>,
): Promise<RoomReplyPolicy> {
  const normalizedRoomId = String(roomId ?? '').trim()
  if (!normalizedRoomId || normalizedRoomId === 'unknown') {
    return { isEnabled: true, messageSearchEnabled: true, calendarAiAutoCreateEnabled: true }
  }
  if (cache.has(normalizedRoomId)) {
    return cache.get(normalizedRoomId) ?? { isEnabled: true, messageSearchEnabled: true, calendarAiAutoCreateEnabled: true }
  }

  try {
    const { data, error } = await supabase
      .from('room_summary_settings')
      .select('is_enabled, message_search_enabled, calendar_ai_auto_create_enabled')
      .eq('room_id', normalizedRoomId)
      .maybeSingle()

    if (error) {
      console.error(`Failed to load room reply policy for ${normalizedRoomId}:`, error.message)
      const fallback = { isEnabled: true, messageSearchEnabled: true, calendarAiAutoCreateEnabled: true }
      cache.set(normalizedRoomId, fallback)
      return fallback
    }

    const policy: RoomReplyPolicy = {
      isEnabled: data?.is_enabled !== false,
      messageSearchEnabled: data?.message_search_enabled !== false,
      calendarAiAutoCreateEnabled: data?.calendar_ai_auto_create_enabled !== false,
    }
    cache.set(normalizedRoomId, policy)
    return policy
  } catch (err) {
    console.error(`Unexpected error while loading room reply policy for ${normalizedRoomId}:`, err)
    const fallback = { isEnabled: true, messageSearchEnabled: true, calendarAiAutoCreateEnabled: true }
    cache.set(normalizedRoomId, fallback)
    return fallback
  }
}

function isRoomInteractiveReplyEnabled(policy: RoomReplyPolicy): boolean {
  return policy.isEnabled && policy.messageSearchEnabled
}

function isRoomBotReplyEnabled(policy: RoomReplyPolicy): boolean {
  return policy.isEnabled
}

function parseMessageSearchCommand(rawText: string, defaultDays: MessageRetentionDays): MessageSearchParseResult {
  const text = normalizeSpaces(rawText)
  if (!text) return { matched: false, command: null, error: null }

  const compact = normalizeForRuleParsing(text).replace(/\s+/g, '')
  const hasExplicitPrefix = /^(会話|トーク|履歴|チャット)(検索|要約|確認)/.test(compact)
  if (!hasExplicitPrefix) {
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

function isExplicitBotCommandText(rawText: string): boolean {
  const compact = normalizeForRuleParsing(rawText).replace(/\s+/g, '')
  if (!compact) return false
  if (/^予定(?:登録|追加|確認|一覧|報告)/.test(compact)) return true
  if (/^(会話|トーク|履歴|チャット)(検索|要約|確認)/.test(compact)) return true
  return false
}

function detectMessageSearchDays(compactText: string): MessageRetentionDays | null {
  if (/(180日|半年|6ヶ月|6か月|六ヶ月)/.test(compactText)) return 180
  if (/(120日|4ヶ月|4か月|四ヶ月)/.test(compactText)) return 120
  if (/(60日|2ヶ月|2か月|二ヶ月)/.test(compactText)) return 60
  return null
}

function detectMessageSearchScope(compactText: string): MessageSearchScope {
  if (/(このルーム|このグループ|当ルーム|当グループ|このトーク|この会話)/.test(compactText)) {
    return 'current_room'
  }
  if (/(全ルーム|他ルーム|他のルーム|別ルーム|別のルーム|全グループ|他グループ|別グループ|別のグループ)/.test(compactText)) {
    return 'all_rooms'
  }
  return 'all_rooms'
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

async function extractPrimaryIntentWithGroq(
  text: string,
  defaultDays: MessageRetentionDays,
  timezone: string,
  groqApiKey: string,
): Promise<AiPrimaryIntentResult | null> {
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
              'あなたはLINEメッセージの一次振り分け用JSON分類器です。',
              'このチャットは店舗運営連絡が中心で、ほとんどはBotが返信不要です。基本方針は「必要時のみ反応・それ以外は none」です。',
              'intent を次の4つから1つだけ返してください: create_calendar, list_calendar, search_messages, none',
              'create_calendar: 予定登録すべき文。未来の日時が明確で、単発の予定として登録意図が明瞭な場合のみ。',
              'list_calendar: カレンダー予定の有無・日時を尋ねる質問（例: 5月の会議はいつ？）。',
              'search_messages: 過去会話ログを検索したい質問（例: 人参の値段の記述ある？）。',
              'none: 上記以外（雑談・業務連絡・周知・依頼・添付共有・反応不要）。',
              '重要ルール:',
              '1) 「質問」ではない業務連絡・周知・案内・提出依頼・在庫/発注/納品/欠品連絡は基本 none。',
              '2) @All を含む全体周知、長文の通達、資料共有（画像/PDF/動画/ファイル）は基本 none。',
              '3) create_calendar は、本文の主目的が「1件の予定告知/設定」である時だけ。会議資料の文脈や提出期限連絡は none。',
              '4) list_calendar は、予定を尋ねる明確な質問語（いつ/ある/ありますか/教えて/確認）を伴う時のみ。',
              '5) search_messages は、会話・履歴・過去発言の検索意図が明確な時のみ。',
              '6) 少しでも迷う場合は none を選び、confidence を低めにする（0.55以下）。',
              '想定される会話パターン（運用実態ベース）:',
              'A) 在庫・発注・納品・欠品・案内・周知・提出依頼・資料共有・シフト調整依頼: none',
              'B) 「明日の会議参加可否連絡お願いします」「会議資料共有」「提出期限は◯日です」: none',
              'C) 「5/10 14:00 試飲会を入れて」「来週火曜17時から打ち合わせ」: create_calendar',
              'D) 「5月の会議はいつ？」「4/20に会議ある？」: list_calendar',
              'E) 「人参の値段の記述ある？」「過去の発注の話を検索して」: search_messages',
              'F) 「@All 共有です」「お疲れ様です」「ありがとうございます」「よろしくお願いします」: none',
              'G) 画像/動画/PDF/ファイル単体投稿や取り消し通知: none',
              'H) 「予約フォーム」「予約ページ」等の文言は、来店予約や会議予定を“登録せよ”という明示意図がなければ none',
              `会話検索の日数指定が曖昧なら ${defaultDays} を想定し、分類だけを行うこと。`,
              `現在時刻基準の解釈タイムゾーンは ${timezone}。`,
              'JSONのみ返してください。説明文やコードブロックは禁止です。',
              '返却JSONスキーマ:',
              '{"intent":"create_calendar|list_calendar|search_messages|none","confidence":number(0-1),"reason":string}',
            ].join('\n'),
          },
          { role: 'user', content: text },
        ],
      }),
    })

    if (!response.ok) {
      const err = await response.text()
      console.error('Groq primary-intent extraction failed:', response.status, err)
      return null
    }

    const json = await response.json()
    const content = String(json?.choices?.[0]?.message?.content ?? '').trim()
    if (!content) return null

    const extracted = parseFirstJsonObject(content)
    if (!extracted || typeof extracted !== 'object') return null

    const raw = extracted as Record<string, unknown>
    const intent = normalizeAiPrimaryIntent(String(raw.intent ?? ''))
    if (!intent) return null

    const confidenceNum = Number(raw.confidence ?? 0)
    const confidence = Number.isFinite(confidenceNum)
      ? Math.max(0, Math.min(1, confidenceNum))
      : 0

    const reason = normalizeInlineText(String(raw.reason ?? ''))
    return { intent, confidence, reason }
  } catch (err) {
    console.error('Failed to extract primary intent with Groq:', err)
    return null
  }
}

function normalizeAiPrimaryIntent(raw: string): AiPrimaryIntent | null {
  const value = String(raw ?? '').trim().toLowerCase()
  if (value === 'create_calendar' || value === 'create') return 'create_calendar'
  if (value === 'list_calendar' || value === 'list') return 'list_calendar'
  if (value === 'search_messages' || value === 'message_search' || value === 'search') return 'search_messages'
  if (value === 'none' || value === 'other' || value === 'unknown') return 'none'
  return null
}

function buildPrimaryIntentConfirmationPrompt(intent: AiPrimaryIntentResult): string | null {
  const score = Math.round(Math.max(0, Math.min(1, intent.confidence)) * 100)
  if (intent.intent === 'create_calendar') {
    return [
      `判断があいまいです（信頼度 ${score}%）。`,
      'このメッセージは「予定登録」で合っていますか？',
      '登録する場合は、次のように送ってください。',
      '例: 予定登録 2026-05-10 14:00 試飲会',
    ].join('\n')
  }
  if (intent.intent === 'list_calendar') {
    return [
      `判断があいまいです（信頼度 ${score}%）。`,
      'このメッセージは「予定確認」で合っていますか？',
      '確認する場合は、次のように送ってください。',
      '例: 予定確認 2026-05-10',
      '例: 予定確認 5月 会議',
    ].join('\n')
  }
  if (intent.intent === 'search_messages') {
    return [
      `判断があいまいです（信頼度 ${score}%）。`,
      'このメッセージは「会話検索」で合っていますか？',
      '検索する場合は、次のように送ってください。',
      '例: 会話検索 人参 いくら',
      '例: 会話検索 120日 発注',
    ].join('\n')
  }
  return null
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
              'scopeが明示されない場合は all_rooms を返してください。',
              '「他のルーム」「全ルーム」「別グループ」等の意図がある場合は all_rooms。',
              '「このルーム」「このグループ」等の意図がある場合は current_room。',
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
  if (value === 'current_room' || value === 'current' || value === 'this_room' || value === 'local' || value === 'room_only') {
    return 'current_room'
  }
  if (value === 'all_rooms' || value === 'all' || value === 'global' || value === 'cross_room') {
    return 'all_rooms'
  }
  return 'all_rooms'
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
    lines.push(`${i + 1}. ${formatMessageSearchPreview(previewRows[i], command.scope === 'all_rooms')}`)
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

function formatMessageSearchPreview(row: SearchMessageRow, includeRoomLabel = false): string {
  const date = formatSearchDateTime(row.created_at)
  const content = normalizeInlineText(String(row.content ?? ''))
  const compact = content.length > 90 ? `${content.slice(0, 90)}...` : (content || '（内容なし）')
  if (includeRoomLabel) {
    const roomLabel = normalizeInlineText(String(row.room_label ?? '')) || '（ルーム不明）'
    return `ルーム:${roomLabel} / ${date} / ${compact}`
  }
  return `${date} ${compact}`
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

function looksLikeSingleEventAnnouncement(text: string): boolean {
  const compact = normalizeForRuleParsing(text).replace(/\s+/g, '')
  if (!compact) return false
  if (looksLikeExplicitCalendarQuestion(compact)) return false
  if (looksLikeMessageSearchQuestion(text)) return false
  if (parseCalendarCommand(text).matched) return false

  const hasDateHint = /(\d{4}[\/.\-]\d{1,2}[\/.\-]\d{1,2}|\d{4}年\d{1,2}月\d{1,2}日|\d{1,2}[\/.\-]\d{1,2}|(?:\d{1,2}月)?\d{1,2}日|今日|明日|明後日|来週|今週|来月|今月)/.test(compact)
  const hasTimeHint = /(\d{1,2}:\d{2}|\d{1,2}時(?:\d{1,2}分)?)/.test(compact)
  const hasEventWord = /(試飲会|会議|打ち合わせ|打合せ|ミーティング|meeting|mtg|イベント|オープン|グランドオープン|講習会|セミナー|説明会|研修)/i.test(compact)
  const hasListIntent = /(予定確認|予定一覧|予定報告|いつ|何件|教えて|確認したい|ありますか|ある？)/.test(compact)
  if (hasListIntent) return false

  return hasDateHint && hasTimeHint && hasEventWord
}

function looksLikeMultiEventAnnouncement(text: string): boolean {
  const normalized = normalizeForRuleParsing(text)
  if (!normalized) return false
  const compact = normalized.replace(/\s+/g, '')
  if (!compact) return false
  if (looksLikeExplicitCalendarQuestion(compact)) return false
  if (looksLikeMessageSearchQuestion(text)) return false

  const hasEventWord = /(試飲会|会議|打ち合わせ|打合せ|ミーティング|meeting|mtg|イベント|オープン|グランドオープン|講習会|セミナー|説明会|研修)/i.test(compact)
  const hasTimeHint = /(\d{1,2}:\d{2}|\d{1,2}時(?:\d{1,2}分)?)/.test(compact)
  if (!hasEventWord || !hasTimeHint) return false

  const datePattern = /(\d{4}[\/.\-年]\d{1,2}[\/.\-月]\d{1,2}日?|\d{1,2}月\d{1,2}日|\d{1,2}[\/.\-]\d{1,2}(?:日)?)/g
  const uniqueDates = new Set<string>()
  let match: RegExpExecArray | null
  while ((match = datePattern.exec(normalized)) !== null) {
    const token = String(match[1] ?? '').trim()
    if (!token) continue
    uniqueDates.add(token)
    if (uniqueDates.size >= 2) return true
  }

  const lineCount = normalized
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0).length
  const looksLikeList = /(お知らせ|一覧|下記|次の通り|以下)/.test(compact)
  return looksLikeList && lineCount >= 5 && uniqueDates.size >= 1
}

function looksLikeCalendarListQuestion(text: string): boolean {
  const compact = normalizeForRuleParsing(text).replace(/\s+/g, '')
  if (!compact) return false
  if (looksLikeAnnouncementText(compact)) return false
  const hasRuleCreateCandidate = extractCalendarCommandsFromText(text).length > 0
  if (hasRuleCreateCandidate && !looksLikeExplicitCalendarQuestion(compact)) return false

  const hasQuestionIntent = /(いつ|何件|ありますか|ある\?|ある？|ある$|教えて|見せて|みせて|知りたい|一覧|どれ|どこ|空き|空いて|表示|表示して|出して|だして|見たい|確認したい)/.test(compact)
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
              'title は予定の中身だけ（短い名詞句）にしてください。日時・場所・案内文（例: ぜひ来てください）は含めないでください。',
              '場所が読み取れる場合は location に入れてください（例: 「marugoで試飲会」→ title=試飲会, location=marugo）。',
              '複数行の案内文でも同様に分離してください（例: 「試飲会お知らせ / 7/15 / クラウディア2 / 2階 / 15:00-17:00」→ title=試飲会, location=クラウディア2 2階）。',
              'ラベル付きでも同様に分離してください（例: 「【日時】6/19 15時〜17時 / 【場所】マルゴ四谷 / 従業員向け試飲会」→ title=試飲会, location=マルゴ四谷）。',
              '「次回会議は6月12日、14:30～15:30にオンライン会議」のような文は title=会議, location=オンライン にしてください。',
              '提出期限など別目的の日付が混在していても、予定本体（会議/試飲会など）の日時を優先して抽出してください。',
              '「〜のご案内」「よろしくお願いします」「皆様ぜひ〜」等の周知文は title に含めないでください。',
              `現在時刻は ${nowText} (${timezone})。相対表現（今日/明日/来週）を絶対日付に変換してください。`,
              '「18日の18時」のように月が未指定で日付だけある場合は、現在月（現在年）として解釈してください。',
              'JSONのみ返してください。説明文やコードブロックは不要です。',
              '返却JSONスキーマ:',
              '{"should_create":boolean,"confidence":number(0-1),"title":string,"location":"string|optional","date":"YYYY-MM-DD","time":"HH:mm","duration_min":number,"reason":string}',
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
    const locationRaw = String(raw.location ?? raw.place ?? raw.venue ?? '').trim()
    const date = String(raw.date ?? '').trim()
    const time = String(raw.time ?? '').trim()
    const durationRaw = Number(raw.duration_min ?? raw.durationMin ?? DEFAULT_DURATION_MIN)
    const durationMin = Number.isFinite(durationRaw) ? Math.round(durationRaw) : DEFAULT_DURATION_MIN
    const reason = String(raw.reason ?? '').trim()
    const location = locationRaw || undefined

    return {
      shouldCreate,
      confidence,
      title,
      ...(location ? { location } : {}),
      date,
      time,
      durationMin,
      reason,
    }
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
  sourceText?: string | null
  title: string
  location?: string | null
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
  sourceText?: string | null
  title: string
  location?: string | null
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
  const sourceTextRaw = String(raw.sourceText ?? raw.source_text ?? '').trim()
  const title = String(raw.title ?? '').trim()
  const locationRaw = String(raw.location ?? '').trim()
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
    sourceText: sourceTextRaw || null,
    title,
    location: locationRaw || null,
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
    .select('id, conversation_key, source_text, title, date, time, duration_min, confidence, reason, expires_at')
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
        source_text: decoded.sourceText ?? null,
        title: decoded.title,
        location: decoded.location ?? null,
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
    source_text: data.source_text ? String(data.source_text) : null,
    title: String(data.title),
    location: null,
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
    sourceText: pending.source_text ?? null,
    title: pending.title,
    location: pending.location ?? null,
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
      sourceText,
      title: cleanCalendarTitle(intent.title),
      location: intent.location ?? null,
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
  if (intent.location) {
    lines.push(`場所: ${intent.location}`)
  }
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

  const resolvedPendingDetails = resolveAiCalendarDetails(
    pending.source_text ?? '',
    pending.title,
    pending.location ?? undefined,
  )
  const resolvedPendingTitle = resolvedPendingDetails.titleSource === 'default'
    ? cleanCalendarTitle(pending.title)
    : resolvedPendingDetails.title
  const command: CalendarCreateCommand = {
    kind: 'create',
    date: pending.date,
    time: pending.time,
    durationMin: pending.duration_min,
    title: resolvedPendingTitle,
    ...(resolvedPendingDetails.location ? { location: resolvedPendingDetails.location } : {}),
  }
  const result = await createCalendarEvent(command, env, roomId, userId)
  if (!result.ok) {
    return `予定登録に失敗しました。${result.error}\n再試行する場合は「はい」、中止する場合は「いいえ」を送ってください。`
  }

  await resolvePendingCalendarConfirmation(supabase, pending, 'confirmed')
  const lines = [
    '確認済みの予定を登録しました。',
    formatDateOnlyForLine(result.startDate, env.timezone),
    formatTimeOnlyForLine(result.startDate, env.timezone),
    cleanCalendarTitle(result.summary),
  ]
  if (command.location) {
    lines.push(`場所: ${command.location}`)
  }
  return lines.join('\n')
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

  const monthDaySlashRangeRegex = /(?:^|[^\d])(\d{1,2})[\/.\-](\d{1,2})(?:日)?(?:\s*[（(][^）)]*[）)])?\s*(?:の|に)?\s*([0-9]{1,2}(?::[0-9]{2}|時(?:\s*[0-9]{1,2}分?)?))\s*(?:から|[-~〜～‐‑‒–—―ー－])\s*([0-9]{1,2}(?::[0-9]{2}|時(?:\s*[0-9]{1,2}分?)?))/
  const monthDaySlashRange = normalized.match(monthDaySlashRangeRegex)
  if (monthDaySlashRange) {
    const month = Number(monthDaySlashRange[1])
    const day = Number(monthDaySlashRange[2])
    const start = parseFlexibleTimeToken(monthDaySlashRange[3])
    const end = parseFlexibleTimeToken(monthDaySlashRange[4])
    if (!start || !end) return null
    return buildSlotFromDateAndTime(currentYear, month, day, start, end)
  }

  const monthDaySlashSingleRegex = /(?:^|[^\d])(\d{1,2})[\/.\-](\d{1,2})(?:日)?(?:\s*[（(][^）)]*[）)])?\s*(?:の|に)?\s*([0-9]{1,2}(?::[0-9]{2}|時(?:\s*[0-9]{1,2}分?)?))(?:\s*から)?/
  const monthDaySlashSingle = normalized.match(monthDaySlashSingleRegex)
  if (monthDaySlashSingle) {
    const month = Number(monthDaySlashSingle[1])
    const day = Number(monthDaySlashSingle[2])
    const start = parseFlexibleTimeToken(monthDaySlashSingle[3])
    if (!start) return null
    return buildSlotFromDateAndTime(currentYear, month, day, start, null)
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

  const monthDayJaLoose = normalized.match(/(\d{1,2})月(\d{1,2})日/)
  if (monthDayJaLoose) {
    const month = Number(monthDayJaLoose[1])
    const day = Number(monthDayJaLoose[2])
    const tail = normalized.slice((monthDayJaLoose.index ?? 0) + monthDayJaLoose[0].length)
    const looseTimeMatch = tail.match(/([0-9]{1,2}(?::[0-9]{2}|時(?:\s*[0-9]{1,2}分?)?))(?:\s*(?:から|[-~〜～‐‑‒–—―ー－])\s*([0-9]{1,2}(?::[0-9]{2}|時(?:\s*[0-9]{1,2}分?)?)))?/)
    if (looseTimeMatch) {
      const start = parseFlexibleTimeToken(looseTimeMatch[1])
      if (!start) return null
      const end = looseTimeMatch[2] ? parseFlexibleTimeToken(looseTimeMatch[2]) : null
      return buildSlotFromDateAndTime(currentYear, month, day, start, end)
    }
  }

  const monthDaySlashLoose = normalized.match(/(?:^|[^\d])(\d{1,2})[\/.\-](\d{1,2})(?:日)?/)
  if (monthDaySlashLoose) {
    const month = Number(monthDaySlashLoose[1])
    const day = Number(monthDaySlashLoose[2])
    const tail = normalized.slice((monthDaySlashLoose.index ?? 0) + monthDaySlashLoose[0].length)
    const looseTimeMatch = tail.match(/([0-9]{1,2}(?::[0-9]{2}|時(?:\s*[0-9]{1,2}分?)?))(?:\s*(?:から|[-~〜～‐‑‒–—―ー－])\s*([0-9]{1,2}(?::[0-9]{2}|時(?:\s*[0-9]{1,2}分?)?)))?/)
    if (looseTimeMatch) {
      const start = parseFlexibleTimeToken(looseTimeMatch[1])
      if (!start) return null
      const end = looseTimeMatch[2] ? parseFlexibleTimeToken(looseTimeMatch[2]) : null
      return buildSlotFromDateAndTime(currentYear, month, day, start, end)
    }
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
  const stripped = stripDateTimePhrases(normalized)
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
  return normalizeEventTitleCandidate(stripped)
}

function stripDateTimePhrases(raw: string): string {
  return String(raw ?? '')
    .replace(
      /(?:日時\s*[::]\s*)?(?:\d{4}[\/.\-年]\d{1,2}[\/.\-月]\d{1,2}日?)(?:\s*[（(][^）)]*[）)])?(?:\s*の)?\s*\d{1,2}(?::\d{2}|時(?:\s*\d{1,2}分?)?)(?:\s*(?:から|より|[-~〜～‐‑‒–—―ー－])\s*\d{1,2}(?::\d{2}|時(?:\s*\d{1,2}分?)?))?/g,
      ' ',
    )
    .replace(
      /(?:今月|来月|再来月|先月|今日|明日|明後日|本日|当日)?(?:の)?\s*\d{1,2}月\d{1,2}日(?:\s*[（(][^）)]*[）)])?(?:\s*の)?\s*\d{1,2}(?::\d{2}|時(?:\s*\d{1,2}分?)?)(?:\s*(?:から|より|[-~〜～‐‑‒–—―ー－])\s*\d{1,2}(?::\d{2}|時(?:\s*\d{1,2}分?)?))?/g,
      ' ',
    )
    .replace(
      /(?:今月|来月|再来月|先月|今日|明日|明後日|本日|当日)?(?:の)?\s*\d{1,2}日(?:\s*[（(][^）)]*[）)])?(?:\s*の)?\s*\d{1,2}(?::\d{2}|時(?:\s*\d{1,2}分?)?)(?:\s*(?:から|より|[-~〜～‐‑‒–—―ー－])\s*\d{1,2}(?::\d{2}|時(?:\s*\d{1,2}分?)?))?/g,
      ' ',
    )
}

function resolveAiCalendarDetails(
  sourceText: string,
  aiTitle: string,
  aiLocation?: string,
): { title: string; titleSource: 'ai' | 'source_derived' | 'default'; location?: string } {
  let title = '予定'
  let titleSource: 'ai' | 'source_derived' | 'default' = 'default'
  const hasSourceText = normalizeKeywordForSearch(sourceText).length > 0

  const normalizedAiTitle = normalizeEventTitleCandidate(aiTitle)
  if (normalizedAiTitle && (!hasSourceText || isTitleGroundedInSource(normalizedAiTitle, sourceText))) {
    title = normalizedAiTitle
    titleSource = 'ai'
  } else {
    const inferredTitle = inferTitleFromLine(sourceText)
    if (inferredTitle) {
      title = inferredTitle
      titleSource = 'source_derived'
    }
  }

  const location = resolveEventLocation(sourceText, aiLocation, title)
  return {
    title,
    titleSource,
    ...(location ? { location } : {}),
  }
}

function normalizeEventTitleCandidate(raw: string): string {
  const normalized = normalizeForRuleParsing(raw)
  const withoutDateTime = stripDateTimePhrases(normalized)
  const firstSentence = withoutDateTime.split(/[。．.!！?？\n]/)[0] ?? withoutDateTime
  const compact = firstSentence
    .replace(/(?:皆様|みなさま|ぜひ|どうぞ|よろしければ|いらしてください|来てください|お越しください|お願いします|お願い致します).*/g, ' ')
    .replace(/(?:開催します|開催予定です|開催予定|開催です|実施します|行います|あります|がある|です)\s*$/g, ' ')
    .replace(/^[\s:：\-]+/, '')
    .replace(/^(?:から|より|に|へ|で)\s*/g, '')
    .replace(/\s+/g, ' ')
    .trim()
  if (!compact) return ''

  const keywordTitle = extractEventKeywordTitle(compact)
  if (keywordTitle) return keywordTitle
  return cleanCalendarTitle(compact)
}

function isTitleGroundedInSource(title: string, sourceText: string): boolean {
  const normalizedTitle = normalizeKeywordForSearch(title)
  const normalizedSource = normalizeKeywordForSearch(sourceText)
  const compactTitle = compactSearchText(title)
  const compactSource = compactSearchText(sourceText)

  if (normalizedTitle && normalizedSource.includes(normalizedTitle)) return true
  if (compactTitle && compactSource.includes(compactTitle)) return true
  return keywordMatchesHaystacks(title, [sourceText])
}

function extractEventKeywordTitle(raw: string): string | null {
  const text = normalizeForRuleParsing(raw)
  for (const keyword of CALENDAR_EVENT_TITLE_KEYWORDS) {
    const escaped = escapeRegExp(keyword)
    const exact = text.match(new RegExp(escaped, 'i'))
    if (exact) return cleanCalendarTitle(exact[0])
  }
  return null
}

function resolveEventLocation(sourceText: string, aiLocation: string | undefined, title: string): string | null {
  const cleanedAiLocation = cleanCalendarLocation(aiLocation ?? '')
  if (cleanedAiLocation && isLocationGroundedInSource(cleanedAiLocation, sourceText)) {
    return cleanedAiLocation
  }

  const virtualLocation = inferVirtualMeetingLocation(sourceText)
  if (virtualLocation) return virtualLocation

  const inferredLocationFromLines = inferLocationFromStructuredLines(sourceText, title)
  if (inferredLocationFromLines) return inferredLocationFromLines

  const inferredLocation = inferLocationFromLine(sourceText, title)
  if (inferredLocation) return inferredLocation
  return null
}

function inferVirtualMeetingLocation(text: string): string | null {
  const normalized = normalizeKeywordForSearch(text)
  if (!normalized) return null
  if (normalized.includes('meet.google.com') || normalized.includes('google meet')) return 'Google Meet'
  if (normalized.includes('zoom')) return 'Zoom'
  if (normalized.includes('teams')) return 'Microsoft Teams'
  if (normalized.includes('オンライン')) return 'オンライン'
  return null
}

function inferLocationFromStructuredLines(sourceText: string, title: string): string | null {
  const lines = normalizeForRuleParsing(sourceText)
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
  if (lines.length === 0) return null

  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i]
    const labeled = line.match(/^(?:[【\[]\s*)?(?:場所|会場|開催場所|開催会場)(?:\s*[】\]])?\s*(?:[：:]\s*)?(.+)$/i)
    if (labeled && labeled[1]) {
      const location = cleanCalendarLocation(labeled[1])
      if (location) return location
    }
    if (/^(?:[【\[]\s*)?(?:場所|会場|開催場所|開催会場)(?:\s*[】\]])?\s*[：:]?\s*$/i.test(line)) {
      const next = lines[i + 1]
      if (next) {
        const location = cleanCalendarLocation(next)
        if (location) return location
      }
    }
  }

  const compactTitle = compactSearchText(title)
  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i]
    if (isLikelyDateOrTimeLine(line)) continue
    if (isLikelyInstructionLine(line)) continue

    const cleaned = cleanCalendarLocation(line)
    if (!cleaned) continue
    const compact = compactSearchText(cleaned)
    if (!compact) continue
    if (compactTitle && compact === compactTitle) continue

    const joinedFloor = buildJoinedFloorLocation(lines, i, cleaned)
    if (joinedFloor) return joinedFloor

    if (hasLocationHint(cleaned)) return cleaned
  }
  return null
}

function inferLocationFromLine(text: string, title: string): string | null {
  const normalized = normalizeForRuleParsing(text)
  const stripped = stripDateTimePhrases(normalized)
  const firstSentence = stripped.split(/[。．.!！?？\n]/)[0] ?? stripped
  if (!firstSentence) return null

  const compactTitle = normalizeForRuleParsing(title).replace(/\s+/g, '')
  if (compactTitle) {
    const escapedTitle = escapeRegExp(compactTitle)
    const compactSentence = normalizeForRuleParsing(firstSentence).replace(/\s+/g, '')
    const m = compactSentence.match(new RegExp(`^(.{1,40}?)で(?:${escapedTitle})`))
    if (m && m[1]) {
      const location = cleanCalendarLocation(m[1])
      if (location) return location
    }
  }

  const m2 = firstSentence.match(/(.{1,40}?)\s*で\s*(?:[^。]*)(?:試飲会|打ち合わせ|打合せ|会議|ミーティング|meeting|mtg|商談|面談|イベント|予約|アポ|グランドオープン|オープン|ランチ|ディナー|セミナー|講習会|説明会|研修)/i)
  if (m2 && m2[1]) {
    const location = cleanCalendarLocation(m2[1])
    if (location) return location
  }
  return null
}

function buildJoinedFloorLocation(lines: string[], index: number, base: string): string | null {
  const nextRaw = lines[index + 1] ? normalizeForRuleParsing(lines[index + 1]).trim() : ''
  if (!nextRaw) return null
  const next = nextRaw.replace(/\s*(?:にて|で)\s*$/i, '').trim()
  if (!/^(?:\d{1,2}階|[Bb]\d{1,2}F|[1-9]\d?F)$/i.test(next)) return null
  if (isLikelyDateOrTimeLine(base) || isLikelyInstructionLine(base)) return null
  const joined = cleanCalendarLocation(`${base} ${next}`)
  return joined
}

function isLikelyDateOrTimeLine(line: string): boolean {
  const normalized = normalizeForRuleParsing(line)
  if (!normalized) return false
  if (/^(?:日時|日程|開催日|開催日時)\s*[：:]/.test(normalized)) return true
  if (parseDateTimeSlotFromLine(normalized)) return true
  if (/^\d{1,2}[\/.\-]\d{1,2}(?:\([^)]+\))?$/.test(normalized)) return true
  if (/^\d{1,2}:\d{2}(?:\s*[-~〜～‐‑‒–—―ー－]\s*\d{1,2}:\d{2})?$/.test(normalized)) return true
  if (/^\d{1,2}時(?:\d{1,2}分?)?(?:\s*[-~〜～‐‑‒–—―ー－]\s*\d{1,2}時(?:\d{1,2}分?)?)?$/.test(normalized)) return true
  return false
}

function isLikelyInstructionLine(line: string): boolean {
  const normalized = normalizeForRuleParsing(line)
  if (!normalized) return true
  if (/^(?:お疲れ様|よろしく|お願いします|お願い致します|お願い申し上げます|ご周知|周知|共有|リマインド|ご案内|案内|参加|ご参加|皆様)/.test(normalized)) {
    return true
  }
  if (/https?:\/\//i.test(normalized)) return true
  return false
}

function hasLocationHint(line: string): boolean {
  const normalized = normalizeKeywordForSearch(line)
  if (!normalized) return false
  if (/(?:\d{1,2}階|[Bb]\d{1,2}f|[1-9]\d?f)$/.test(normalized)) return true
  return CALENDAR_LOCATION_HINT_KEYWORDS.some((keyword) => normalized.includes(normalizeKeywordForSearch(keyword)))
}

function cleanCalendarLocation(raw: string): string | null {
  const normalized = normalizeForRuleParsing(stripDateTimePhrases(raw))
  const firstSentence = normalized.split(/[。．.!！?？\n]/)[0] ?? normalized
  const cleaned = firstSentence
    .replace(/^(?:[【\[]\s*)?(?:場所|会場|開催場所|開催会場)(?:\s*[】\]])?\s*(?:[：:]\s*)?/i, '')
    .replace(/(?:皆様|みなさま|ぜひ|どうぞ|よろしければ|いらしてください|来てください|お越しください|お願いします|お願い致します).*/g, ' ')
    .replace(/(?:開催します|開催予定です|開催予定|開催です|実施します|行います|あります|がある|です)\s*$/g, ' ')
    .replace(/(?:試飲会お知らせ|会議お知らせ|イベントお知らせ|ご案内)\s*$/g, ' ')
    .replace(/\s*(?:にて|で)\s*(?:開催|実施|予定)?\s*$/i, ' ')
    .replace(/^[\s:：\-]+/, '')
    .replace(/^[【\[]+/, '')
    .replace(/[】\]]+$/, '')
    .replace(/^(?:から|より|に|へ|で)\s*/g, '')
    .replace(/\s*(?:で|にて|に|へ)\s*$/g, '')
    .replace(/[\s、,。．]+$/g, '')
    .replace(/^[\s、,。．]+/, '')
    .replace(/\s+/g, ' ')
    .trim()
  if (!cleaned) return null
  const compact = compactSearchText(cleaned)
  if (!compact) return null
  if (compact.length <= 1) return null
  if (extractEventKeywordTitle(cleaned) && !/(会議室|イベントホール|ホール|スタジアム)/.test(cleaned)) return null
  return cleaned.length > 80 ? cleaned.slice(0, 80) : cleaned
}

function isLocationGroundedInSource(location: string, sourceText: string): boolean {
  const normalizedLocation = normalizeKeywordForSearch(location)
  const normalizedSource = normalizeKeywordForSearch(sourceText)
  const compactLocation = compactSearchText(location)
  const compactSource = compactSearchText(sourceText)
  if (normalizedLocation && normalizedSource.includes(normalizedLocation)) return true
  if (compactLocation && compactSource.includes(compactLocation)) return true
  return false
}

function escapeRegExp(value: string): string {
  return String(value ?? '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
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
  const successes: Array<{
    summary: string
    startDate: Date
  }> = []
  const failures: string[] = []

  for (const cmd of targets) {
    const result = await createCalendarEvent(cmd, env, roomId, userId, accessToken)
    if (result.ok) {
      successes.push({
        summary: result.summary,
        startDate: result.startDate,
      })
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
  const ymdSlashDate = canonical.match(/^(\d{4})[\/.-](\d{1,2})[\/.-](\d{1,2})$/)
  if (ymdSlashDate) {
    const year = Number(ymdSlashDate[1])
    const month = Number(ymdSlashDate[2])
    const day = Number(ymdSlashDate[3])
    const date = toIsoDateStringSafe(year, month, day)
    if (date) return { scope: 'date', date }
  }
  const ymdJaDate = canonical.match(/^(\d{4})年(\d{1,2})月(\d{1,2})日$/)
  if (ymdJaDate) {
    const year = Number(ymdJaDate[1])
    const month = Number(ymdJaDate[2])
    const day = Number(ymdJaDate[3])
    const date = toIsoDateStringSafe(year, month, day)
    if (date) return { scope: 'date', date }
  }
  const monthDay = canonical.match(/^(\d{1,2})月(?:の)?(\d{1,2})日$/)
  if (monthDay) {
    const { year: currentYear } = getJstYearMonth()
    const month = Number(monthDay[1])
    const day = Number(monthDay[2])
    const date = toIsoDateStringSafe(currentYear, month, day)
    if (date) return { scope: 'date', date }
  }
  const dayOnly = canonical.match(/^(\d{1,2})日$/)
  if (dayOnly) {
    const { year: currentYear, month: currentMonth } = getJstYearMonth()
    const day = Number(dayOnly[1])
    const date = toIsoDateStringSafe(currentYear, currentMonth, day)
    if (date) return { scope: 'date', date }
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
  const compactNoPunct = compact.replace(/[?？!！。．、,]+/g, '')
  if (!compactNoPunct) return null
  if (/^予定(?:確認|一覧|報告)/.test(compactNoPunct)) return null
  if (looksLikeAnnouncementText(compactNoPunct)) return null
  const hasRuleCreateCandidate = extractCalendarCommandsFromText(rawText).length > 0
  if (hasRuleCreateCandidate && !looksLikeExplicitCalendarQuestion(compactNoPunct)) return null

  const hasQuestionIntent = /(いつ|何件|ありますか|ある\?|ある？|ある$|教えて|見せて|みせて|知りたい|一覧|どれ|どこ|空き|空いて|表示|表示して|出して|だして|見たい|確認したい)/.test(compactNoPunct)
  const hasShortListIntent = /(?:今日|明日|今週|来週|今月|来月|当月|今月中|来月中|今後|これから|直近|近日|近々|向こう30日|30日以内|1ヶ月|1か月|1ヵ月|一ヶ月|\d{1,2}月|\d{4}年\d{1,2}月|\d{4}[\/.-]\d{1,2}|\d{4}年)(?:の)?予定(?:一覧|確認|報告)?(?:だけ)?(?:は|って)?$/.test(compactNoPunct)
  if (!hasQuestionIntent && !hasShortListIntent) return null

  const hasScheduleHint = /(予定|会議|打ち合わせ|打合せ|ミーティング|meeting|mtg|予約|アポ|面談|イベント)/.test(compactNoPunct)
  if (!hasScheduleHint) return null

  const scopeToken = detectRangeToken(compactNoPunct)
  const scope = scopeToken ? parseCalendarListScope(scopeToken) : { scope: 'upcoming_30d' as CalendarListScope }
  if (!scope) return null

  let residue = scopeToken ? compactNoPunct.replace(scopeToken, '') : compactNoPunct
  residue = residue
    .replace(/(?:の)?予定(?:一覧|確認|報告)?/g, ' ')
    // Remove sentence endings first; otherwise "で" stripping can leave trailing "す".
    .replace(/(?:いつ|ありますか|あります|ある|教えて|見せて|みせて|知りたい|確認|一覧|表示|表示して|出して|だして|見たい|確認したい|でしたか|でしょうか|ですか|ますか|すか|です|ます|かな|か)/g, ' ')
    .replace(/(?:は|を|に|で|が|って|とは)/g, ' ')
    .replace(/(?:今後|これから|直近|近日|近々|向こう30日|30日以内|1ヶ月|1か月|1ヵ月|一ヶ月)/g, ' ')
    .replace(/[?？!！。．、,]/g, ' ')
    .replace(/^の+/, ' ')

  if (scope.scope === 'date') {
    residue = residue
      .replace(/(?:\d{4}[\/.-]\d{1,2}[\/.-]\d{1,2}|\d{4}年\d{1,2}月\d{1,2}日|\d{1,2}月(?:の)?\d{1,2}日|\d{1,2}日|\d{1,2}月)/g, ' ')
  }

  const keyword = normalizeKeywordForFilter(residue)
  if (!keyword || isCalendarListStopKeyword(keyword)) {
    return scope
  }
  return { ...scope, keyword }
}

function looksLikeAnnouncementText(compactText: string): boolean {
  if (!compactText) return false
  if (looksLikeExplicitCalendarQuestion(compactText)) return false
  const hasBroadcastMarker = /(@all|各位|周知|共有|協力|お願い致します|お願いいたします|お願いします|よろしくお願いします|よろしくお願いいたします|引き続き)/.test(compactText)
  if (!hasBroadcastMarker) return false
  if (compactText.length >= 60) return true
  return /確認をお願い|共有をお願い|周知をお願い/.test(compactText)
}

function looksLikeExplicitCalendarQuestion(compactText: string): boolean {
  if (!compactText) return false
  if (/^予定(?:確認|一覧|報告)/.test(compactText)) return true

  const hasQuestionIntent = /(いつ|何件|ありますか|ある\?|ある？|ある$|教えて|見せて|みせて|知りたい|一覧|どれ|どこ|空き|空いて|表示|表示して|出して|だして|見たい|確認したい)/.test(compactText)
  if (!hasQuestionIntent) return false

  const hasCalendarHint = /(\d{4}[\/.\-]\d{1,2}|\d{4}年\d{1,2}月|\d{1,2}月|今日|明日|今週|来週|今月|来月|今後|これから|予定|会議|打ち合わせ|打合せ|ミーティング|mtg|meeting|予約|アポ|面談|イベント)/.test(compactText)
  return hasCalendarHint
}

function detectRangeToken(compactText: string): string | null {
  const patterns = [
    /(\d{4}年\d{1,2}月\d{1,2}日)/,
    /(\d{4}[\/.-]\d{1,2}[\/.-]\d{1,2})/,
    /(\d{1,2}月(?:の)?\d{1,2}日)/,
    /(\d{4}年\d{1,2}月)/,
    /(\d{4}[\/.-]\d{1,2})/,
    /(\d{4}年)/,
    /(今後|これから|直近|近日|近々|向こう30日|30日以内|1ヶ月|1か月|1ヵ月|一ヶ月)/,
    /(今月中|来月中|今月|来月|今週|来週|今日|明日|当月)/,
    /(\d{1,2}日)/,
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

function isCalendarListStopKeyword(keyword: string): boolean {
  const normalized = normalizeForRuleParsing(keyword).replace(/\s+/g, '')
  if (!normalized) return true
  return /^(何|なに|何が|何を|何か|どれ|どこ|いつ|何がありますか|何があります|なにがありますか|なにがあります)$/.test(normalized)
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
    ...(command.location ? [`場所: ${command.location}`] : []),
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
  const normalizedStartTime = normalizeTimeToHhMm(command.time)
  if (!normalizedStartTime) {
    return { ok: false, error: '時刻の解釈に失敗しました。' }
  }
  const endLocal = addMinutesToLocalDateTime(command.date, normalizedStartTime, command.durationMin)
  if (!endLocal) {
    return { ok: false, error: '終了時刻の解釈に失敗しました。' }
  }

  const startDate = parseJstDateTime(command.date, command.time)
  if (!startDate) {
    return { ok: false, error: '日時の解釈に失敗しました。' }
  }
  const endDate = new Date(startDate.getTime() + command.durationMin * 60 * 1000)
  const accessToken = providedAccessToken || await fetchGoogleAccessToken(env)
  const startDateTimeLocal = `${command.date}T${normalizedStartTime}:00+09:00`
  const endDateTimeLocal = `${endLocal.date}T${endLocal.time}:00+09:00`

  const calendarPath = `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(env.calendarId)}/events`
  const response = await fetch(calendarPath, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${accessToken}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      summary: command.title,
      ...(command.location ? { location: command.location } : {}),
      description: `LINE room_id: ${roomId}\nLINE user_id: ${userId ?? 'unknown'}\nsource: line-webhook`,
      start: {
        dateTime: startDateTimeLocal,
        timeZone: CALENDAR_CREATE_TIMEZONE,
      },
      end: {
        dateTime: endDateTimeLocal,
        timeZone: CALENDAR_CREATE_TIMEZONE,
      },
    }),
  })

  if (!response.ok) {
    const text = await response.text()
    return { ok: false, error: `Google Calendar API error (${response.status}): ${text}` }
  }

  const finalized = await response.json() as GoogleCalendarEvent
  const summary = String(finalized?.summary ?? command.title)
  return {
    ok: true,
    summary,
    startDate,
    endDate,
    savedStartRaw: finalized?.start?.dateTime ?? finalized?.start?.date,
    savedStartTimeZone: finalized?.start?.timeZone,
    savedEndRaw: finalized?.end?.dateTime ?? finalized?.end?.date,
    savedEndTimeZone: finalized?.end?.timeZone,
    eventId: finalized?.id,
    eventLink: finalized?.htmlLink,
  }
}

function normalizeTimeToHhMm(time: string): string | null {
  if (!isValidTime(time)) return null
  const [hour, minute] = time.split(':').map(Number)
  return `${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}`
}

function addMinutesToLocalDateTime(
  date: string,
  time: string,
  minutes: number,
): { date: string; time: string } | null {
  if (!isValidDate(date) || !isValidTime(time)) return null
  const [year, month, day] = date.split('-').map(Number)
  const [hour, minute] = time.split(':').map(Number)
  const base = new Date(Date.UTC(year, month - 1, day, hour, minute, 0, 0))
  const next = new Date(base.getTime() + minutes * 60 * 1000)
  const yy = String(next.getUTCFullYear()).padStart(4, '0')
  const mm = String(next.getUTCMonth() + 1).padStart(2, '0')
  const dd = String(next.getUTCDate()).padStart(2, '0')
  const hh = String(next.getUTCHours()).padStart(2, '0')
  const min = String(next.getUTCMinutes()).padStart(2, '0')
  return { date: `${yy}-${mm}-${dd}`, time: `${hh}:${min}` }
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

function toIsoDateStringSafe(year: number, month: number, day: number): string | null {
  if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) return null
  const iso = `${String(year).padStart(4, '0')}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`
  return isValidDate(iso) ? iso : null
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
