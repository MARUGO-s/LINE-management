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
      kind: 'update'
      eventId: string
      title?: string
      date?: string
      time?: string
      durationMin?: number
      location?: string
      clearLocation?: boolean
      description?: string
      clearDescription?: boolean
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
type CalendarUpdateCommand = Extract<CalendarCommand, { kind: 'update' }>

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

type PendingCalendarUpdateTargetEntry = {
  event_id: string
  summary: string
  date?: string
  time?: string
}

type PendingCalendarUpdateContext = {
  id: string
  conversation_key: string
  target_events: PendingCalendarUpdateTargetEntry[]
  source_line_message_ids: string[]
  expires_at: string
}

type PendingLibrarySearchConfirmation = {
  id: string
  conversation_key: string
  keyword: string
  search_days: MessageRetentionDays
  search_scope: MessageSearchScope
  retention_adjusted: boolean
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

type AiCalendarUpdateIntent = {
  shouldUpdate: boolean
  confidence: number
  title?: string
  date?: string
  time?: string
  durationMin?: number
  location?: string
  clearLocation?: boolean
  description?: string
  clearDescription?: boolean
  reason: string
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

type MessageRetentionDays = 0 | 60 | 120 | 180 | 365 | 730 | 1095

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
  roomName: string | null
  requiresRegistration: boolean
  isEnabled: boolean
  botReplyEnabled: boolean
  messageSearchEnabled: boolean
  messageSearchLibraryEnabled: boolean
  mediaFileAccessEnabled: boolean
  calendarAiAutoCreateEnabled: boolean
  calendarSilentAutoRegisterEnabled: boolean
}

type LineUserPermissionPolicy = {
  isActive: boolean
  canMessageSearch: boolean
  canLibrarySearch: boolean
  canCalendarCreate: boolean
  canCalendarUpdate: boolean
  canMediaAccess: boolean
}

type CalendarSourceMeta = {
  roomName: string | null
  userName: string | null
}

type SearchMessageRow = {
  room_id: string
  room_label?: string | null
  content: string
  created_at: string
  user_id: string | null
}
type SearchDocumentRow = {
  room_id: string | null
  room_label?: string | null
  original_file_name: string
  mime_type: string
  extracted_text: string
  created_at: string
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
const AI_UPDATE_MIN_CONFIDENCE = 0.62
const AI_PRIMARY_INTENT_MIN_CONFIDENCE_STRONG_CREATE = 0.68
const AI_PRIMARY_INTENT_CONFIRM_MIN_CONFIDENCE_STRONG_CREATE = 0.56
const AI_CREATE_MIN_CONFIDENCE_STRONG_CREATE = 0.76
const AI_CREATE_CONFIRM_MIN_CONFIDENCE_STRONG_CREATE = 0.62
const AI_AUTO_CREATE_MAX_EVENTS = 5
const PAST_EVENT_GRACE_MS = 5 * 60 * 1000
const PENDING_CONFIRMATION_TTL_MIN = 30
const CALENDAR_PENDING_CONFIRMATION_TTL_MIN = 5
const CALENDAR_PENDING_TABLE = 'calendar_pending_confirmations'
const CALENDAR_UPDATE_PENDING_TABLE = 'calendar_update_pending_targets'
const LIBRARY_SEARCH_PENDING_TABLE = 'message_search_library_pending_confirmations'
const LINE_DOCUMENT_LIBRARY_FILENAME_MARKER = '[LINE]'
const LEGACY_PENDING_PREFIX = '[[CAL_PENDING]]'
const LEGACY_PENDING_DONE_PREFIX = '[[CAL_PENDING_DONE]]'
const DEFAULT_MESSAGE_RETENTION_DAYS: MessageRetentionDays = 365
const SEARCH_MAX_FETCH_ROWS = 800
const SEARCH_MAX_SUMMARY_ROWS = 120
const SEARCH_AI_SUMMARY_MAX_HITS = 80
const SEARCH_MAX_DOCUMENT_ROWS = 300
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
    const lineUserPermissionCache = new Map<string, LineUserPermissionPolicy>()
    const lineUserSeededCache = new Set<string>()
    const messageRetentionDays = await loadMessageRetentionDays(supabase)
    const mediaUploadMaxBytes = await loadMediaUploadMaxBytes(supabase)

    for (const event of events) {
      const source = event.source || {}
      const sourceType = String(source?.type ?? '').trim().toLowerCase()
      const userId = source.userId ? String(source.userId) : null
      await ensureLineUserPermissionSeed(
        supabase,
        lineAccessToken,
        source,
        userId,
        lineUserSeededCache,
      )
      if (event.type !== 'message') continue

      // Determine room/group ID or user ID as fallback
      const isDirectUserChat = sourceType === 'user'
      const roomId = String(source.groupId || source.roomId || source.userId || 'unknown')
      const replyToken = String(event.replyToken ?? '')
      let aiAutoCreateReply: string | null = null
      let senderDisplayName: string | null = null

      if (!roomNameSyncDone.has(roomId)) {
        roomNameSyncDone.add(roomId)
        await syncRoomDisplayNameIfMissing(supabase, lineAccessToken, source, roomId)
      }
      let roomReplyPolicy = await loadRoomReplyPolicy(
        supabase,
        roomId,
        roomReplyPolicyCache,
      )
      if (isDirectUserChat) {
        roomReplyPolicy = buildDirectUserRoomPolicy(roomReplyPolicy)
      }
      const lineUserPermission = await loadLineUserPermissionPolicy(
        supabase,
        userId,
        lineUserPermissionCache,
      )
      const shouldPersistMessage = shouldPersistLineMessage(source, event.message)
      const storableMediaType = normalizeStorableLineMediaType(event.message?.type)
      const canUseMedia = roomReplyPolicy.mediaFileAccessEnabled && lineUserPermission.canMediaAccess && lineUserPermission.isActive
      const shouldStoreMediaFile = !!storableMediaType && canUseMedia

      if (event.message?.type === 'text') {
        if (lineAccessToken && !senderDisplayName) {
          senderDisplayName = await fetchLineMessageSenderDisplayName(source, lineAccessToken)
        }
        const calendarSourceMeta: CalendarSourceMeta = {
          roomName: roomReplyPolicy.roomName,
          userName: senderDisplayName,
        }
        const roomCanReply = shouldSendRoomReply(roomReplyPolicy)
        const userIsActive = lineUserPermission.isActive
        const canMessageSearch = userIsActive && roomReplyPolicy.messageSearchEnabled && lineUserPermission.canMessageSearch
        const canLibrarySearch = userIsActive && roomReplyPolicy.messageSearchLibraryEnabled && lineUserPermission.canLibrarySearch
        const canCalendarCreate = userIsActive && roomReplyPolicy.calendarAiAutoCreateEnabled && lineUserPermission.canCalendarCreate
        const canCalendarUpdate = userIsActive && lineUserPermission.canCalendarUpdate
        const text = String(event.message.text ?? '').trim()
        const quotedMessageId = extractQuotedLineMessageId(event.message)
        if (!userIsActive) {
          if (roomCanReply && lineAccessToken && replyToken) {
            await replyLineMessage(
              replyToken,
              'このアカウントは現在Bot利用権限がないため、実行できません。',
              lineAccessToken,
            )
          }
          continue
        }
        if (!isDirectUserChat && roomReplyPolicy.requiresRegistration && roomCanReply) {
          if (!lineAccessToken) {
            console.error('LINE_CHANNEL_ACCESS_TOKEN is missing. Cannot reply room registration guidance.')
            continue
          }
          if (!replyToken) {
            console.error('Missing replyToken for room registration guidance.')
            continue
          }
          const replyResult = await replyLineMessage(
            replyToken,
            buildRoomRegistrationRequiredReply(roomReplyPolicy.roomName),
            lineAccessToken,
          )
          if (!replyResult.ok) {
            console.error('Failed to reply room registration guidance:', replyResult.error)
          }
          continue
        }
        const capabilityStatusReply = isDirectUserChat
          ? null
          : (roomCanReply ? buildRoomCapabilityStatusReply(roomReplyPolicy, text) : null)
        if (capabilityStatusReply) {
          if (!lineAccessToken) {
            console.error('LINE_CHANNEL_ACCESS_TOKEN is missing. Cannot reply room capability status.')
            continue
          }
          if (!replyToken) {
            console.error('Missing replyToken for room capability status.')
            continue
          }
          const replyResult = await replyLineMessage(replyToken, capabilityStatusReply, lineAccessToken)
          if (!replyResult.ok) {
            console.error('Failed to reply room capability status:', replyResult.error)
          }
          continue
        }
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
              calendarSourceMeta,
            )
            if (confirmationReply) {
              if (!roomCanReply) {
                continue
              }
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
              } else if (
                calendarEnvState.ok &&
                confirmationReply.includes('確認済みの予定を登録しました。') &&
                replyResult.sentMessageIds.length > 0
              ) {
                await attachLineMessageIdsToPendingCalendarUpdateContext(
                  supabase,
                  roomId,
                  userId,
                  replyResult.sentMessageIds,
                )
              }
              continue
            }
          }

          if (calendarEnvState.ok && canCalendarUpdate) {
            const updateConversationReply = await tryHandlePendingCalendarUpdateConversation(
              text,
              supabase,
              calendarEnvState.env,
              roomId,
              userId,
              groqApiKey,
              quotedMessageId,
            )
            if (updateConversationReply) {
              if (!roomCanReply) {
                continue
              }
              if (!lineAccessToken) {
                console.error('LINE_CHANNEL_ACCESS_TOKEN is missing. Cannot reply calendar update conversation.')
                continue
              }
              if (!replyToken) {
                console.error('Missing replyToken for calendar update conversation.')
                continue
              }
              const replyResult = await replyLineMessage(replyToken, updateConversationReply, lineAccessToken)
              if (!replyResult.ok) {
                console.error('Failed to reply calendar update conversation:', replyResult.error)
              }
              continue
            }
            if (looksLikeCalendarUpdateConversationText(text)) {
              aiAutoCreateReply = [
                '予定変更の対象を特定できませんでした。',
                '先に「予定確認」で候補を表示してから、対象メッセージに返信して変更してください。',
                '例: 「1件目の時間を19:00に変更」「先ほどの予定を5月7日に戻して」',
              ].join('\n')
            }
          }

          if (canLibrarySearch) {
            const librarySearchReply = await tryHandlePendingLibrarySearchConfirmation(
              text,
              supabase,
              roomId,
              userId,
            )
            if (librarySearchReply) {
              if (!roomCanReply) {
                continue
              }
              if (!lineAccessToken) {
                console.error('LINE_CHANNEL_ACCESS_TOKEN is missing. Cannot reply library search confirmation.')
                continue
              }
              if (!replyToken) {
                console.error('Missing replyToken for library search confirmation.')
                continue
              }
              const replyResult = await replyLineMessage(replyToken, librarySearchReply, lineAccessToken)
              if (!replyResult.ok) {
                console.error('Failed to reply library search confirmation:', replyResult.error)
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
              const isOperationalCoordination = looksLikeOperationalCoordinationText(text)
              const strongCreateCue = looksLikeStrongCalendarCreateIntent(text)
              const primaryMinConfidence = strongCreateCue
                ? AI_PRIMARY_INTENT_MIN_CONFIDENCE_STRONG_CREATE
                : AI_PRIMARY_INTENT_MIN_CONFIDENCE
              const primaryConfirmMinConfidence = strongCreateCue
                ? AI_PRIMARY_INTENT_CONFIRM_MIN_CONFIDENCE_STRONG_CREATE
                : AI_PRIMARY_INTENT_CONFIRM_MIN_CONFIDENCE
              if (primaryIntent.confidence >= primaryMinConfidence) {
                forceAiMessageSearch = primaryIntent.intent === 'search_messages'
                forceAiCalendarList = primaryIntent.intent === 'list_calendar' && canCalendarUpdate
                forceAiCalendarCreate = primaryIntent.intent === 'create_calendar' && !isOperationalCoordination
              } else if (primaryIntent.confidence >= primaryConfirmMinConfidence) {
                if (isRoomInteractiveReplyEnabled(roomReplyPolicy)) {
                  const confirmPrompt = buildPrimaryIntentConfirmationPrompt(primaryIntent)
                  if (confirmPrompt) {
                    if (!roomCanReply) {
                      continue
                    }
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

          if (
            !forceAiMessageSearch &&
            !forceAiCalendarCreate &&
            !forceAiCalendarList &&
            looksLikeMessageSearchQuestion(text)
          ) {
            forceAiMessageSearch = true
          }

          // Fallback: capture explicit single-event announcements even when primary intent misses.
          if (
            !forceAiCalendarCreate &&
            !forceAiCalendarList &&
            !forceAiMessageSearch &&
            calendarEnvState.ok &&
            !!groqApiKey &&
            looksLikeSingleEventAnnouncement(text) &&
            !looksLikeOperationalCoordinationText(text)
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
            if (!roomCanReply) {
              continue
            }

            if (!canMessageSearch) {
              if (!lineAccessToken) {
                console.error('LINE_CHANNEL_ACCESS_TOKEN is missing. Cannot reply message search permission status.')
                continue
              }
              if (!replyToken) {
                console.error('Missing replyToken for message search permission status.')
                continue
              }
              const permissionReply = messageSearchCommand && canLibrarySearch
                ? await buildLibrarySearchPromptWhenMessageSearchDisabled(
                  supabase,
                  roomId,
                  userId,
                  messageSearchCommand,
                  messageRetentionDays,
                )
                : (messageSearchError || 'この質問は、現在このルームで権限が付与されていないため実行できません。')
              const permissionReplyResult = await replyLineMessage(replyToken, permissionReply, lineAccessToken)
              if (!permissionReplyResult.ok) {
                console.error('Failed to reply message search permission status:', permissionReplyResult.error)
              }
              continue
            }

            const replyMessages = await buildMessageSearchReply(
              messageSearchCommand,
              messageSearchError,
              supabase,
              roomId,
              userId,
              canLibrarySearch,
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
            const replyResult = await replyLineMessage(replyToken, replyMessages, lineAccessToken)
            if (!replyResult.ok) {
              console.error('Failed to reply message search:', replyResult.error)
            }
            continue
          }

          const commandParse = parseCalendarCommand(text)
          if (commandParse.matched) {
            const command = commandParse.command
            const calendarPermissionDenied =
              (command?.kind === 'create' && !canCalendarCreate) ||
              (command?.kind === 'update' && !canCalendarUpdate) ||
              (command?.kind === 'list' && !canCalendarUpdate)
            if (calendarPermissionDenied) {
              if (!roomCanReply) continue
              if (!lineAccessToken) {
                console.error('LINE_CHANNEL_ACCESS_TOKEN is missing. Cannot reply calendar permission status.')
                continue
              }
              if (!replyToken) {
                console.error('Missing replyToken for calendar permission status.')
                continue
              }
              const deniedReplyResult = await replyLineMessage(
                replyToken,
                'この質問は、現在このルームで権限が付与されていないため実行できません。',
                lineAccessToken,
              )
              if (!deniedReplyResult.ok) {
                console.error('Failed to reply calendar permission status:', deniedReplyResult.error)
              }
              continue
            }
            const replyMessage = await buildCalendarReplyMessage(
              commandParse,
              calendarEnvState,
              supabase,
              roomId,
              userId,
              calendarSourceMeta,
            )
            if (!roomCanReply) {
              continue
            }

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
            } else if (commandParse.command?.kind === 'list' && replyResult.sentMessageIds.length > 0) {
              await attachLineMessageIdsToPendingCalendarUpdateContext(
                supabase,
                roomId,
                userId,
                replyResult.sentMessageIds,
              )
            }
            continue
          }

          if (!commandParse.matched && calendarEnvState.ok && !!groqApiKey && forceAiCalendarList && canCalendarUpdate) {
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
              if (!roomCanReply) {
                continue
              }
              const replyMessage = await listCalendarEventsReply(
                aiCommand,
                calendarEnvState.env,
                supabase,
                roomId,
                userId,
              )

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
              } else if (replyResult.sentMessageIds.length > 0) {
                await attachLineMessageIdsToPendingCalendarUpdateContext(
                  supabase,
                  roomId,
                  userId,
                  replyResult.sentMessageIds,
                )
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

              const strongCreateCue = looksLikeStrongCalendarCreateIntent(text)
              const createMinConfidence = strongCreateCue
                ? AI_CREATE_MIN_CONFIDENCE_STRONG_CREATE
                : AI_MIN_CONFIDENCE
              const createConfirmMinConfidence = strongCreateCue
                ? AI_CREATE_CONFIRM_MIN_CONFIDENCE_STRONG_CREATE
                : AI_CONFIRMATION_MIN_CONFIDENCE
              const canAutoCreate =
                aiAutoCreateEnabled &&
                canCalendarCreate &&
                normalizedAiIntent &&
                isHighConfidenceAiCalendarIntent(normalizedAiIntent, createMinConfidence) &&
                resolvedDetails?.titleSource !== 'default' &&
                !isLikelyMultiEvent
              const shouldSilentAutoCreateHighConfidence =
                canCalendarCreate &&
                roomReplyPolicy.calendarSilentAutoRegisterEnabled &&
                normalizedAiIntent &&
                isHighConfidenceAiCalendarIntent(normalizedAiIntent, createMinConfidence) &&
                resolvedDetails?.titleSource !== 'default' &&
                !isLikelyMultiEvent
              const shouldSilentAutoCreateProvisional =
                canCalendarCreate &&
                roomReplyPolicy.calendarSilentAutoRegisterEnabled &&
                normalizedAiIntent &&
                !shouldSilentAutoCreateHighConfidence &&
                isConfirmableAiCalendarIntent(normalizedAiIntent, createConfirmMinConfidence)
              const shouldAutoCreateWithoutReply =
                canCalendarCreate &&
                !roomReplyPolicy.calendarSilentAutoRegisterEnabled &&
                !roomCanReply &&
                normalizedAiIntent &&
                isConfirmableAiCalendarIntent(normalizedAiIntent, createConfirmMinConfidence)

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
                  calendarSourceMeta,
                )
                if (roomCanReply) {
                  aiAutoCreateReply = `AI判断で予定を自動登録しました（信頼度 ${Math.round(normalizedAiIntent.confidence * 100)}%）。\n${reply}`
                }
              } else if (shouldSilentAutoCreateHighConfidence && normalizedAiIntent) {
                const silentCommand: CalendarCreateCommand = {
                  kind: 'create',
                  date: normalizedAiIntent.date,
                  time: normalizedAiIntent.time,
                  durationMin: normalizedAiIntent.durationMin,
                  title: normalizedAiIntent.title,
                  ...(normalizedAiIntent.location ? { location: normalizedAiIntent.location } : {}),
                }
                const silentResult = await createCalendarEvent(
                  silentCommand,
                  calendarEnvState.env,
                  roomId,
                  userId,
                  undefined,
                  calendarSourceMeta,
                )
                if (!silentResult.ok) {
                  console.error('Silent high-confidence auto-create failed:', silentResult.error)
                }
              } else if (shouldSilentAutoCreateProvisional && normalizedAiIntent) {
                const silentCommand: CalendarCreateCommand = {
                  kind: 'create',
                  date: normalizedAiIntent.date,
                  time: normalizedAiIntent.time,
                  durationMin: normalizedAiIntent.durationMin,
                  title: appendProvisionalSuffixToTitle(normalizedAiIntent.title),
                  ...(normalizedAiIntent.location ? { location: normalizedAiIntent.location } : {}),
                }
                const silentResult = await createCalendarEvent(
                  silentCommand,
                  calendarEnvState.env,
                  roomId,
                  userId,
                  undefined,
                  calendarSourceMeta,
                )
                if (!silentResult.ok) {
                  console.error('Silent provisional auto-create failed:', silentResult.error)
                }
              } else if (shouldAutoCreateWithoutReply && normalizedAiIntent) {
                const silentCommand: CalendarCreateCommand = {
                  kind: 'create',
                  date: normalizedAiIntent.date,
                  time: normalizedAiIntent.time,
                  durationMin: normalizedAiIntent.durationMin,
                  title: normalizedAiIntent.title,
                  ...(normalizedAiIntent.location ? { location: normalizedAiIntent.location } : {}),
                }
                const silentResult = await createCalendarEvent(
                  silentCommand,
                  calendarEnvState.env,
                  roomId,
                  userId,
                  undefined,
                  calendarSourceMeta,
                )
                if (!silentResult.ok) {
                  console.error('Silent auto-create failed:', silentResult.error)
                }
              } else if (roomCanReply && canCalendarCreate && normalizedAiIntent && isConfirmableAiCalendarIntent(normalizedAiIntent, createConfirmMinConfidence)) {
                const pendingSaved = await savePendingCalendarConfirmation(
                  supabase,
                  roomId,
                  userId,
                  text,
                  normalizedAiIntent,
                )
                if (pendingSaved) {
                  const basePrompt = buildPendingCalendarConfirmationPrompt(normalizedAiIntent, calendarEnvState.env.timezone)
                  if (roomCanReply) {
                    aiAutoCreateReply = basePrompt
                  }
                } else {
                  if (roomCanReply) {
                    aiAutoCreateReply = '予定候補を解釈しましたが、確認待ちの保存に失敗しました。もう一度送ってください。'
                  }
                }
              }
          }

          if (
            !aiAutoCreateReply &&
            roomCanReply &&
            canMessageSearch &&
            shouldOfferMessageSearchGuidance(text)
          ) {
            aiAutoCreateReply = buildMessageSearchGuidanceReply(text)
          }
          if (
            !aiAutoCreateReply &&
            roomCanReply &&
            shouldOfferUnknownIntentFallback(text)
          ) {
            aiAutoCreateReply = buildUnknownIntentReply()
          }
        }
      }

      // Parse content based on message type
      const content = toStoredMessageContent(event.message, shouldStoreMediaFile)
      if (shouldPersistMessage) {
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
          if (savedMessageId && shouldStoreMediaFile) {
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
      }

      if (!aiAutoCreateReply && isDirectUserChat) {
        aiAutoCreateReply = buildDirectUserFallbackReply(event.message)
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

function toStoredMessageContent(message: any, includeMediaTag = true): string {
  if (!message || typeof message !== 'object') {
    return '【不明なメッセージが送信されました】'
  }

  const mediaTag = includeMediaTag ? buildLineMediaTag(message?.id) : ''

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

function extractQuotedLineMessageId(message: any): string | null {
  if (!message || typeof message !== 'object') return null
  const candidates = [
    (message as any).quotedMessageId,
    (message as any).quotedMessageID,
    (message as any).quoteMessageId,
    (message as any).quote?.messageId,
    (message as any).quotedMessage?.id,
  ]
  for (const candidate of candidates) {
    const id = String(candidate ?? '').trim()
    if (id) return id
  }
  return null
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
      is_enabled: false,
      bot_reply_enabled: false,
      message_search_enabled: false,
      message_search_library_enabled: false,
      media_file_access_enabled: false,
      calendar_ai_auto_create_enabled: false,
      send_room_summary: false,
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

async function fetchLineMessageSenderDisplayName(source: any, lineAccessToken: string): Promise<string | null> {
  const sourceType = String(source?.type ?? '').trim().toLowerCase()
  const userId = String(source?.userId ?? '').trim()
  if (!userId) return null
  if (!lineAccessToken) return null

  if (sourceType === 'group') {
    const groupId = String(source?.groupId ?? '').trim()
    if (!groupId) return null
    const profile = await fetchLineJson(
      `https://api.line.me/v2/bot/group/${encodeURIComponent(groupId)}/member/${encodeURIComponent(userId)}`,
      lineAccessToken,
    )
    return normalizeDisplayName(profile?.displayName)
  }

  if (sourceType === 'room') {
    const roomId = String(source?.roomId ?? '').trim()
    if (!roomId) return null
    const profile = await fetchLineJson(
      `https://api.line.me/v2/bot/room/${encodeURIComponent(roomId)}/member/${encodeURIComponent(userId)}`,
      lineAccessToken,
    )
    return normalizeDisplayName(profile?.displayName)
  }

  if (sourceType === 'user') {
    const profile = await fetchLineJson(
      `https://api.line.me/v2/bot/profile/${encodeURIComponent(userId)}`,
      lineAccessToken,
    )
    return normalizeDisplayName(profile?.displayName)
  }

  return null
}

async function ensureLineUserPermissionSeed(
  supabase: ReturnType<typeof createClient>,
  lineAccessToken: string,
  source: any,
  lineUserId: string | null,
  cache: Set<string>,
): Promise<void> {
  const normalizedUserId = String(lineUserId ?? '').trim()
  if (!normalizedUserId) return
  if (cache.has(normalizedUserId)) return
  cache.add(normalizedUserId)

  let displayName: string | null = null
  if (lineAccessToken) {
    displayName = await fetchLineMessageSenderDisplayName(
      { ...source, userId: normalizedUserId },
      lineAccessToken,
    )
    if (!displayName) {
      displayName = await fetchLineConversationDisplayName(
        { ...source, type: 'user', userId: normalizedUserId },
        lineAccessToken,
      )
    }
  }

  const { data: existing, error: existingError } = await supabase
    .from('line_user_permissions')
    .select('line_user_id, display_name')
    .eq('line_user_id', normalizedUserId)
    .maybeSingle()
  if (existingError) {
    console.error(`Failed to inspect line_user_permissions for ${normalizedUserId}:`, existingError.message)
    return
  }

  if (existing?.line_user_id) {
    if (!existing.display_name && displayName) {
      const { error: updateError } = await supabase
        .from('line_user_permissions')
        .update({
          display_name: displayName,
          updated_at: new Date().toISOString(),
        })
        .eq('line_user_id', normalizedUserId)
      if (updateError) {
        console.error(`Failed to backfill display_name for ${normalizedUserId}:`, updateError.message)
      }
    }
    return
  }

  const { error: insertError } = await supabase
    .from('line_user_permissions')
    .insert({
      line_user_id: normalizedUserId,
      display_name: displayName,
      // New users start in pending approval state.
      is_active: false,
      can_message_search: false,
      can_library_search: false,
      can_calendar_create: false,
      can_calendar_update: false,
      can_media_access: false,
      note: '承認待ち',
      updated_at: new Date().toISOString(),
    })
  if (insertError) {
    const code = String((insertError as any)?.code ?? '')
    if (code !== '23505') {
      console.error(`Failed to create line_user_permissions for ${normalizedUserId}:`, insertError.message)
    }
  }
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

function shouldPersistLineMessage(source: any, message: any): boolean {
  const sourceType = String(source?.type ?? '').trim().toLowerCase()
  const senderUserId = String(source?.userId ?? '').trim()
  const botUserId = String(Deno.env.get('LINE_BOT_USER_ID') ?? '').trim()
  // 1:1 chat (source.type=user) is excluded from history storage.
  if (sourceType === 'user') return false
  // Messages without sender user id are treated as non-user-origin and are not persisted.
  if (!senderUserId) return false
  // Optional hard guard: when bot user id is configured, never persist bot-origin messages.
  if (botUserId && senderUserId === botUserId) return false
  return true
}

function buildDirectUserRoomPolicy(base: RoomReplyPolicy): RoomReplyPolicy {
  return {
    ...base,
    requiresRegistration: false,
    isEnabled: true,
    botReplyEnabled: true,
    messageSearchEnabled: true,
    messageSearchLibraryEnabled: true,
    mediaFileAccessEnabled: false,
    calendarAiAutoCreateEnabled: true,
    calendarSilentAutoRegisterEnabled: false,
  }
}

function buildDirectUserFallbackReply(message: any): string {
  const type = String(message?.type ?? '').trim().toLowerCase()
  if (type !== 'text') {
    return 'メッセージありがとうございます。内容を正確に解釈するため、テキストで送ってください。'
  }
  return [
    'うまく意図を解釈できませんでした。',
    '会話検索なら「会話検索 キーワード」、予定確認なら「予定確認 5月」、予定変更なら「1件目の時間を19:00に変更」の形式で送ってください。',
  ].join('\n')
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
  if (days === 0 || days === 60 || days === 120 || days === 180 || days === 365 || days === 730 || days === 1095) {
    return days
  }
  return DEFAULT_MESSAGE_RETENTION_DAYS
}

async function loadRoomReplyPolicy(
  supabase: ReturnType<typeof createClient>,
  roomId: string,
  cache: Map<string, RoomReplyPolicy>,
): Promise<RoomReplyPolicy> {
  const fallbackPolicy = {
    roomName: null,
    requiresRegistration: true,
    isEnabled: false,
    botReplyEnabled: false,
    messageSearchEnabled: false,
    messageSearchLibraryEnabled: false,
    mediaFileAccessEnabled: false,
    calendarAiAutoCreateEnabled: false,
    calendarSilentAutoRegisterEnabled: false,
  } satisfies RoomReplyPolicy
  const normalizedRoomId = String(roomId ?? '').trim()
  if (!normalizedRoomId || normalizedRoomId === 'unknown') {
    return fallbackPolicy
  }
  if (cache.has(normalizedRoomId)) {
    return cache.get(normalizedRoomId) ?? fallbackPolicy
  }

  try {
    const { data, error } = await supabase
      .from('room_summary_settings')
      .select('room_name, is_enabled, bot_reply_enabled, message_search_enabled, message_search_library_enabled, media_file_access_enabled, calendar_ai_auto_create_enabled, calendar_silent_auto_register_enabled')
      .eq('room_id', normalizedRoomId)
      .maybeSingle()

    if (error) {
      console.error(`Failed to load room reply policy for ${normalizedRoomId}:`, error.message)
      cache.set(normalizedRoomId, fallbackPolicy)
      return fallbackPolicy
    }

    if (!data) {
      cache.set(normalizedRoomId, fallbackPolicy)
      return fallbackPolicy
    }

    const isEnabled = data?.is_enabled !== false
    const botReplyEnabled = data?.bot_reply_enabled !== false
    const messageSearchEnabled = data?.message_search_enabled !== false
    const messageSearchLibraryEnabled = data?.message_search_library_enabled !== false
    const mediaFileAccessEnabled = data?.media_file_access_enabled !== false
    const calendarAiAutoCreateEnabled = data?.calendar_ai_auto_create_enabled !== false
    const calendarSilentAutoRegisterEnabled = data?.calendar_silent_auto_register_enabled === true
    const requiresRegistration =
      data?.is_enabled === false &&
      data?.bot_reply_enabled === false &&
      data?.message_search_enabled === false &&
      data?.message_search_library_enabled === false &&
      data?.media_file_access_enabled === false &&
      data?.calendar_ai_auto_create_enabled === false &&
      data?.calendar_silent_auto_register_enabled !== true
    const policy: RoomReplyPolicy = {
      roomName: normalizeDisplayName(data?.room_name ?? null),
      requiresRegistration,
      isEnabled,
      botReplyEnabled,
      messageSearchEnabled,
      messageSearchLibraryEnabled,
      mediaFileAccessEnabled,
      calendarAiAutoCreateEnabled,
      calendarSilentAutoRegisterEnabled,
    }
    cache.set(normalizedRoomId, policy)
    return policy
  } catch (err) {
    console.error(`Unexpected error while loading room reply policy for ${normalizedRoomId}:`, err)
    cache.set(normalizedRoomId, fallbackPolicy)
    return fallbackPolicy
  }
}

async function loadLineUserPermissionPolicy(
  supabase: ReturnType<typeof createClient>,
  lineUserId: string | null,
  cache: Map<string, LineUserPermissionPolicy>,
): Promise<LineUserPermissionPolicy> {
  const fallback: LineUserPermissionPolicy = {
    isActive: true,
    canMessageSearch: true,
    canLibrarySearch: true,
    canCalendarCreate: true,
    canCalendarUpdate: true,
    canMediaAccess: true,
  }
  const normalizedUserId = String(lineUserId ?? '').trim()
  if (!normalizedUserId) return fallback
  if (cache.has(normalizedUserId)) return cache.get(normalizedUserId) ?? fallback

  try {
    const { data, error } = await supabase
      .from('line_user_permissions')
      .select('is_active, can_message_search, can_library_search, can_calendar_create, can_calendar_update, can_media_access')
      .eq('line_user_id', normalizedUserId)
      .maybeSingle()
    if (error || !data) {
      cache.set(normalizedUserId, fallback)
      return fallback
    }
    const policy: LineUserPermissionPolicy = {
      isActive: data?.is_active !== false,
      canMessageSearch: data?.can_message_search !== false,
      canLibrarySearch: data?.can_library_search !== false,
      canCalendarCreate: data?.can_calendar_create !== false,
      canCalendarUpdate: data?.can_calendar_update !== false,
      canMediaAccess: data?.can_media_access !== false,
    }
    cache.set(normalizedUserId, policy)
    return policy
  } catch (_err) {
    cache.set(normalizedUserId, fallback)
    return fallback
  }
}

function isRoomInteractiveReplyEnabled(policy: RoomReplyPolicy): boolean {
  return policy.isEnabled && policy.botReplyEnabled && policy.messageSearchEnabled
}

function isRoomBotReplyEnabled(policy: RoomReplyPolicy): boolean {
  return policy.isEnabled
}

function shouldSendRoomReply(policy: RoomReplyPolicy): boolean {
  return policy.isEnabled && policy.botReplyEnabled && !policy.calendarSilentAutoRegisterEnabled
}

function looksLikeBotInteractionRequest(text: string): boolean {
  const normalized = normalizeForRuleParsing(String(text ?? '')).trim()
  if (!normalized) return false
  const compact = normalized.replace(/\s+/g, '')
  if (parseCalendarCommand(normalized).matched) return true
  if (looksLikeMessageSearchQuestion(normalized)) return true
  if (looksLikeCalendarListQuestion(normalized)) return true
  if (looksLikeExplicitCalendarQuestion(compact)) return true
  if (/(教えて|知りたい|ありますか|ある\?|ある？|検索|参照|確認|一覧|予定|会議|履歴|会話)/.test(compact)) return true
  return false
}

function buildRoomCapabilityStatusReply(
  policy: RoomReplyPolicy,
  text: string,
): string | null {
  if (!policy.isEnabled) return null
  if (!looksLikeBotInteractionRequest(text)) return null

  if (!policy.botReplyEnabled) {
    return 'この質問は、現在このルームで権限が付与されていないため実行できません。'
  } else if (!policy.messageSearchEnabled && looksLikeMessageSearchQuestion(text)) {
    return 'この質問は、現在このルームで権限が付与されていないため実行できません。'
  } else {
    return null
  }
}

function buildRoomRegistrationRequiredReply(roomName: string | null): string {
  const normalizedRoomName = normalizeInlineText(String(roomName ?? ''))
  const lines = [
    'このトークルームはまだ利用申請が完了していないため、Bot機能を利用できません。',
    `管理者に登録申請してください。${normalizedRoomName ? `（ルーム名: ${normalizedRoomName}）` : ''}`.trim(),
    '管理画面で権限が有効化されると、会話検索・予定確認などが使えるようになります。',
  ]
  return lines.join('\n')
}

function parseMessageSearchCommand(rawText: string, defaultDays: MessageRetentionDays): MessageSearchParseResult {
  const text = normalizeSpaces(rawText)
  if (!text) return { matched: false, command: null, error: null }

  const compact = normalizeForRuleParsing(text).replace(/\s+/g, '')
  const hasExplicitPrefix = /^(会話|トーク|履歴|チャット)(検索|要約|確認|まとめ|まとめて)/.test(compact)
  const hasConversationHint = /(会話|トーク|履歴|チャット|メッセージ|発言)/.test(compact)
  const hasSearchIntent =
    /(検索|要約|まとめ|まとめて|要点|要旨|教えて|見せて|みせて|確認|知りたい|ありますか|あるか|あります|ある|記述|言及|話してた|言ってた)/.test(compact)
  if (!hasExplicitPrefix && !(hasConversationHint && hasSearchIntent)) {
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
  if (/^予定(?:登録|追加|変更|確認|一覧|報告)/.test(compact)) return true
  if (/^(会話|トーク|履歴|チャット)(検索|要約|確認)/.test(compact)) return true
  return false
}

function detectMessageSearchDays(compactText: string): MessageRetentionDays | null {
  if (/(全期間|無制限|すべて|全部|全件)/.test(compactText)) return 0
  if (/(1095日|3年|三年)/.test(compactText)) return 1095
  if (/(730日|2年|二年)/.test(compactText)) return 730
  if (/(365日|1年|一年|12ヶ月|12か月|十二ヶ月)/.test(compactText)) return 365
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
    .replace(/(1095日|730日|365日|180日|120日|60日|3年|2年|1年|三年|二年|一年|半年|12ヶ月|12か月|十二ヶ月|6ヶ月|6か月|六ヶ月|4ヶ月|4か月|四ヶ月|2ヶ月|2か月|二ヶ月|全期間|無制限)/g, ' ')
    .replace(/(過去|最近|直近|以内|分|間)/g, ' ')
    .replace(/(会話|トーク|履歴|チャット|メッセージ|発言|ルーム|グループ|全ルーム|他ルーム|他のルーム|別ルーム|別のルーム)/g, ' ')
    .replace(/(検索|探し|探して|探す|要約|まとめ|まとめて|教えて|見せて|みせて|確認|表示|表示して|出して|だして|知りたい|記述|言及)/g, ' ')
    .replace(/(ありますか|あるか|あります|ある|でしたか|ですか|ますか|でしょうか|だったっけ|だっけ|っけ|かな|です|ます)/g, ' ')
    // NOTE: Longer particles first to avoid partial matches (e.g. "について" -> "に" + "ついて").
    .replace(/(について|に関して|に対して|してください|して下さい|お願いします|お願い|下さい|ください|だけ|から|とか|って|こと|もの|やつ|して|を|は|が|に|で|の)/g, ' ')
    .replace(/[?？!！。．、,「」『』（）()\[\]【】]/g, ' ')
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
              '3-1) 「参加希望は連絡ください」「ヘルプ募集」「使用店舗ありますか」「在庫共有」「締切案内」など募集・調整の業務連絡は none。',
              '4) list_calendar は、予定を尋ねる明確な質問語（いつ/ある/ありますか/教えて/確認）を伴う時のみ。',
              '5) search_messages は、会話・履歴・過去発言の検索意図が明確な時のみ。',
              '6) 少しでも迷う場合は none を選び、confidence を低めにする（0.55以下）。',
              '想定される会話パターン（運用実態ベース）:',
              'A) 在庫・発注・納品・欠品・案内・周知・提出依頼・資料共有・シフト調整依頼: none',
              'B) 「明日の会議参加可否連絡お願いします」「会議資料共有」「提出期限は◯日です」: none',
              'B-1) 「参加希望者はご連絡ください」「ヘルプ行く時に持って行きます」「使用店舗あれば連絡」: none',
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

  const hasSearchIntent = /(検索|探し|探して|探す|教えて|表示|表示して|見せて|みせて|確認|知りたい|要約|まとめ|まとめて|要点|要旨|ありますか|あるか|あります|ある|記述|言及|話してた|言ってた)/.test(compact)
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
              `days は 0/60/120/180/365/730/1095 のいずれか。0 は全期間。未指定時は ${defaultDays}。`,
              'scope は current_room または all_rooms。',
              'scopeが明示されない場合は all_rooms を返してください。',
              '「他のルーム」「全ルーム」「別グループ」等の意図がある場合は all_rooms。',
              '「このルーム」「このグループ」等の意図がある場合は current_room。',
              'keyword は検索に使う短い語句のみ。',
              '返却JSONスキーマ:',
              '{"should_search":boolean,"keyword":string,"days":0|60|120|180|365|730|1095,"scope":"current_room|all_rooms","confidence":number(0-1)}',
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
    const days = isSupportedMessageRetentionDays(rawDays)
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

function isSupportedMessageRetentionDays(value: number): value is MessageRetentionDays {
  return value === 0
    || value === 60
    || value === 120
    || value === 180
    || value === 365
    || value === 730
    || value === 1095
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
  userId: string | null,
  librarySearchEnabled: boolean,
  configuredRetentionDays: MessageRetentionDays,
  groqApiKey: string,
): Promise<string[]> {
  if (parseError) return [parseError]
  if (!command) return ['会話検索の意図を解釈できませんでした。']

  const { error: supersedeStaleLibraryPendingError } = await supabase
    .from(LIBRARY_SEARCH_PENDING_TABLE)
    .update({
      status: 'superseded',
      resolved_at: new Date().toISOString(),
    })
    .eq('conversation_key', buildConversationKey(roomId, userId))
    .eq('status', 'pending')
  if (supersedeStaleLibraryPendingError && !isMissingLibraryPendingTableError(supersedeStaleLibraryPendingError)) {
    console.error('Failed to supersede stale library search pending:', supersedeStaleLibraryPendingError)
  }

  const { effectiveDays, adjustedByRetention } = resolveEffectiveMessageSearchDays(
    command.days,
    configuredRetentionDays,
  )
  const sinceIso = effectiveDays > 0
    ? new Date(Date.now() - effectiveDays * 24 * 60 * 60 * 1000).toISOString()
    : null

  let query = supabase
    .from('line_messages')
    .select('room_id, content, created_at, user_id')
    .order('created_at', { ascending: false })
    .limit(SEARCH_MAX_FETCH_ROWS)
  if (sinceIso) {
    query = query.gte('created_at', sinceIso)
  }

  if (command.scope !== 'all_rooms') {
    query = query.eq('room_id', roomId)
  }

  const { data, error } = await query

  if (error) {
    return [`会話検索に失敗しました。${error.message}`]
  }

  const rows: SearchMessageRow[] = Array.isArray(data)
    ? data.map((row: any) => ({
        room_id: String(row?.room_id ?? ''),
        content: String(row?.content ?? ''),
        created_at: String(row?.created_at ?? ''),
        user_id: row?.user_id == null ? null : String(row.user_id),
      }))
    : []

  const hitsRaw = rows.filter((row) => {
    if (!messageMatchesKeyword(row.content, command.keyword)) return false
    if (isLikelyBotConversationText(row.content)) return false
    return true
  })

  if (hitsRaw.length === 0) {
    const periodText = effectiveDays > 0 ? `過去${effectiveDays}日` : '全期間'
    if (!librarySearchEnabled) {
      const lines: string[] = [`「${command.keyword}」に一致する会話はありません（${periodText}）`]
      if (adjustedByRetention) {
        lines.push(`※保持期間設定が${configuredRetentionDays}日のため、検索範囲を調整しました。`)
      }
      lines.push('※このルームでは資料ライブラリ検索（2段階目）が無効です。')
      return [lines.join('\n')]
    }
    const pendingSaved = await savePendingLibrarySearchConfirmation(
      supabase,
      roomId,
      userId,
      command,
      effectiveDays,
      adjustedByRetention,
    )
    const lines: string[] = [
      `「${command.keyword}」に一致する会話はありません（${periodText}）`,
    ]
    if (adjustedByRetention) {
      lines.push(`※保持期間設定が${configuredRetentionDays}日のため、検索範囲を調整しました。`)
    }
    if (pendingSaved) {
      lines.push('')
      lines.push('資料ライブラリ（ファイル名に[LINE]を含む資料のみ）も検索しますか？')
      lines.push('「はい」で検索 / 「いいえ」でキャンセル')
      lines.push(`※${PENDING_CONFIRMATION_TTL_MIN}分以内にご返信ください`)
    } else {
      lines.push('')
      lines.push('資料ライブラリへ進む確認を保存できませんでした。しばらくしてからもう一度お試しください。')
    }
    return [lines.join('\n')]
  }

  const roomLabels = command.scope === 'all_rooms'
    ? await loadRoomLabelsForHits(supabase, hitsRaw.map((row) => ({ room_id: row.room_id })))
    : new Map<string, string>()
  const hits = hitsRaw.map((row) => ({
    ...row,
    room_label: command.scope === 'all_rooms'
      ? (roomLabels.get(row.room_id) ?? row.room_id)
      : null,
  }))

  const shouldSummarize = !!groqApiKey && hits.length > 0 && hits.length <= SEARCH_AI_SUMMARY_MAX_HITS
  const summary = await summarizeMessageSearchHitsWithGroq(
    shouldSummarize ? hits.slice(0, SEARCH_MAX_SUMMARY_ROWS) : [],
    command.keyword,
    effectiveDays,
    groqApiKey,
  )

  const scopeLabel = command.scope === 'all_rooms' ? '全ルーム横断' : 'このルーム'
  const periodLabel = effectiveDays > 0 ? `過去${effectiveDays}日` : '全期間'
  const lines: string[] = [
    '会話検索結果',
    `対象: ${scopeLabel}`,
    `期間: ${periodLabel}`,
    `キーワード: ${command.keyword}`,
    `会話一致: ${hits.length}件`,
  ]
  if (adjustedByRetention) {
    lines.push(`※保持期間設定が${configuredRetentionDays}日のため、検索範囲を調整しました。`)
  }
  if (rows.length >= SEARCH_MAX_FETCH_ROWS) {
    lines.push(`※検索対象が多いため、新しい順で先頭${SEARCH_MAX_FETCH_ROWS}件を対象にしています。`)
  }
  if (summary) {
    lines.push('')
    lines.push('会話要約:')
    lines.push(summary)
  } else if (!!groqApiKey && hits.length > SEARCH_AI_SUMMARY_MAX_HITS) {
    lines.push(`※一致件数が多いため、AI要約は省略しています（${SEARCH_AI_SUMMARY_MAX_HITS}件超）。`)
  }
  if (hits.length > 0) {
    lines.push('')
    lines.push('一致メッセージ（新しい順）:')
    for (let i = 0; i < hits.length; i += 1) {
      lines.push('')
      lines.push(...formatMessageSearchPreview(hits[i], i + 1, command.scope === 'all_rooms'))
    }
  }
  return splitTextForLineReply(lines.join('\n'))
}

function resolveEffectiveMessageSearchDays(
  requestedDays: MessageRetentionDays,
  configuredRetentionDays: MessageRetentionDays,
): { effectiveDays: MessageRetentionDays; adjustedByRetention: boolean } {
  const effectiveDays = (configuredRetentionDays === 0
    ? requestedDays
    : (requestedDays === 0 ? configuredRetentionDays : Math.min(requestedDays, configuredRetentionDays))) as MessageRetentionDays
  const adjustedByRetention = configuredRetentionDays > 0 && (
    requestedDays === 0 || requestedDays > configuredRetentionDays
  )
  return { effectiveDays, adjustedByRetention }
}

async function buildLibrarySearchPromptWhenMessageSearchDisabled(
  supabase: ReturnType<typeof createClient>,
  roomId: string,
  userId: string | null,
  command: MessageSearchCommand,
  configuredRetentionDays: MessageRetentionDays,
): Promise<string> {
  const { effectiveDays, adjustedByRetention } = resolveEffectiveMessageSearchDays(
    command.days,
    configuredRetentionDays,
  )
  const periodText = effectiveDays > 0 ? `過去${effectiveDays}日` : '全期間'
  const pendingSaved = await savePendingLibrarySearchConfirmation(
    supabase,
    roomId,
    userId,
    command,
    effectiveDays,
    adjustedByRetention,
  )
  const lines: string[] = [
    'このユーザーは会話検索の権限がないため、会話履歴は検索できません。',
    `キーワード: ${command.keyword}`,
    `対象期間: ${periodText}`,
  ]
  if (adjustedByRetention) {
    lines.push(`※保持期間設定が${configuredRetentionDays}日のため、検索範囲を調整しました。`)
  }
  lines.push('')
  if (pendingSaved) {
    lines.push('資料ライブラリ（ファイル名に[LINE]を含む資料のみ）を検索しますか？')
    lines.push('「はい」で検索 / 「いいえ」でキャンセル')
    lines.push(`※${PENDING_CONFIRMATION_TTL_MIN}分以内にご返信ください`)
  } else {
    lines.push('資料ライブラリ検索の確認保存に失敗しました。しばらくしてからもう一度お試しください。')
  }
  return lines.join('\n')
}

async function loadRoomLabelsForHits(
  supabase: ReturnType<typeof createClient>,
  rows: Array<{ room_id: string | null }>,
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

function formatDocumentSearchPreview(
  row: SearchDocumentRow,
  index: number,
  keyword: string,
  includeRoomLabel = false,
): string[] {
  const date = formatSearchDateTime(row.created_at)
  const talkDateTime = extractTalkDateTimeFromDocument(row.extracted_text, keyword)
  const fileName = normalizeInlineText(row.original_file_name) || '（ファイル名不明）'
  const mimeType = normalizeInlineText(row.mime_type) || '-'
  const snippet = buildDocumentSearchSnippet(row.extracted_text, keyword)
  const snippetLines = splitMessagePreviewIntoParagraphLines(snippet, 24)
  const lines = [`${index}件目`]
  if (includeRoomLabel) {
    const roomLabel = normalizeInlineText(String(row.room_label ?? '')) || '（ルーム不明）'
    lines.push(`  ルーム: ${roomLabel}`)
  }
  lines.push(`  資料登録日時: ${date}`)
  if (talkDateTime) {
    lines.push(`  トーク日時: ${talkDateTime}`)
  }
  lines.push(`  ファイル: ${fileName}`)
  lines.push(`  種別: ${mimeType}`)
  lines.push('  抜粋:')
  for (const line of snippetLines) {
    lines.push(`    ${line}`)
  }
  return lines
}

async function buildLineTaggedDocumentLibrarySearchReply(
  command: MessageSearchCommand,
  supabase: ReturnType<typeof createClient>,
  roomId: string,
): Promise<string[]> {
  let docQuery = supabase
    .from('line_search_documents')
    .select('room_id, original_file_name, mime_type, extracted_text, created_at')
    .order('created_at', { ascending: false })
    .limit(SEARCH_MAX_DOCUMENT_ROWS)
  if (command.scope !== 'all_rooms') {
    docQuery = docQuery.eq('room_id', roomId)
  }
  const { data: docData, error: docError } = await docQuery
  if (docError) {
    return [`資料ライブラリの検索に失敗しました。${docError.message}`]
  }
  const docRows: SearchDocumentRow[] = Array.isArray(docData)
    ? docData.map((row: any) => ({
        room_id: row?.room_id == null ? null : String(row.room_id),
        original_file_name: String(row?.original_file_name ?? ''),
        mime_type: String(row?.mime_type ?? ''),
        extracted_text: String(row?.extracted_text ?? ''),
        created_at: String(row?.created_at ?? ''),
      }))
    : []

  const lineTaggedRows = docRows.filter((row) =>
    row.original_file_name.includes(LINE_DOCUMENT_LIBRARY_FILENAME_MARKER),
  )
  const docHitsRaw = lineTaggedRows.filter((row) => {
    const searchable = `${row.original_file_name}\n${row.extracted_text}`
    return messageMatchesKeyword(searchable, command.keyword)
  })

  const periodText = '登録済み[LINE]資料全体'
  if (docHitsRaw.length === 0) {
    const lines = [`「${command.keyword}」に一致する[LINE]資料はありません（${periodText}）`]
    return splitTextForLineReply(lines.join('\n'))
  }

  const roomLabels = command.scope === 'all_rooms'
    ? await loadRoomLabelsForHits(supabase, docHitsRaw.map((row) => ({ room_id: row.room_id })))
    : new Map<string, string>()
  const docHits = docHitsRaw.map((row) => ({
    ...row,
    room_label: command.scope === 'all_rooms'
      ? (row.room_id ? (roomLabels.get(row.room_id) ?? row.room_id) : '共通資料')
      : null,
  }))

  const scopeLabel = command.scope === 'all_rooms' ? '全ルーム横断' : 'このルーム'
  const lines: string[] = [
    '資料ライブラリ検索結果（ファイル名に[LINE]を含む資料のみ）',
    `対象: ${scopeLabel}`,
    `期間: ${periodText}`,
    `キーワード: ${command.keyword}`,
    `資料一致: ${docHits.length}件`,
    '※日時は会話発生時刻ではなく、資料の登録日時です。',
  ]
  if (docRows.length >= SEARCH_MAX_DOCUMENT_ROWS) {
    lines.push(`※資料件数が多いため、新しい順で先頭${SEARCH_MAX_DOCUMENT_ROWS}件を対象にしています（その中から[LINE]付きのみを表示）。`)
  }
  lines.push('')
  lines.push('一致資料（新しい順）:')
  for (let i = 0; i < docHits.length; i += 1) {
    lines.push('')
    lines.push(...formatDocumentSearchPreview(docHits[i], i + 1, command.keyword, command.scope === 'all_rooms'))
  }
  return splitTextForLineReply(lines.join('\n'))
}

function buildDocumentSearchSnippet(text: string, keyword: string): string {
  const normalized = normalizeMessagePreviewText(String(text ?? '')).replace(/\s+/g, ' ').trim()
  if (!normalized) return '（本文抽出なし。ファイル名のみ一致）'

  const key = normalizeMessagePreviewText(keyword).toLowerCase()
  const keywordTokens = key
    .split(/[\s、，。]+/)
    .map((token) => token.trim())
    .filter((token) => token.length > 0)

  if (!key || keywordTokens.length === 0) {
    return normalized.length > 180 ? `${normalized.slice(0, 180)}...` : normalized
  }

  const sentenceCandidates = splitMessagePreviewIntoParagraphLines(normalized, 24)
    .map((line) => line.trim())
    .filter((line) => line.length > 0)

  for (const sentence of sentenceCandidates) {
    const sentenceLower = sentence.toLowerCase()
    const matched = keywordTokens.some((token) => sentenceLower.includes(token))
    if (!matched) continue
    return sentence.length > 180 ? `${sentence.slice(0, 180)}...` : sentence
  }

  const lower = normalized.toLowerCase()
  const idx = lower.indexOf(key)
  if (idx < 0) {
    return normalized.length > 180 ? `${normalized.slice(0, 180)}...` : normalized
  }

  const start = Math.max(0, idx - 70)
  const end = Math.min(normalized.length, idx + key.length + 70)
  const prefix = start > 0 ? '...' : ''
  const suffix = end < normalized.length ? '...' : ''
  return `${prefix}${normalized.slice(start, end)}${suffix}`
}

function extractTalkDateTimeFromDocument(text: string, keyword: string): string | null {
  const source = String(text ?? '')
  if (!source.trim()) return null

  const keywordTokens = normalizeKeywordForSearch(keyword)
    .split(/[\s、，。]+/)
    .map((token) => token.trim())
    .filter((token) => token.length > 0)
  if (keywordTokens.length === 0) return null

  const lines = source.split(/\r?\n/)
  let currentDate: { year: number, month: number, day: number } | null = null

  for (let i = 0; i < lines.length; i += 1) {
    const line = normalizeForRuleParsing(lines[i]).trim()
    if (!line) continue

    const dateOnLine = extractDatePartsFromText(line)
    if (dateOnLine) {
      currentDate = dateOnLine
    }

    const normalizedLine = normalizeKeywordForSearch(line)
    const hasKeyword = keywordTokens.some((token) => normalizedLine.includes(token))
    if (!hasKeyword) continue

    const lineTime = extractTimePartsFromText(line)
    if (dateOnLine && lineTime) {
      return formatTalkDateTime(dateOnLine, lineTime)
    }
    if (currentDate && lineTime) {
      return formatTalkDateTime(currentDate, lineTime)
    }

    if (currentDate) {
      const nearbyTime = findNearbyTimeParts(lines, i, 3)
      if (nearbyTime) {
        return formatTalkDateTime(currentDate, nearbyTime)
      }
    }
  }

  return null
}

function extractDatePartsFromText(text: string): { year: number, month: number, day: number } | null {
  const m = normalizeForRuleParsing(text).match(/(\d{4})[\/.\-年](\d{1,2})[\/.\-月](\d{1,2})日?/)
  if (!m || !m[1] || !m[2] || !m[3]) return null
  const year = Number(m[1])
  const month = Number(m[2])
  const day = Number(m[3])
  const iso = toIsoDateStringSafe(year, month, day)
  if (!iso) return null
  return { year, month, day }
}

function extractTimePartsFromText(text: string): { hour: number, minute: number } | null {
  const normalized = normalizeForRuleParsing(text)
  const colon = normalized.match(/(\d{1,2}):(\d{2})/)
  if (colon && colon[1] && colon[2]) {
    const hour = Number(colon[1])
    const minute = Number(colon[2])
    if (Number.isInteger(hour) && Number.isInteger(minute) && hour >= 0 && hour <= 23 && minute >= 0 && minute <= 59) {
      return { hour, minute }
    }
  }
  const japanese = normalized.match(/(\d{1,2})時(?:\s*(\d{1,2})分?)?/)
  if (!japanese || !japanese[1]) return null
  const hour = Number(japanese[1])
  const minute = japanese[2] ? Number(japanese[2]) : 0
  if (!Number.isInteger(hour) || !Number.isInteger(minute)) return null
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59) return null
  return { hour, minute }
}

function findNearbyTimeParts(lines: string[], index: number, distance: number): { hour: number, minute: number } | null {
  for (let offset = 0; offset <= distance; offset += 1) {
    const prevIndex = index - offset
    if (prevIndex >= 0) {
      const prev = normalizeForRuleParsing(lines[prevIndex]).trim()
      const prevTime = extractTimePartsFromText(prev)
      if (prevTime) return prevTime
    }
    const nextIndex = index + offset
    if (offset > 0 && nextIndex < lines.length) {
      const next = normalizeForRuleParsing(lines[nextIndex]).trim()
      const nextTime = extractTimePartsFromText(next)
      if (nextTime) return nextTime
    }
  }
  return null
}

function formatTalkDateTime(
  date: { year: number, month: number, day: number },
  time: { hour: number, minute: number },
): string {
  const yyyy = String(date.year).padStart(4, '0')
  const mm = String(date.month).padStart(2, '0')
  const dd = String(date.day).padStart(2, '0')
  const hh = String(time.hour).padStart(2, '0')
  const mi = String(time.minute).padStart(2, '0')
  return `${yyyy}/${mm}/${dd} ${hh}:${mi}`
}

function formatMessageSearchPreview(
  row: SearchMessageRow,
  index: number,
  includeRoomLabel = false,
): string[] {
  const date = formatSearchDateTime(row.created_at)
  const content = normalizeMessagePreviewText(String(row.content ?? ''))
  const compact = content.length > 220 ? `${content.slice(0, 220)}...` : (content || '（内容なし）')
  const previewLines = splitMessagePreviewIntoParagraphLines(compact, 24)
  const lines = [`${index}件目`]
  if (includeRoomLabel) {
    const roomLabel = normalizeInlineText(String(row.room_label ?? '')) || '（ルーム不明）'
    lines.push(`  ルーム: ${roomLabel}`)
  }
  lines.push(`  日時: ${date}`)
  lines.push('  内容:')
  for (const line of previewLines) {
    lines.push(line)
  }
  return lines
}

function normalizeMessagePreviewText(raw: string): string {
  const normalized = normalizeInlineText(String(raw ?? ''))
  if (!normalized) return ''
  return normalized
    .replace(/([一-龥ぁ-んァ-ヶー々〆〤])\s+([一-龥ぁ-んァ-ヶー々〆〤])/g, '$1$2')
    .replace(/([一-龥ぁ-んァ-ヶー々〆〤])\s+([、。．，！？!?：:])/g, '$1$2')
    .replace(/([、。．，！？!?：:])\s+([一-龥ぁ-んァ-ヶー々〆〤])/g, '$1$2')
    .replace(/([／/「『（(])\s+/g, '$1')
    .replace(/\s+([」』）),])/g, '$1')
    .replace(/\s{2,}/g, ' ')
    .trim()
}

function splitMessagePreviewIntoParagraphLines(text: string, maxCharsPerLine: number): string[] {
  const normalized = normalizeMessagePreviewText(text)
  if (!normalized) return ['（内容なし）']

  const sentenceCandidates = normalized
    .replace(/([。！？!?])/g, '$1\n')
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line.length > 0)

  if (sentenceCandidates.length <= 1) {
    return [normalized]
  }

  const lines: string[] = []
  for (const sentence of sentenceCandidates) {
    lines.push(sentence)
  }
  return lines.length > 0 ? lines : ['（内容なし）']
}

function wrapTextForLineDisplay(text: string, maxCharsPerLine: number): string[] {
  const normalized = normalizeInlineText(String(text ?? ''))
  if (!normalized) return ['（内容なし）']
  const chars = Array.from(normalized)
  const lines: string[] = []
  for (let i = 0; i < chars.length; i += maxCharsPerLine) {
    lines.push(chars.slice(i, i + maxCharsPerLine).join(''))
  }
  return lines.length > 0 ? lines : ['（内容なし）']
}

function formatSearchDateTime(iso: string): string {
  const date = new Date(iso)
  if (Number.isNaN(date.getTime())) return '(時刻不明)'
  return new Intl.DateTimeFormat('ja-JP', {
    timeZone: 'Asia/Tokyo',
    year: 'numeric',
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
              `検索範囲: ${days > 0 ? `過去${days}日` : '全期間'}`,
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

function isLikelyBotDirectedSearchPrompt(text: string): boolean {
  const normalized = normalizeForRuleParsing(String(text ?? '')).trim()
  if (!normalized) return false
  const compact = normalized.replace(/\s+/g, '')
  if (/^(会話|トーク|履歴|チャット)(検索|要約|確認)/.test(compact)) return true
  if (/について(教えて|知りたい|ありますか|あるか|ある\?|ある？)/.test(compact) && compact.length <= 80) {
    return true
  }
  if (looksLikeMessageSearchQuestion(normalized) && /(教えて|知りたい|参照|検索|見せて|表示|出して)/.test(compact)) {
    return true
  }
  return false
}

function isLikelyBotConversationText(text: string): boolean {
  const normalized = normalizeForRuleParsing(String(text ?? '')).trim()
  if (!normalized) return false
  const compact = normalized.replace(/\s+/g, '')
  if (isExplicitBotCommandText(normalized)) return true
  if (parseCalendarCommand(normalized).matched) return true
  if (looksLikeCalendarListQuestion(normalized)) return true
  if (looksLikeMessageSearchQuestion(normalized)) return true
  if (isLikelyBotDirectedSearchPrompt(normalized)) return true
  if (/^(予定確認|予定一覧|予定報告|会話検索|履歴検索|トーク検索|チャット検索)/.test(compact)) return true
  return false
}

function shouldOfferMessageSearchGuidance(text: string): boolean {
  const normalized = normalizeForRuleParsing(String(text ?? '')).trim()
  if (!normalized) return false
  const compact = normalized.replace(/\s+/g, '')
  if (looksLikeMessageSearchQuestion(normalized)) return false
  if (looksLikeCalendarListQuestion(normalized)) return false
  if (looksLikeExplicitCalendarQuestion(compact)) return false
  if (parseCalendarCommand(normalized).matched) return false
  if (looksLikeAnnouncementText(compact)) return false
  const hasQuestionIntent = /(教えて|知りたい|ありますか|ある\?|ある？|何|どこ|いつ)/.test(compact)
  return hasQuestionIntent && /について/.test(compact)
}

function buildMessageSearchGuidanceReply(text: string): string {
  const keyword = extractMessageSearchKeyword(text) || normalizeKeywordForFilter(text)
  const exampleKeyword = keyword || 'キーワード'
  return [
    '履歴検索の意図かもしれないですが、通常文では自動検索しない設定です。',
    `会話検索する場合は「会話検索 ${exampleKeyword}」の形式で送ってください。`,
  ].join('\n')
}

function shouldOfferUnknownIntentFallback(text: string): boolean {
  const normalized = normalizeForRuleParsing(String(text ?? '')).trim()
  if (!normalized) return false
  if (!looksLikeBotInteractionRequest(normalized)) return false
  if (parseCalendarCommand(normalized).matched) return false
  if (isExplicitBotCommandText(normalized)) return true
  if (looksLikeMessageSearchQuestion(normalized)) return true
  if (looksLikeCalendarListQuestion(normalized)) return true
  if (looksLikeCalendarUpdateConversationText(normalized)) return true
  return true
}

function buildUnknownIntentReply(): string {
  return [
    'ごめんなさい。内容を正確に理解できませんでした。',
    'もう少し具体的に、対象・日時・やりたい操作を教えてください。',
    '例: 「会話検索 ペローニ」「予定確認 5月」「1件目の時間を19:00に変更」',
  ].join('\n')
}

function looksLikeCalendarCandidate(text: string): boolean {
  const normalized = normalizeForRuleParsing(text).toLowerCase()
  const hasDateHint = /(\d{4}[\/.\-]\d{1,2}[\/.\-]\d{1,2}|\d{4}年\d{1,2}月\d{1,2}日|\d{1,2}[\/.\-]\d{1,2}|\d{1,2}日|今日|明日|明後日|来週|今週)/.test(normalized)
  const hasTimeHint = /(\d{1,2}:\d{2}|\d{1,2}時(\d{1,2}分)?)/.test(normalized)
  const hasIntentWord = /(予定|会議|打ち合わせ|ミーティング|mtg|予約|アポ|面談|訪問|来店|ランチ|ディナー)/.test(normalized)
  return (hasDateHint && hasTimeHint) || (hasIntentWord && (hasDateHint || hasTimeHint))
}

function looksLikeOperationalCoordinationText(text: string): boolean {
  const compact = normalizeForRuleParsing(text).replace(/\s+/g, '')
  if (!compact) return false

  const hasCoordinationCue =
    /(ヘルプ|参加者|参加希望|募集|使用店舗|希望店舗|使っていただける店舗|ご連絡|連絡お願いします|ご検討|共有|周知|案内|締切|締め切り|提出期限|回収|ピックアップ|納品|発注|在庫|欠品|配達|取りに来)/.test(compact)
  if (!hasCoordinationCue) return false

  const hasExplicitCalendarCreateIntent =
    /(予定登録|予定追加|予定作成|カレンダー登録|登録して|登録お願いします|入れて|追加して)/.test(compact)
  if (hasExplicitCalendarCreateIntent) return false

  return true
}

function looksLikeStrongCalendarCreateIntent(text: string): boolean {
  const compact = normalizeForRuleParsing(text).replace(/\s+/g, '')
  if (!compact) return false
  if (looksLikeOperationalCoordinationText(compact)) return false

  const hasExplicitCreatePhrase =
    /(予定登録|予定追加|予定作成|カレンダー登録|カレンダー追加|予定を入れて|予定入れて|登録して|登録お願いします|追加して|追加お願いします|入れてください)/.test(compact)
  if (hasExplicitCreatePhrase) return true

  const hasDateHint = /(\d{4}[\/.\-]\d{1,2}[\/.\-]\d{1,2}|\d{1,2}月\d{1,2}日|今日|明日|明後日|来週|今週)/.test(compact)
  const hasTimeHint = /(\d{1,2}:\d{2}|\d{1,2}時(?:\d{1,2}分)?)/.test(compact)
  const hasEventWord = /(予定|会議|打ち合わせ|打合せ|ミーティング|mtg|meeting|講習会|セミナー|試飲会|イベント)/i.test(compact)
  const hasRequestTone = /(お願いします|お願い|してください|したい|します|でお願いします|です)/.test(compact)

  return hasDateHint && hasTimeHint && hasEventWord && hasRequestTone
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
              'title は短縮しすぎず、修飾語を保持してください（例: 「シェフミーティング」は「ミーティング」に短縮しない）。',
              '場所が読み取れる場合は location に入れてください（例: 「marugoで試飲会」→ title=試飲会, location=marugo）。',
              '複数行の案内文でも同様に分離してください（例: 「試飲会お知らせ / 7/15 / クラウディア2 / 2階 / 15:00-17:00」→ title=試飲会, location=クラウディア2 2階）。',
              'ラベル付きでも同様に分離してください（例: 「【日時】6/19 15時〜17時 / 【場所】マルゴ四谷 / 従業員向け試飲会」→ title=試飲会, location=マルゴ四谷）。',
              '「次回会議は6月12日、14:30～15:30にオンライン会議」のような文は title=会議, location=オンライン にしてください。',
              '提出期限など別目的の日付が混在していても、予定本体（会議/試飲会など）の日時を優先して抽出してください。',
              'ただし、次のような「募集・調整・共有」文脈は should_create=false にしてください: 「参加希望者はご連絡ください」「ヘルプ募集」「使用店舗ありますか」「在庫共有」「締切案内」。',
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

function normalizeAiCalendarUpdateDateCandidate(raw: unknown): string | undefined {
  const value = normalizeForRuleParsing(String(raw ?? '')).trim()
  if (!value) return undefined
  if (isValidDate(value)) return value

  const m = value.match(/^(\d{4})[\/.\-年](\d{1,2})[\/.\-月](\d{1,2})日?$/)
  if (!m) return undefined
  const year = Number(m[1])
  const month = Number(m[2])
  const day = Number(m[3])
  return toIsoDateStringSafe(year, month, day) ?? undefined
}

function normalizeAiCalendarUpdateTimeCandidate(raw: unknown): string | undefined {
  const value = normalizeForRuleParsing(String(raw ?? '')).trim()
  if (!value) return undefined
  if (isValidTime(value)) return value

  const parsed = parseFlexibleTimeToken(value)
  if (!parsed) return undefined
  const normalized = `${String(parsed.hour).padStart(2, '0')}:${String(parsed.minute).padStart(2, '0')}`
  return isValidTime(normalized) ? normalized : undefined
}

function normalizeAiCalendarUpdateDurationCandidate(raw: unknown): number | undefined {
  const text = normalizeForRuleParsing(String(raw ?? '')).trim()
  if (!text) return undefined
  const direct = Number(text)
  if (Number.isInteger(direct) && direct > 0 && direct <= MAX_DURATION_MIN) {
    return direct
  }
  const matched = text.match(/(\d{1,3})/)
  if (!matched || !matched[1]) return undefined
  const value = Number(matched[1])
  if (!Number.isInteger(value) || value <= 0 || value > MAX_DURATION_MIN) return undefined
  return value
}

async function extractCalendarUpdateIntentWithGroq(
  text: string,
  timezone: string,
  groqApiKey: string,
  currentTitle: string,
): Promise<AiCalendarUpdateIntent | null> {
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
              'あなたは「予定変更」専用のJSON抽出器です。',
              '入力は1件の予定に対する修正指示です。返すのは「変更項目のみ」です。',
              `現在時刻は ${nowText} (${timezone})。`,
              `現在の予定名: ${cleanCalendarTitle(currentTitle || '予定')}`,
              '言葉の揺れを同義として解釈してください。',
              '例: 変更/修正/更新/直して/変えて/ずらして/前倒し/後ろ倒し/早めて/遅らせて。',
              '例: 件名/タイトル/予定名/中身、場所/会場/開催場所、時間/時刻/開始時間/スタート、内容/詳細/説明。',
              '「19時半」「7時」「20:15」は time に HH:mm で返してください。',
              '「場所なし」「会場未設定」「場所をクリア」は clear_location=true を返してください。',
              '「内容なし」「説明をクリア」は clear_description=true を返してください。',
              'title/location/description は実際に登録する値のみ返してください。',
              '操作説明語（例: 次の/以下/下記/言葉/文言/文章/テキスト/変えて/変更して/記載して）は値に含めないでください。',
              'description には管理メタ行（例: LINE room_id:, LINE user_id:, source: line-webhook）を含めないでください。',
              '例: 「内容に次の言葉に変えて、店長のみの会議ですと記載して」→ description=店長のみの会議です',
              '例: 「予定の会議を店長会議に変更して」→ title=店長会議',
              '例: 「場所をmarugoにして」→ location=marugo',
              '複数変更があれば同時に抽出してください。',
              '変更が読み取れない・曖昧な場合は should_update=false にしてください。',
              '日付は YYYY-MM-DD、時刻は HH:mm、duration_min は分(整数)で返してください。',
              'JSONのみ返してください。説明文やコードブロックは禁止です。',
              '返却JSONスキーマ:',
              '{"should_update":boolean,"confidence":number(0-1),"title":"string|optional","date":"YYYY-MM-DD|optional","time":"HH:mm|optional","duration_min":number|optional,"location":"string|optional","clear_location":boolean|optional,"description":"string|optional","clear_description":boolean|optional,"reason":string}',
            ].join('\n'),
          },
          { role: 'user', content: text },
        ],
      }),
    })

    if (!response.ok) {
      const err = await response.text()
      console.error('Groq update extraction failed:', response.status, err)
      return null
    }

    const json = await response.json()
    const content = String(json?.choices?.[0]?.message?.content ?? '').trim()
    if (!content) return null

    const extracted = parseFirstJsonObject(content)
    if (!extracted || typeof extracted !== 'object') return null

    const raw = extracted as Record<string, unknown>
    const shouldUpdate = Boolean(raw.should_update ?? raw.shouldUpdate ?? false)
    const confidenceNum = Number(raw.confidence ?? 0)
    const confidence = Number.isFinite(confidenceNum)
      ? Math.max(0, Math.min(1, confidenceNum))
      : 0
    const titleRaw = normalizeForRuleParsing(String(raw.title ?? raw.summary ?? raw.event_name ?? '')).trim()
    const title = titleRaw ? cleanCalendarTitle(titleRaw) : undefined
    const date = normalizeAiCalendarUpdateDateCandidate(raw.date ?? raw.start_date ?? raw.day)
    const time = normalizeAiCalendarUpdateTimeCandidate(raw.time ?? raw.start_time ?? raw.start)
    const durationMin = normalizeAiCalendarUpdateDurationCandidate(
      raw.duration_min ?? raw.durationMin ?? raw.duration ?? raw.length_min,
    )
    const locationRaw = normalizeForRuleParsing(String(raw.location ?? raw.place ?? raw.venue ?? '')).trim()
    let clearLocation = Boolean(raw.clear_location ?? raw.clearLocation ?? false)
    let location: string | undefined
    if (locationRaw) {
      const compactLocation = normalizeForRuleParsing(locationRaw).replace(/\s+/g, '')
      if (/^(なし|未設定|空|クリア|削除|消去)$/.test(compactLocation)) {
        clearLocation = true
      } else {
        const cleanedLocation = cleanCalendarLocation(locationRaw)
        if (cleanedLocation) location = cleanedLocation
      }
    }
    if (clearLocation) {
      location = undefined
    }
    const descriptionRaw = normalizeForRuleParsing(String(raw.description ?? raw.content ?? raw.detail ?? '')).trim()
    let clearDescription = Boolean(raw.clear_description ?? raw.clearDescription ?? false)
    let description: string | undefined
    if (descriptionRaw) {
      const compactDescription = normalizeForRuleParsing(descriptionRaw).replace(/\s+/g, '')
      if (/^(なし|未設定|空|クリア|削除|消去)$/.test(compactDescription)) {
        clearDescription = true
      } else {
        const cleanedDescription = cleanCalendarDescription(descriptionRaw)
        if (cleanedDescription) description = cleanedDescription
      }
    }
    if (clearDescription) {
      description = undefined
    }
    const reason = normalizeForRuleParsing(String(raw.reason ?? '')).trim()

    return {
      shouldUpdate,
      confidence,
      ...(title ? { title } : {}),
      ...(date ? { date } : {}),
      ...(time ? { time } : {}),
      ...(typeof durationMin === 'number' ? { durationMin } : {}),
      ...(location ? { location } : {}),
      ...(clearLocation ? { clearLocation: true } : {}),
      ...(description ? { description } : {}),
      ...(clearDescription ? { clearDescription: true } : {}),
      reason,
    }
  } catch (err) {
    console.error('Failed to extract calendar update intent with Groq:', err)
    return null
  }
}

function isAcceptableAiCalendarUpdateIntent(intent: AiCalendarUpdateIntent): boolean {
  if (!intent.shouldUpdate) return false
  if (intent.confidence < AI_UPDATE_MIN_CONFIDENCE) return false
  if (intent.date && !isValidDate(intent.date)) return false
  if (intent.time && !isValidTime(intent.time)) return false
  if (typeof intent.durationMin === 'number') {
    if (!Number.isInteger(intent.durationMin) || intent.durationMin <= 0 || intent.durationMin > MAX_DURATION_MIN) {
      return false
    }
  }
  if (intent.title && cleanCalendarTitle(intent.title) === '予定') return false
  if (intent.location && !cleanCalendarLocation(intent.location)) return false
  if (intent.description && !cleanCalendarDescription(intent.description)) return false
  const hasAnyField =
    !!intent.title
    || !!intent.date
    || !!intent.time
    || typeof intent.durationMin === 'number'
    || !!intent.location
    || !!intent.clearLocation
    || !!intent.description
    || !!intent.clearDescription
  return hasAnyField
}

function buildCalendarUpdateCommandFromAiIntent(
  eventId: string,
  intent: AiCalendarUpdateIntent,
): CalendarUpdateCommand | null {
  if (!isAcceptableAiCalendarUpdateIntent(intent)) return null
  return {
    kind: 'update',
    eventId,
    ...(intent.title ? { title: cleanCalendarTitle(intent.title) } : {}),
    ...(intent.date ? { date: intent.date } : {}),
    ...(intent.time ? { time: intent.time } : {}),
    ...(typeof intent.durationMin === 'number' ? { durationMin: intent.durationMin } : {}),
    ...(intent.location ? { location: intent.location } : {}),
    ...(intent.clearLocation ? { clearLocation: true } : {}),
    ...(intent.description ? { description: intent.description } : {}),
    ...(intent.clearDescription ? { clearDescription: true } : {}),
  }
}

function mergeCalendarUpdateCommands(
  base: CalendarUpdateCommand,
  override: CalendarUpdateCommand,
): CalendarUpdateCommand {
  const merged: CalendarUpdateCommand = {
    kind: 'update',
    eventId: base.eventId,
  }

  const title = override.title ?? base.title
  if (title) merged.title = title

  // Keep rule-based schedule when available. AI is primarily for semantic fields.
  const date = base.date ?? override.date
  if (date) merged.date = date

  const time = base.time ?? override.time
  if (time) merged.time = time

  const durationMin = typeof base.durationMin === 'number' ? base.durationMin : override.durationMin
  if (typeof durationMin === 'number') merged.durationMin = durationMin

  if (override.clearLocation) {
    merged.clearLocation = true
  } else {
    if (override.location) {
      merged.location = override.location
    } else if (base.location) {
      merged.location = base.location
    }
    if (base.clearLocation) merged.clearLocation = true
  }

  if (override.clearDescription) {
    merged.clearDescription = true
  } else {
    if (override.description) {
      merged.description = override.description
    } else if (base.description) {
      merged.description = base.description
    }
    if (base.clearDescription) merged.clearDescription = true
  }

  return merged
}

function looksLikeSemanticFieldUpdateText(rawText: string): boolean {
  const compact = normalizeForRuleParsing(rawText).replace(/\s+/g, '')
  if (!compact) return false
  const hasSemanticFieldCue = /(内容|詳細|説明|件名|タイトル|予定名|中身|場所|会場|開催場所|開催会場)/.test(compact)
  if (!hasSemanticFieldCue) return false
  return /(変更|修正|更新|直して|直す|変えて|変える|にして|記載|記入|追記|追加|書いて|入れて|載せて|残して|メモして)/.test(compact)
}

function isLikelyInstructionOnlyFieldValue(raw: string): boolean {
  const normalized = normalizeForRuleParsing(raw).trim()
  if (!normalized) return true
  const compact = normalized.replace(/\s+/g, '')
  if (!compact) return true
  if (/^(?:言葉|文言|文章|テキスト|内容|詳細|説明|件名|タイトル|予定名|場所|会場|予定)$/i.test(compact)) return true
  if (/^(?:次(?:の)?|以下(?:の)?|下記(?:の)?|つぎの)$/.test(compact)) return true
  if (
    /^(?:(?:次(?:の)?|以下(?:の)?|下記(?:の)?|つぎの))?(?:言葉|文言|文章|テキスト|内容|詳細|説明|件名|タイトル|予定名|場所|会場)/.test(compact) &&
    /(変更|修正|更新|変えて|して|書き換えて|置き換えて|記載|記入|追記|追加|入れて|書いて|載せて|残して)/.test(compact)
  ) {
    return true
  }
  return false
}

function isLikelyLowQualityRuleBasedUpdateCommand(
  command: CalendarUpdateCommand,
  rawText: string,
): boolean {
  const normalized = normalizeForRuleParsing(rawText)
  if (command.title) {
    const cleaned = cleanCalendarTitle(command.title)
    if (!cleaned || cleaned === '予定') return true
    if (isLikelyInstructionOnlyFieldValue(cleaned)) return true
  }
  if (command.location) {
    const cleaned = cleanCalendarLocation(command.location)
    if (!cleaned) return true
    if (isLikelyInstructionOnlyFieldValue(cleaned)) return true
  }
  if (command.description) {
    const cleaned = cleanCalendarDescription(command.description)
    if (!cleaned) return true
    if (isLikelyInstructionOnlyFieldValue(cleaned)) return true
    if (
      cleaned.length <= 3 &&
      /(内容|詳細|説明|記載|記入|追記|追加|入れて|書いて|載せて|残して)/.test(normalized)
    ) {
      return true
    }
  }
  return false
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

function isHighConfidenceAiCalendarIntent(intent: AiCalendarIntent, minConfidence = AI_MIN_CONFIDENCE): boolean {
  return isValidAiCalendarIntent(intent) && intent.confidence >= minConfidence
}

function isConfirmableAiCalendarIntent(intent: AiCalendarIntent, minConfidence = AI_CONFIRMATION_MIN_CONFIDENCE): boolean {
  return isValidAiCalendarIntent(intent) && intent.confidence >= minConfidence
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

function isMissingCalendarUpdatePendingTableError(error: any): boolean {
  const code = String(error?.code ?? '')
  if (code === '42P01') return true
  const text = `${String(error?.message ?? '')} ${String(error?.details ?? '')}`.toLowerCase()
  return text.includes('calendar_update_pending_targets')
    && (text.includes('does not exist') || text.includes('relation'))
}

function isMissingCalendarUpdatePendingSourceIdsColumnError(error: any): boolean {
  const code = String(error?.code ?? '')
  if (code === '42703') return true
  const text = `${String(error?.message ?? '')} ${String(error?.details ?? '')}`.toLowerCase()
  return text.includes('source_line_message_ids_json')
}

function normalizePendingCalendarUpdateTargetEntries(raw: unknown): PendingCalendarUpdateTargetEntry[] {
  if (!Array.isArray(raw)) return []
  const out: PendingCalendarUpdateTargetEntry[] = []
  for (const item of raw) {
    const eventId = String((item as any)?.event_id ?? '').trim()
    if (!eventId) continue
    const summary = cleanCalendarTitle(String((item as any)?.summary ?? ''))
    const dateRaw = normalizeForRuleParsing(String((item as any)?.date ?? '')).trim()
    const timeRaw = normalizeForRuleParsing(String((item as any)?.time ?? '')).trim()
    const date = isValidDate(dateRaw) ? dateRaw : undefined
    const time = isValidTime(timeRaw) ? timeRaw : undefined
    out.push({
      event_id: eventId,
      summary: summary || '(無題)',
      ...(date ? { date } : {}),
      ...(time ? { time } : {}),
    })
  }
  return out
}

function normalizeLineMessageIds(raw: unknown): string[] {
  if (!Array.isArray(raw)) return []
  const unique = new Set<string>()
  const out: string[] = []
  for (const item of raw) {
    const id = String(item ?? '').trim()
    if (!id || unique.has(id)) continue
    unique.add(id)
    out.push(id)
  }
  return out
}

async function fetchPendingCalendarUpdateContext(
  supabase: ReturnType<typeof createClient>,
  roomId: string,
  userId: string | null,
): Promise<PendingCalendarUpdateContext | null> {
  const conversationKey = buildConversationKey(roomId, userId)
  const { data, error } = await supabase
    .from(CALENDAR_UPDATE_PENDING_TABLE)
    .select('id, conversation_key, target_events_json, source_line_message_ids_json, expires_at')
    .eq('conversation_key', conversationKey)
    .eq('status', 'pending')
    .order('created_at', { ascending: false })
    .limit(1)
    .maybeSingle()
  let row = data as any
  if (error) {
    if (!isMissingCalendarUpdatePendingSourceIdsColumnError(error)) {
      if (!isMissingCalendarUpdatePendingTableError(error)) {
        console.error('Failed to fetch pending calendar update context:', error)
      }
      return null
    }
    const fallback = await supabase
      .from(CALENDAR_UPDATE_PENDING_TABLE)
      .select('id, conversation_key, target_events_json, expires_at')
      .eq('conversation_key', conversationKey)
      .eq('status', 'pending')
      .order('created_at', { ascending: false })
      .limit(1)
      .maybeSingle()
    if (fallback.error) {
      if (!isMissingCalendarUpdatePendingTableError(fallback.error)) {
        console.error('Failed to fetch pending calendar update context (fallback):', fallback.error)
      }
      return null
    }
    row = fallback.data as any
  }
  if (!row) return null

  const targetEvents = normalizePendingCalendarUpdateTargetEntries(row.target_events_json)
  if (targetEvents.length === 0) return null

  return {
    id: String(row.id ?? ''),
    conversation_key: String(row.conversation_key ?? ''),
    target_events: targetEvents,
    source_line_message_ids: normalizeLineMessageIds(row.source_line_message_ids_json),
    expires_at: String(row.expires_at ?? ''),
  }
}

async function resolvePendingCalendarUpdateContext(
  supabase: ReturnType<typeof createClient>,
  pending: PendingCalendarUpdateContext,
  status: 'resolved' | 'cancelled' | 'expired' | 'superseded',
): Promise<void> {
  const { error } = await supabase
    .from(CALENDAR_UPDATE_PENDING_TABLE)
    .update({
      status,
      resolved_at: new Date().toISOString(),
    })
    .eq('id', Number(pending.id))
    .eq('status', 'pending')
  if (error && !isMissingCalendarUpdatePendingTableError(error)) {
    console.error('Failed to resolve pending calendar update context:', error)
  }
}

async function savePendingCalendarUpdateContext(
  supabase: ReturnType<typeof createClient>,
  roomId: string,
  userId: string | null,
  events: GoogleCalendarEvent[],
  timezone: string,
): Promise<PendingCalendarUpdateTargetEntry[]> {
  const entries = events
    .map((event) => {
      const eventId = String(event.id ?? '').trim()
      if (!eventId) return null
      const detail = formatEventDetailBlock(event, timezone)
      const dateCandidate = normalizeForRuleParsing(detail.date).replace(/\//g, '-')
      const timeCandidate = normalizeForRuleParsing(detail.time).split('-')[0]?.trim() ?? ''
      const date = isValidDate(dateCandidate) ? dateCandidate : undefined
      const time = isValidTime(timeCandidate) ? timeCandidate : undefined
      return {
        event_id: eventId,
        summary: cleanCalendarTitle(String(event.summary ?? '(無題)')),
        ...(date ? { date } : {}),
        ...(time ? { time } : {}),
      }
    })
    .filter((entry): entry is PendingCalendarUpdateTargetEntry => !!entry)
    .slice(0, 20)

  if (entries.length === 0) return []

  const conversationKey = buildConversationKey(roomId, userId)
  const nowIso = new Date().toISOString()
  const expiresAt = new Date(Date.now() + PENDING_CONFIRMATION_TTL_MIN * 60 * 1000).toISOString()

  const { error: supersedeError } = await supabase
    .from(CALENDAR_UPDATE_PENDING_TABLE)
    .update({
      status: 'superseded',
      resolved_at: nowIso,
    })
    .eq('conversation_key', conversationKey)
    .eq('status', 'pending')
  if (supersedeError && !isMissingCalendarUpdatePendingTableError(supersedeError)) {
    console.error('Failed to supersede pending calendar update context:', supersedeError)
  }

  const payloadBase = {
    conversation_key: conversationKey,
    room_id: roomId,
    user_id: userId,
    target_events_json: entries,
    status: 'pending',
    expires_at: expiresAt,
  }
  const { error: insertError } = await supabase
    .from(CALENDAR_UPDATE_PENDING_TABLE)
    .insert({ ...payloadBase, source_line_message_ids_json: [] })
  if (insertError) {
    if (isMissingCalendarUpdatePendingSourceIdsColumnError(insertError)) {
      const fallbackInsert = await supabase
        .from(CALENDAR_UPDATE_PENDING_TABLE)
        .insert(payloadBase)
      if (fallbackInsert.error && !isMissingCalendarUpdatePendingTableError(fallbackInsert.error)) {
        console.error('Failed to save pending calendar update context (fallback):', fallbackInsert.error)
      }
    } else if (!isMissingCalendarUpdatePendingTableError(insertError)) {
      console.error('Failed to save pending calendar update context:', insertError)
    }
  }
  return entries
}

async function attachLineMessageIdsToPendingCalendarUpdateContext(
  supabase: ReturnType<typeof createClient>,
  roomId: string,
  userId: string | null,
  lineMessageIds: string[],
): Promise<void> {
  const ids = normalizeLineMessageIds(lineMessageIds)
  if (ids.length === 0) return
  const conversationKey = buildConversationKey(roomId, userId)
  const { data, error } = await supabase
    .from(CALENDAR_UPDATE_PENDING_TABLE)
    .select('id, source_line_message_ids_json')
    .eq('conversation_key', conversationKey)
    .eq('status', 'pending')
    .order('created_at', { ascending: false })
    .limit(1)
    .maybeSingle()
  if (error) {
    if (!isMissingCalendarUpdatePendingTableError(error) && !isMissingCalendarUpdatePendingSourceIdsColumnError(error)) {
      console.error('Failed to fetch pending calendar update context for message-id attach:', error)
    }
    return
  }
  if (!data) return
  const current = normalizeLineMessageIds((data as any).source_line_message_ids_json)
  const merged = normalizeLineMessageIds([...current, ...ids])
  const { error: updateError } = await supabase
    .from(CALENDAR_UPDATE_PENDING_TABLE)
    .update({ source_line_message_ids_json: merged })
    .eq('id', Number((data as any).id ?? 0))
    .eq('status', 'pending')
  if (
    updateError &&
    !isMissingCalendarUpdatePendingTableError(updateError) &&
    !isMissingCalendarUpdatePendingSourceIdsColumnError(updateError)
  ) {
    console.error('Failed to attach line message ids to pending calendar update context:', updateError)
  }
}

function isMissingLibraryPendingTableError(error: any): boolean {
  const code = String(error?.code ?? '')
  if (code === '42P01') return true
  const text = `${String(error?.message ?? '')} ${String(error?.details ?? '')}`.toLowerCase()
  return text.includes('message_search_library_pending_confirmations')
    && (text.includes('does not exist') || text.includes('relation'))
}

async function fetchPendingLibrarySearchConfirmation(
  supabase: ReturnType<typeof createClient>,
  roomId: string,
  userId: string | null,
): Promise<PendingLibrarySearchConfirmation | null> {
  const conversationKey = buildConversationKey(roomId, userId)
  const { data, error } = await supabase
    .from(LIBRARY_SEARCH_PENDING_TABLE)
    .select('id, conversation_key, keyword, search_days, search_scope, retention_adjusted, expires_at')
    .eq('conversation_key', conversationKey)
    .eq('status', 'pending')
    .order('created_at', { ascending: false })
    .limit(1)
    .maybeSingle()

  if (error) {
    if (!isMissingLibraryPendingTableError(error)) {
      console.error('Failed to fetch library search pending:', error)
    }
    return null
  }
  if (!data) return null
  const daysRaw = Number((data as any).search_days)
  const days = isSupportedMessageRetentionDays(daysRaw) ? daysRaw : DEFAULT_MESSAGE_RETENTION_DAYS
  const scopeRaw = String((data as any).search_scope ?? '').trim()
  const scope: MessageSearchScope = scopeRaw === 'current_room' ? 'current_room' : 'all_rooms'
  return {
    id: String((data as any).id ?? ''),
    conversation_key: String((data as any).conversation_key ?? ''),
    keyword: String((data as any).keyword ?? ''),
    search_days: days,
    search_scope: scope,
    retention_adjusted: Boolean((data as any).retention_adjusted),
    expires_at: String((data as any).expires_at ?? ''),
  }
}

async function resolvePendingLibrarySearchConfirmation(
  supabase: ReturnType<typeof createClient>,
  pending: PendingLibrarySearchConfirmation,
  status: 'confirmed' | 'cancelled' | 'expired' | 'superseded',
): Promise<void> {
  const { error } = await supabase
    .from(LIBRARY_SEARCH_PENDING_TABLE)
    .update({
      status,
      resolved_at: new Date().toISOString(),
    })
    .eq('id', Number(pending.id))
    .eq('status', 'pending')
  if (error && !isMissingLibraryPendingTableError(error)) {
    console.error('Failed to resolve library search pending:', error)
  }
}

async function savePendingLibrarySearchConfirmation(
  supabase: ReturnType<typeof createClient>,
  roomId: string,
  userId: string | null,
  command: MessageSearchCommand,
  effectiveSearchDays: MessageRetentionDays,
  retentionAdjusted: boolean,
): Promise<boolean> {
  const conversationKey = buildConversationKey(roomId, userId)
  const nowIso = new Date().toISOString()
  const expiresAt = new Date(Date.now() + PENDING_CONFIRMATION_TTL_MIN * 60 * 1000).toISOString()

  const { error: supersedeError } = await supabase
    .from(LIBRARY_SEARCH_PENDING_TABLE)
    .update({
      status: 'superseded',
      resolved_at: nowIso,
    })
    .eq('conversation_key', conversationKey)
    .eq('status', 'pending')
  if (supersedeError && !isMissingLibraryPendingTableError(supersedeError)) {
    console.error('Failed to supersede library search pending:', supersedeError)
  }

  const { error: insertError } = await supabase
    .from(LIBRARY_SEARCH_PENDING_TABLE)
    .insert({
      conversation_key: conversationKey,
      room_id: roomId,
      user_id: userId,
      keyword: command.keyword,
      search_days: effectiveSearchDays,
      search_scope: command.scope,
      retention_adjusted: retentionAdjusted,
      status: 'pending',
      expires_at: expiresAt,
    })

  if (insertError) {
    if (!isMissingLibraryPendingTableError(insertError)) {
      console.error('Failed to save library search pending:', insertError)
    }
    return false
  }
  return true
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
    .select('id, conversation_key, source_text, title, location, date, time, duration_min, confidence, reason, expires_at')
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
    location: data.location ? String(data.location) : null,
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
  const expiresAt = new Date(Date.now() + CALENDAR_PENDING_CONFIRMATION_TTL_MIN * 60 * 1000).toISOString()

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
      location: intent.location ?? null,
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
    console.error('Pending table is missing; legacy fallback is disabled. Apply DB migrations.', insertError)
    return false
  }
  return true
}

function buildPendingCalendarConfirmationPrompt(
  intent: AiCalendarIntent,
  _timezone: string,
): string {
  const lines = [
    '予定候補を見つけました。登録しますか？',
    ...buildCalendarDetailTemplateLines({
      title: intent.title,
      date: formatDateForCalendarTemplate(intent.date),
      time: formatTimeRangeForCalendarTemplate(intent.date, intent.time, intent.durationMin),
      location: intent.location ?? null,
      content: null,
    }),
  ]
  lines.push('')
  lines.push('修正する場合は「場所をmarugoに変更」「時間を19:00に変更」のように送ってください。')
  lines.push('「はい」で登録 / 「いいえ」でキャンセル')
  lines.push(`※返信がない場合は、${CALENDAR_PENDING_CONFIRMATION_TTL_MIN}分後に件名末尾へ「（仮）」を付けて自動登録します。`)
  return lines.join('\n')
}

function appendProvisionalSuffixToTitle(title: string): string {
  const cleaned = cleanCalendarTitle(title)
  if (/[（(]仮[）)]$/.test(cleaned)) return cleaned
  return cleanCalendarTitle(`${cleaned}（仮）`)
}

function buildCalendarCreateCommandFromPending(
  pending: PendingCalendarConfirmation,
  useProvisionalTitle = false,
): CalendarCreateCommand {
  const explicitPendingTitle = normalizeEventTitleCandidate(pending.title) || cleanCalendarTitle(pending.title)
  const explicitPendingLocation = cleanCalendarLocation(pending.location ?? '') ?? null
  const resolvedPendingDetails = resolveAiCalendarDetails(
    pending.source_text ?? '',
    explicitPendingTitle,
    explicitPendingLocation ?? undefined,
  )
  const resolvedPendingTitle = explicitPendingTitle || (resolvedPendingDetails.titleSource === 'default'
    ? cleanCalendarTitle(pending.title)
    : resolvedPendingDetails.title)
  const resolvedPendingLocation = explicitPendingLocation ?? resolvedPendingDetails.location ?? null
  const finalTitle = useProvisionalTitle
    ? appendProvisionalSuffixToTitle(resolvedPendingTitle)
    : resolvedPendingTitle
  return {
    kind: 'create',
    date: pending.date,
    time: pending.time,
    durationMin: pending.duration_min,
    title: finalTitle,
    ...(resolvedPendingLocation ? { location: resolvedPendingLocation } : {}),
  }
}

function buildPendingCalendarIntent(pending: PendingCalendarConfirmation): AiCalendarIntent {
  return {
    shouldCreate: true,
    confidence: pending.confidence,
    title: cleanCalendarTitle(pending.title),
    ...(pending.location ? { location: pending.location } : {}),
    date: pending.date,
    time: pending.time,
    durationMin: pending.duration_min,
    reason: pending.reason ?? '',
  }
}

function appendCorrectionToPendingSourceText(sourceText: string | null | undefined, correctionText: string): string {
  const base = normalizeForRuleParsing(sourceText ?? '').trim()
  const correction = normalizeForRuleParsing(correctionText).trim()
  if (!base) return correction
  if (!correction) return base
  return `${base}\n[修正] ${correction}`
}

function looksLikePendingCorrectionText(rawText: string): boolean {
  const compact = normalizeForRuleParsing(rawText).replace(/\s+/g, '')
  if (!compact) return false
  if (/(訂正|修正|変更|変えて|直して|更新|場所|会場|時間|時刻|開始|日付|日にち|日時|件名|タイトル|予定名|内容)/.test(compact)) {
    return true
  }
  const normalized = normalizeForRuleParsing(rawText).trim()
  if (!normalized) return false
  if (/(?:件名|タイトル|予定名|内容|予定)?(?:の)?[^\n。]{1,24}を[^\n。]{1,40}(?:に|へ)(?:変更|修正|更新|して|直して|変えて|してください|します|する)/i.test(normalized)) {
    return true
  }
  return /(試飲会|打ち合わせ|打合せ|会議|ミーティング|meeting|mtg|商談|面談|イベント|予約|アポ|グランドオープン|オープン|ランチ|ディナー).*(?:に|へ)(?:変更|修正|更新|して|直して|変えて|してください|します|する)/i
    .test(normalized)
}

function extractCorrectionLocation(rawText: string): string | undefined {
  const normalized = normalizeForRuleParsing(rawText).trim()
  if (!normalized) return undefined
  const patterns = [
    /(?:場所|会場|開催場所|開催会場)\s*(?:を|は)?\s*([^\n。]+?)\s*(?:に(?:変更|して|変えて|してください)|へ(?:変更|して|変えて|してください)|です|でお願いします|でおねがい|にします|にする)/i,
    /(?:場所|会場|開催場所|開催会場)\s*(?:を|は)?\s*([^\n。]+?)\s*(?:で|に|へ)\s*$/i,
    /(?:場所|会場|開催場所|開催会場)\s*[：:]\s*([^\n。]+)/i,
  ]
  for (const pattern of patterns) {
    const match = normalized.match(pattern)
    if (!match || !match[1]) continue
    let candidate = String(match[1]).trim()
    const fromTo = candidate.match(/(?:.+?)から\s*(.+)$/)
    if (fromTo && fromTo[1]) candidate = fromTo[1].trim()
    candidate = candidate.replace(/\s*(?:に|へ)\s*$/g, '').trim()
    const cleaned = cleanCalendarLocation(candidate)
    if (cleaned) return cleaned
  }
  return undefined
}

function extractCorrectionTitle(rawText: string): string | undefined {
  const normalized = normalizeForRuleParsing(rawText).trim()
  if (!normalized) return undefined
  const patterns = [
    /(?:件名|タイトル|予定名)\s*(?:を|は)?\s*([^\n。]+?)\s*(?:に(?:変更|して|変えて|してください)|です|でお願いします|にします|にする)/i,
    /(?:件名|タイトル|予定名)\s*[：:]\s*([^\n。]+)/i,
  ]
  for (const pattern of patterns) {
    const match = normalized.match(pattern)
    if (!match || !match[1]) continue
    const cleaned = normalizeEventTitleCandidate(match[1]) || cleanCalendarTitle(match[1])
    if (cleaned && cleaned !== '予定') return cleaned
  }

  const replacePattern = /(?:件名|タイトル|予定名|予定)?(?:の)?\s*([^\n。]{1,40}?)\s*を\s*([^\n。]{1,80}?)\s*(?:に|へ)(?:変更|修正|更新|して|直して|変えて|してください|します|する)/i
  const replaceMatch = normalized.match(replacePattern)
  if (replaceMatch && replaceMatch[2]) {
    const fromTokenCompact = normalizeForRuleParsing(String(replaceMatch[1] ?? '')).replace(/\s+/g, '')
    if (!/^(時間|時刻|開始|日付|日にち|日時|場所|会場|所要|内容|詳細|説明|duration|location|description)$/i.test(fromTokenCompact)) {
      const toRaw = String(replaceMatch[2] ?? '')
        .replace(/^[\s、,。．:：\-]+/, '')
        .trim()
      const looksLikeDateOrTime = isLikelyDateOrTimeExpressionForTitleCandidate(toRaw)
      if (!looksLikeDateOrTime) {
        const cleaned = normalizeEventTitleCandidate(toRaw) || cleanCalendarTitle(toRaw)
        if (cleaned && cleaned !== '予定') return cleaned
      }
    }
  }

  const directToPattern = /^([^\n。]{1,80}?)\s*(?:に|へ)(?:変更|修正|更新|して|直して|変えて|してください|します|する)$/
  const directToMatch = normalized.match(directToPattern)
  if (directToMatch && directToMatch[1]) {
    const candidateRaw = String(directToMatch[1]).trim()
    const looksLikeDateOrTime = isLikelyDateOrTimeExpressionForTitleCandidate(candidateRaw)
    const hasFieldWord = /(時間|時刻|開始|日付|日にち|日時|場所|会場|所要|内容|詳細|説明|duration|location|description)/i.test(candidateRaw)
    if (!looksLikeDateOrTime && !hasFieldWord) {
      const cleaned = normalizeEventTitleCandidate(candidateRaw) || cleanCalendarTitle(candidateRaw)
      if (cleaned && cleaned !== '予定') return cleaned
    }
  }
  return undefined
}

function isLikelyDateOrTimeExpressionForTitleCandidate(raw: string): boolean {
  const normalized = normalizeForRuleParsing(String(raw ?? '')).trim()
  if (!normalized) return false
  const compact = normalized.replace(/\s+/g, '')
  return (
    isValidDate(normalized)
    || isValidTime(normalized)
    || !!parseFlexibleTimeToken(normalized)
    || /^(\d{4}[\/.\-年]\d{1,2}[\/.\-月]\d{1,2}日?|\d{1,2}月\d{1,2}日|\d{1,2}日)$/.test(compact)
    || /(^|[^\d])\d{1,2}日(?:$|[^\d])/u.test(compact)
  )
}

function extractCorrectionDescription(rawText: string): string | undefined {
  const normalized = normalizeForRuleParsing(rawText).trim()
  if (!normalized) return undefined

  const quoted = normalized.match(/(?:内容|詳細|説明)[^「『"']*[「『"']([^"'」』\n]{1,500})[」』"']/i)
  if (quoted && quoted[1]) {
    const cleanedQuoted = cleanCalendarDescription(quoted[1])
    if (cleanedQuoted) return cleanedQuoted
  }

  const patterns = [
    /(?:内容|詳細|説明)\s*(?:を|は|に|へ)?\s*(?:次(?:の)?|以下(?:の)?|下記(?:の)?|つぎの)?\s*(?:言葉|文言|文章|テキスト)\s*(?:に|へ)?\s*(?:変更|修正|更新|変えて|して|書き換えて|置き換えて)\s*[、,。．:：\-]?\s*([^\n。]+)/i,
    /(?:内容|詳細|説明)\s*(?:を|は)?\s*(?:次(?:の)?(?:よう)?|以下(?:の)?|下記(?:の)?|つぎの)\s*(?:ように)?\s*(?:変更|修正|更新|して|変えて)\s*[、,。．:：\-]?\s*([^\n。]+)/i,
    /(?:内容|詳細|説明)\s*(?:に|へ)\s*(?:(?:次(?:の)?|以下(?:の)?|下記(?:の)?|つぎの)\s*[、,。．:：\-]?\s*)?([^\n。]+?)\s*(?:と)?\s*(?:記載|記入|追記|追加|入れて|書いて|載せて|残して)(?:ください|下さい|して|ほしい|欲しい)?/i,
    /(?:内容|詳細|説明)\s*(?:を|は)?\s*(?:(?:次(?:の)?|以下(?:の)?|下記(?:の)?|つぎの)\s*[、,。．:：\-]?\s*)?([^\n。]+?)\s*(?:と)?\s*(?:記載|記入|追記|追加|入れて|書いて|載せて|残して)(?:ください|下さい|して|ほしい|欲しい)?/i,
    /(?:内容|詳細|説明)\s*(?:を|は)?\s*([^\n。]+?)\s*(?:に(?:変更|して|変えて|してください)|です|でお願いします|にします|にする)/i,
    /(?:内容|詳細|説明)\s*[：:]\s*([^\n]+)/i,
  ]
  for (const pattern of patterns) {
    const match = normalized.match(pattern)
    if (!match || !match[1]) continue
    const cleaned = cleanCalendarDescription(match[1])
    if (cleaned) return cleaned
  }
  return undefined
}

function looksLikeEventTextForPendingCorrection(rawText: string): boolean {
  const normalized = normalizeForRuleParsing(rawText).trim()
  if (!normalized) return false
  if (parseDateTimeSlotFromLine(normalized)) return true
  return /(試飲会|打ち合わせ|打合せ|会議|ミーティング|meeting|mtg|商談|面談|イベント|予約|アポ|グランドオープン|オープン|ランチ|ディナー)/i
    .test(normalized)
}

function extractCorrectionDate(rawText: string, baseDate = new Date()): string | null {
  const normalized = normalizeForRuleParsing(rawText).trim()
  if (!normalized) return null

  let match = normalized.match(/(\d{4})[\/.\-年](\d{1,2})[\/.\-月](\d{1,2})日?/)
  if (match) {
    const year = Number(match[1])
    const month = Number(match[2])
    const day = Number(match[3])
    return toIsoDateStringSafe(year, month, day)
  }

  match = normalized.match(/(\d{1,2})月(\d{1,2})日/)
  if (match) {
    const { year } = getJstYearMonth(baseDate)
    const month = Number(match[1])
    const day = Number(match[2])
    return toIsoDateStringSafe(year, month, day)
  }

  match = normalized.match(/(?:日付|日にち|日程|日時)\s*(?:を|は|:|：)?\s*(\d{1,2})日/)
  if (match) {
    const { year, month } = getJstYearMonth(baseDate)
    const day = Number(match[1])
    return toIsoDateStringSafe(year, month, day)
  }

  match = normalized.match(/(\d{1,2})日(?:に|へ)?(?:変更|修正|更新|して|にして|戻して|に戻して|へ戻して)/)
  if (match) {
    const { year, month } = getJstYearMonth(baseDate)
    const day = Number(match[1])
    return toIsoDateStringSafe(year, month, day)
  }

  return null
}

function extractCorrectionTime(rawText: string): string | null {
  const normalized = normalizeForRuleParsing(rawText).trim()
  if (!normalized) return null
  const patterns = [
    /(?:時間|時刻|開始(?:時間|時刻)?|開始|スタート)\s*(?:を|は|:|：)?\s*([0-9]{1,2}(?::[0-9]{2}|時(?:\s*[0-9]{1,2}分?)?))/i,
    /([0-9]{1,2}(?::[0-9]{2}|時(?:\s*[0-9]{1,2}分?)?))\s*(?:に(?:変更|して|変えて|します|する)|開始|スタート|です|でお願いします)/i,
    /^([0-9]{1,2}(?::[0-9]{2}|時(?:\s*[0-9]{1,2}分?)?))$/i,
  ]
  for (const pattern of patterns) {
    const match = normalized.match(pattern)
    if (!match || !match[1]) continue
    const parsed = parseFlexibleTimeToken(match[1])
    if (!parsed) continue
    const time = `${String(parsed.hour).padStart(2, '0')}:${String(parsed.minute).padStart(2, '0')}`
    if (isValidTime(time)) return time
  }
  return null
}

function tryBuildPendingCorrection(
  rawText: string,
  pending: PendingCalendarConfirmation,
): {
  updates: {
    source_text: string
    title?: string
    location?: string | null
    date?: string
    time?: string
    duration_min?: number
  }
  changedFields: string[]
  guidance?: string
} | null {
  const text = String(rawText ?? '').trim()
  if (!text) return null

  const hasCue = looksLikePendingCorrectionText(text)
  const slot = parseDateTimeSlotFromLine(text)
  const dateFromSlot = slot?.date
  const timeFromSlot = slot?.time
  const durationFromSlot = slot?.durationMin
  const dateOnly = dateFromSlot ? null : extractCorrectionDate(text)
  const timeOnly = timeFromSlot ? null : extractCorrectionTime(text)
  const location = extractCorrectionLocation(text)
  const explicitTitle = extractCorrectionTitle(text)
  const inferredTitle = !explicitTitle && looksLikeEventTextForPendingCorrection(text)
    ? resolveAiCalendarDetails(text, pending.title, pending.location ?? undefined).title
    : undefined
  const title = explicitTitle || inferredTitle

  const updates: {
    source_text: string
    title?: string
    location?: string | null
    date?: string
    time?: string
    duration_min?: number
  } = {
    source_text: appendCorrectionToPendingSourceText(pending.source_text, text),
  }
  const changedFields: string[] = []

  const nextDate = dateFromSlot ?? dateOnly
  if (nextDate && nextDate !== pending.date) {
    updates.date = nextDate
    changedFields.push('日付')
  }

  const nextTime = timeFromSlot ?? timeOnly
  if (nextTime && nextTime !== pending.time) {
    updates.time = nextTime
    changedFields.push('時刻')
  }

  if (
    typeof durationFromSlot === 'number' &&
    Number.isInteger(durationFromSlot) &&
    durationFromSlot > 0 &&
    durationFromSlot <= MAX_DURATION_MIN &&
    durationFromSlot !== pending.duration_min
  ) {
    updates.duration_min = durationFromSlot
    changedFields.push('時間幅')
  }

  if (title && cleanCalendarTitle(title) !== cleanCalendarTitle(pending.title)) {
    updates.title = cleanCalendarTitle(title)
    changedFields.push('件名')
  }

  if (typeof location !== 'undefined') {
    const currentLocation = cleanCalendarLocation(pending.location ?? '')
    if (location !== currentLocation) {
      updates.location = location
      changedFields.push('場所')
    }
  }

  if (changedFields.length === 0) {
    if (!hasCue) return null
    return {
      updates,
      changedFields,
      guidance: '修正内容を読み取れませんでした。例: 「場所をmarugoに変更」「時間を19:00に変更」',
    }
  }

  return { updates, changedFields }
}

async function updatePendingCalendarConfirmation(
  supabase: any,
  pending: PendingCalendarConfirmation,
  updates: {
    source_text: string
    title?: string
    location?: string | null
    date?: string
    time?: string
    duration_min?: number
  },
): Promise<PendingCalendarConfirmation | null> {
  const next: PendingCalendarConfirmation = {
    ...pending,
    source_text: updates.source_text,
    title: updates.title ?? pending.title,
    location: typeof updates.location !== 'undefined' ? updates.location : pending.location ?? null,
    date: updates.date ?? pending.date,
    time: updates.time ?? pending.time,
    duration_min: typeof updates.duration_min === 'number' ? updates.duration_min : pending.duration_min,
  }

  if (pending.storage === 'pending_table') {
    const { error } = await supabase
      .from(CALENDAR_PENDING_TABLE)
      .update({
        source_text: next.source_text ?? '',
        title: next.title,
        location: next.location ?? null,
        date: next.date,
        time: next.time,
        duration_min: next.duration_min,
      })
      .eq('id', Number(pending.id))
      .eq('status', 'pending')
    if (error) {
      console.error('Failed to update pending confirmation:', error)
      return null
    }
    return next
  }

  const payloadContent = encodeLegacyPendingContent({
    conversationKey: next.conversation_key,
    sourceText: next.source_text ?? null,
    title: next.title,
    location: next.location ?? null,
    date: next.date,
    time: next.time,
    durationMin: next.duration_min,
    confidence: next.confidence,
    reason: next.reason ?? null,
    expiresAt: next.expires_at,
  })
  const { error } = await supabase
    .from('line_messages')
    .update({ content: payloadContent })
    .eq('id', next.id)
    .eq('processed', true)
  if (error) {
    console.error('Failed to update legacy pending confirmation:', error)
    return null
  }
  return next
}

async function tryHandlePendingCalendarConfirmation(
  text: string,
  supabase: any,
  env: CalendarEnv,
  roomId: string,
  userId: string | null,
  sourceMeta?: CalendarSourceMeta,
): Promise<string | null> {
  const pending = await fetchPendingCalendarConfirmation(supabase, roomId, userId)
  if (!pending) return null

  const decision = normalizeConfirmationDecision(text)
  const expireAtMs = new Date(pending.expires_at).getTime()
  const isExpired = !Number.isFinite(expireAtMs) || Date.now() >= expireAtMs

  if (decision === 'no') {
    await resolvePendingCalendarConfirmation(supabase, pending, 'cancelled')
    return '予定登録をキャンセルしました。'
  }

  if (isExpired && decision !== 'yes') {
    return [
      `確認期限（${CALENDAR_PENDING_CONFIRMATION_TTL_MIN}分）を過ぎています。`,
      '自動登録はバックグラウンド処理で順次実行されます。反映まで少しお待ちください。',
    ].join('\n')
  }

  if (decision !== 'yes') {
    const correction = tryBuildPendingCorrection(text, pending)
    if (!correction) return null
    if (correction.guidance) {
      return correction.guidance
    }

    const updatedPending = await updatePendingCalendarConfirmation(supabase, pending, correction.updates)
    if (!updatedPending) {
      return '予定候補の修正保存に失敗しました。もう一度修正内容を送ってください。'
    }
    const fields = correction.changedFields.join('・')
    const lines = [
      fields ? `予定候補を更新しました（${fields}）。` : '予定候補を更新しました。',
      buildPendingCalendarConfirmationPrompt(buildPendingCalendarIntent(updatedPending), env.timezone),
    ]
    return lines.join('\n')
  }

  const command = buildCalendarCreateCommandFromPending(pending, false)
  const result = await createCalendarEvent(command, env, roomId, userId, undefined, sourceMeta)
  if (!result.ok) {
    return `予定登録に失敗しました。${result.error}\n再試行する場合は「はい」、中止する場合は「いいえ」を送ってください。`
  }

  if (result.eventId) {
    const pendingEvent: GoogleCalendarEvent = {
      id: result.eventId,
      summary: result.summary,
      ...(command.location ? { location: command.location } : {}),
      ...(result.savedStartRaw ? { start: { dateTime: result.savedStartRaw, timeZone: result.savedStartTimeZone ?? env.timezone } } : {}),
      ...(result.savedEndRaw ? { end: { dateTime: result.savedEndRaw, timeZone: result.savedEndTimeZone ?? env.timezone } } : {}),
    }
    await savePendingCalendarUpdateContext(
      supabase,
      roomId,
      userId,
      [pendingEvent],
      env.timezone,
    )
  }

  await resolvePendingCalendarConfirmation(supabase, pending, 'confirmed')
  const registeredDate = formatDateOnlyForLine(result.startDate, env.timezone)
  const registeredTime = `${formatTimeOnlyForLine(result.startDate, env.timezone)}-${formatTimeOnlyForLine(result.endDate, env.timezone)}`
  const lines = [
    '確認済みの予定を登録しました。',
    ...buildCalendarDetailTemplateLines({
      title: result.summary,
      date: registeredDate,
      time: registeredTime,
      location: command.location ?? null,
      content: null,
    }),
  ]
  return lines.join('\n')
}

function looksLikeCalendarUpdateConversationText(rawText: string): boolean {
  const normalized = normalizeForRuleParsing(rawText).trim()
  if (!normalized) return false
  const compact = normalized.replace(/\s+/g, '')
  if (!compact) return false
  if (/^予定変更/.test(compact)) return false
  if (parseCalendarCommand(rawText).matched) return false
  const hasChangeCue = /(変更|修正|更新|ずらして|ずらす|移動|直して|直す|変えて|変える|にして|前倒し|後ろ倒し|早めて|早める|遅らせて|遅らせる|リスケ|書き換えて|置き換えて|記載|記入|追記|追加|書いて|入れて|載せて|残して|メモして)/.test(compact)
  const hasFieldCue = /(時間|時刻|開始|日付|日にち|日時|件名|タイトル|予定名|内容|場所|会場|所要|分|duration|location)/i.test(compact)
  const hasAssignmentCue = /(?:時間|時刻|開始|日付|日にち|日時|件名|タイトル|予定名|内容|場所|会場|所要|duration|location)\s*(?:は|を|:|：|=)/i
    .test(normalized)
  const hasTitleRewriteCue = /(?:件名|タイトル|予定名|内容|予定)?(?:の)?[^\n。]{1,24}を[^\n。]{1,40}(?:に|へ)(?:変更|修正|更新|して|直して|変えて|してください|します|する)/i
    .test(normalized)
  const hasEventWordCue = /(試飲会|打ち合わせ|打合せ|会議|ミーティング|meeting|mtg|商談|面談|イベント|予約|アポ|グランドオープン|オープン|ランチ|ディナー|研修|セミナー|講習会|説明会)/i
    .test(normalized)
  const hasTargetCue = extractPendingCalendarTargetIndex(rawText) != null
  const hasDayOnlyUpdateCue = /(\d{1,2})日(?:に|へ)?(?:変更|修正|更新|して|にして|戻して|に戻して|へ戻して)/.test(compact)
  if (hasTitleRewriteCue) return true
  if (hasDayOnlyUpdateCue) return true
  if (hasFieldCue && (hasChangeCue || hasAssignmentCue || hasTargetCue)) return true
  if (hasEventWordCue && hasChangeCue) return true
  if (hasTargetCue && hasChangeCue) return true
  return false
}

function extractPendingCalendarTargetIndex(rawText: string): number | null {
  const normalized = normalizeForRuleParsing(rawText)
  const match = normalized.match(/(\d{1,2})\s*件目/)
  if (!match || !match[1]) return null
  const idx = Number(match[1])
  if (!Number.isInteger(idx) || idx <= 0) return null
  return idx - 1
}

function extractPendingCalendarTargetSummaryHint(rawText: string): string | null {
  const normalized = normalizeForRuleParsing(rawText)
    .trim()
  if (!normalized) return null

  const patterns = [
    /(?:予定|件名|タイトル|予定名|内容)?(?:の)?\s*([^\n。]{1,40}?)\s*を\s*[^\n。]{1,80}\s*(?:に|へ)(?:変更|修正|更新|して|直して|変えて|してください|します|する)/i,
    /([^\n。]{1,40}?)\s*(?:の予定|の件名|のタイトル|の予定名|の内容)\s*(?:を)?\s*[^\n。]{1,80}\s*(?:に|へ)(?:変更|修正|更新|して|直して|変えて|してください|します|する)/i,
    /(?:対象|予定)\s*(?:は|を|:|：)\s*([^\n。]{1,40})/i,
  ]

  for (const pattern of patterns) {
    const match = normalized.match(pattern)
    const raw = String(match?.[1] ?? '').trim()
    if (!raw) continue
    const cleaned = normalizeForRuleParsing(raw)
      .replace(/^[\s、,。．:：\-]+/, '')
      .replace(/[」』"'\s、,。．:：\-]+$/g, '')
      .trim()
    if (!cleaned) continue
    if (/(時間|時刻|開始|日付|日にち|日時|件名|タイトル|予定名|内容|場所|会場|所要|分|duration|location)/i.test(cleaned)) {
      continue
    }
    if (cleaned.length <= 1) continue
    return cleaned
  }
  return null
}

function pendingTargetMentionsDateOrTime(entry: PendingCalendarUpdateTargetEntry, rawText: string): boolean {
  const normalized = normalizeForRuleParsing(rawText)
  const compact = normalizeKeywordForSearch(rawText)
  if (entry.date) {
    const slashDate = entry.date.replace(/-/g, '/')
    const monthDay = slashDate.split('/').slice(1).join('/')
    if (normalized.includes(entry.date) || normalized.includes(slashDate) || (monthDay && normalized.includes(monthDay))) {
      return true
    }
  }
  if (entry.time) {
    const [hh, mm] = entry.time.split(':')
    const hour = Number(hh)
    const minute = Number(mm)
    const jaTime = Number.isInteger(hour) && Number.isInteger(minute)
      ? (minute === 0 ? `${hour}時` : `${hour}時${minute}分`)
      : ''
    if (
      compact.includes(entry.time)
      || compact.includes(`${hour}:${String(minute).padStart(2, '0')}`)
      || compact.includes(`${hour}時`)
      || (jaTime && compact.includes(jaTime))
    ) {
      return true
    }
  }
  return false
}

function extractCalendarDurationForUpdate(rawText: string): number | null {
  const normalized = normalizeForRuleParsing(rawText)
  const patterns = [
    /(?:所要|時間幅|時間|duration)\s*(?:を|は|:|：)?\s*(\d{1,3})\s*分/i,
    /(\d{1,3})\s*分\s*(?:に(?:変更|して|します|する)|でお願いします|です)/i,
  ]
  for (const pattern of patterns) {
    const match = normalized.match(pattern)
    if (!match || !match[1]) continue
    const value = Number(match[1])
    if (!Number.isInteger(value) || value <= 0 || value > MAX_DURATION_MIN) continue
    return value
  }
  return null
}

function buildCalendarUpdateCommandFromConversation(
  rawText: string,
  eventId: string,
  baseDate: Date = new Date(),
): { command: CalendarUpdateCommand | null; guidance?: string } {
  const text = String(rawText ?? '').trim()
  if (!text) return { command: null }

  const slot = parseDateTimeSlotFromLine(text)
  const dateFromSlot = slot?.date
  const timeFromSlot = slot?.time
  const durationFromSlot = slot?.durationMin
  const dateOnly = dateFromSlot ? null : extractCorrectionDate(text, baseDate)
  const timeOnly = timeFromSlot ? null : extractCorrectionTime(text)
  const explicitTitle = extractCorrectionTitle(text)
  const explicitDescription = extractCorrectionDescription(text)
  const location = extractCorrectionLocation(text)
  const duration = durationFromSlot ?? extractCalendarDurationForUpdate(text)
  const compact = normalizeForRuleParsing(text).replace(/\s+/g, '')
  const clearLocation = /(場所|会場).*(なし|未設定|クリア|削除|消去)/.test(compact)
  const clearDescription = /(内容|詳細|説明).*(なし|未設定|クリア|削除|消去)/.test(compact)

  const nextDate = dateFromSlot ?? dateOnly ?? undefined
  const nextTime = timeFromSlot ?? timeOnly ?? undefined
  const nextTitle = explicitTitle ? cleanCalendarTitle(explicitTitle) : undefined
  const nextDescription = explicitDescription ? cleanCalendarDescription(explicitDescription) ?? undefined : undefined
  const nextLocation = location ?? undefined
  const nextDuration = (typeof duration === 'number' && Number.isInteger(duration) && duration > 0 && duration <= MAX_DURATION_MIN)
    ? duration
    : undefined

  if (
    !nextDate &&
    !nextTime &&
    !nextTitle &&
    !nextLocation &&
    !nextDescription &&
    !clearLocation &&
    !clearDescription &&
    typeof nextDuration === 'undefined'
  ) {
    return {
      command: null,
      guidance: [
        '変更内容を読み取れませんでした。',
        '例: 「時間を19:00に変更」「2件目の日付を2026-05-20に変更」「件名を試飲会に変更」',
        '例: 「開始を19時半にして」「会場はmarugoで」「タイトルをシェフミーティングに修正」',
        '例: 「内容に、店長のみの会議ですと記載して」',
        '例: 「予定を店長会議に変更」「店長会議にして」',
      ].join('\n'),
    }
  }

  return {
    command: {
      kind: 'update',
      eventId,
      ...(nextTitle ? { title: nextTitle } : {}),
      ...(nextDate ? { date: nextDate } : {}),
      ...(nextTime ? { time: nextTime } : {}),
      ...(typeof nextDuration === 'number' ? { durationMin: nextDuration } : {}),
      ...(nextLocation ? { location: nextLocation } : {}),
      ...(clearLocation ? { clearLocation: true } : {}),
      ...(nextDescription ? { description: nextDescription } : {}),
      ...(clearDescription ? { clearDescription: true } : {}),
    },
  }
}

function selectCalendarUpdateTargetFromPending(
  text: string,
  pending: PendingCalendarUpdateContext,
): { entry: PendingCalendarUpdateTargetEntry | null; requiresIndex: boolean; hint?: string | null } {
  const index = extractPendingCalendarTargetIndex(text)
  if (index != null) {
    if (index < 0 || index >= pending.target_events.length) {
      return { entry: null, requiresIndex: true, hint: null }
    }
    return { entry: pending.target_events[index], requiresIndex: false, hint: null }
  }

  const hint = extractPendingCalendarTargetSummaryHint(text)
  if (hint) {
    const normalizedHint = normalizeKeywordForSearch(hint)
    const compactHint = compactSearchText(hint)
    const matched = pending.target_events.filter((entry) => {
      const normalizedSummary = normalizeKeywordForSearch(entry.summary)
      const compactSummary = compactSearchText(entry.summary)
      if (normalizedHint && normalizedSummary && normalizedSummary.includes(normalizedHint)) return true
      if (compactHint && compactSummary && compactSummary.includes(compactHint)) return true
      if (normalizedHint && normalizedSummary && normalizedHint.includes(normalizedSummary)) return true
      if (compactHint && compactSummary && compactHint.includes(compactSummary)) return true
      return false
    })
    if (matched.length === 1) {
      return { entry: matched[0], requiresIndex: false, hint }
    }
  }

  const textNormalized = normalizeKeywordForSearch(text)
  const textCompact = compactSearchText(text)
  const scored = pending.target_events
    .map((entry) => {
      let score = 0
      const summaryNormalized = normalizeKeywordForSearch(entry.summary)
      const summaryCompact = compactSearchText(entry.summary)
      if (summaryNormalized && textNormalized.includes(summaryNormalized)) score += 5
      if (summaryCompact && textCompact.includes(summaryCompact)) score += 4
      if (pendingTargetMentionsDateOrTime(entry, text)) score += 3
      return { entry, score }
    })
    .filter((item) => item.score > 0)
    .sort((a, b) => b.score - a.score)

  if (scored.length > 0) {
    const top = scored[0]
    const second = scored[1]
    if (!second || top.score > second.score) {
      return { entry: top.entry, requiresIndex: false, hint: null }
    }
  }

  if (pending.target_events.length === 1) {
    return { entry: pending.target_events[0], requiresIndex: false, hint: null }
  }
  return { entry: null, requiresIndex: true, hint }
}

async function tryHandlePendingCalendarUpdateConversation(
  text: string,
  supabase: ReturnType<typeof createClient>,
  env: CalendarEnv,
  roomId: string,
  userId: string | null,
  groqApiKey: string,
  quotedMessageId: string | null,
): Promise<string | null> {
  if (!looksLikeCalendarUpdateConversationText(text)) return null
  const compact = normalizeForRuleParsing(text).replace(/\s+/g, '')
  const hasExplicitUpdateCue = /(変更|修正|更新|戻して|に戻して|へ戻して|直して|直す|ずらして|ずらす|リスケ)/.test(compact)
  // Guard: when user sends a fresh event announcement (date/time + event), prefer new registration flow.
  if (!hasExplicitUpdateCue && looksLikeSingleEventAnnouncement(text)) return null

  const pending = await fetchPendingCalendarUpdateContext(supabase, roomId, userId)
  if (!pending) return null

  const expireAtMs = new Date(pending.expires_at).getTime()
  if (!Number.isFinite(expireAtMs) || Date.now() >= expireAtMs) {
    await resolvePendingCalendarUpdateContext(supabase, pending, 'expired')
    return '変更対象の候補が期限切れです。もう一度「予定確認」を実行してから変更してください。'
  }

  const hasQuotedContext = !!quotedMessageId && pending.source_line_message_ids.length > 0
  const quotedMessageMatched = !hasQuotedContext || pending.source_line_message_ids.includes(String(quotedMessageId))

  if (/^(いいえ|no|n|キャンセル|中止|やめる)$/.test(compact)) {
    await resolvePendingCalendarUpdateContext(supabase, pending, 'cancelled')
    return '予定変更の操作をキャンセルしました。'
  }

  const selected = selectCalendarUpdateTargetFromPending(text, pending)
  if (!selected.entry) {
    if (selected.requiresIndex) {
      return [
        ...(hasQuotedContext && !quotedMessageMatched
          ? ['返信先の予定候補とは一致しなかったため、本文からも特定を試みましたが対象を絞れませんでした。']
          : []),
        selected.hint
          ? `「${selected.hint}」に一致する予定を特定できませんでした。`
          : '変更する予定を特定できませんでした。',
        '変更したい予定のメッセージに返信して、次のように送ってください。',
        '例: 「1件目の時間を19:00に変更」',
        '例: 「会議を店長会議に変更」「5/15の予定の場所をmarugoに変更」',
      ].join('\n')
    }
    return null
  }

  const selectedBaseDate = parseBaseDateForCalendarUpdate(selected.entry.date)
  const updateCommand = buildCalendarUpdateCommandFromConversation(
    text,
    selected.entry.event_id,
    selectedBaseDate,
  )
  let resolvedCommand = updateCommand.command
  const shouldTryAiFallback =
    !!groqApiKey &&
    (
      !resolvedCommand ||
      isLikelyLowQualityRuleBasedUpdateCommand(resolvedCommand, text) ||
      looksLikeSemanticFieldUpdateText(text)
    )
  if (shouldTryAiFallback) {
    const aiIntent = await extractCalendarUpdateIntentWithGroq(
      text,
      env.timezone,
      groqApiKey,
      selected.entry.summary,
    )
    if (aiIntent) {
      const aiCommand = buildCalendarUpdateCommandFromAiIntent(selected.entry.event_id, aiIntent)
      if (aiCommand) {
        resolvedCommand = resolvedCommand
          ? mergeCalendarUpdateCommands(resolvedCommand, aiCommand)
          : aiCommand
      }
    }
  }

  if (!resolvedCommand) {
    return updateCommand.guidance ?? null
  }
  return await updateCalendarEventReply(resolvedCommand, env)
}

function parseBaseDateForCalendarUpdate(date: string | null | undefined): Date {
  const normalized = String(date ?? '').trim()
  if (isValidDate(normalized)) {
    const [year, month, day] = normalized.split('-').map(Number)
    return new Date(Date.UTC(year, month - 1, day, 0, 0, 0, 0))
  }
  return new Date()
}

async function tryHandlePendingLibrarySearchConfirmation(
  text: string,
  supabase: ReturnType<typeof createClient>,
  roomId: string,
  userId: string | null,
): Promise<string[] | null> {
  const pending = await fetchPendingLibrarySearchConfirmation(supabase, roomId, userId)
  if (!pending) return null

  const expireAtMs = new Date(pending.expires_at).getTime()
  if (!Number.isFinite(expireAtMs) || Date.now() >= expireAtMs) {
    await resolvePendingLibrarySearchConfirmation(supabase, pending, 'expired')
    return ['資料ライブラリ検索の確認が期限切れです。もう一度会話検索からやり直してください。']
  }

  const decision = normalizeConfirmationDecision(text)
  if (decision === 'no') {
    await resolvePendingLibrarySearchConfirmation(supabase, pending, 'cancelled')
    return ['資料ライブラリ検索をキャンセルしました。']
  }
  if (decision !== 'yes') {
    return null
  }

  const command: MessageSearchCommand = {
    kind: 'search_messages',
    keyword: pending.keyword,
    days: pending.search_days,
    scope: pending.search_scope,
  }
  const reply = await buildLineTaggedDocumentLibrarySearchReply(
    command,
    supabase,
    roomId,
  )
  await resolvePendingLibrarySearchConfirmation(supabase, pending, 'confirmed')
  return reply
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

  m = token.match(/^(\d{1,2})時半$/)
  if (m) {
    const hour = Number(m[1])
    if (!Number.isInteger(hour) || hour < 0 || hour > 23) return null
    return { hour, minute: 30 }
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
  const inferredTitle = inferTitleFromLine(sourceText)
  if (normalizedAiTitle && (!hasSourceText || isTitleGroundedInSource(normalizedAiTitle, sourceText))) {
    if (
      inferredTitle &&
      isGenericEventTitle(normalizedAiTitle) &&
      isMoreSpecificDerivedTitle(normalizedAiTitle, inferredTitle)
    ) {
      title = inferredTitle
      titleSource = 'source_derived'
    } else {
      title = normalizedAiTitle
      titleSource = 'ai'
    }
  } else if (inferredTitle) {
    title = inferredTitle
    titleSource = 'source_derived'
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

function isGenericEventTitle(title: string): boolean {
  const normalizedTitle = normalizeKeywordForSearch(title)
  if (!normalizedTitle) return false
  return CALENDAR_EVENT_TITLE_KEYWORDS.some((keyword) => normalizeKeywordForSearch(keyword) === normalizedTitle)
}

function isMoreSpecificDerivedTitle(baseTitle: string, derivedTitle: string): boolean {
  const normalizedBase = normalizeKeywordForSearch(baseTitle)
  const normalizedDerived = normalizeKeywordForSearch(derivedTitle)
  if (!normalizedBase || !normalizedDerived) return false
  if (normalizedBase === normalizedDerived) return false
  if (!normalizedDerived.includes(normalizedBase)) return false
  return compactSearchText(derivedTitle).length > compactSearchText(baseTitle).length
}

function extractEventKeywordTitle(raw: string): string | null {
  const text = normalizeForRuleParsing(raw)
  const firstSentence = text.split(/[、,。．.!！?？\n]/)[0] ?? text
  const normalizedSentence = firstSentence.trim()
  if (!normalizedSentence) return null

  for (const keyword of CALENDAR_EVENT_TITLE_KEYWORDS) {
    const escaped = escapeRegExp(keyword)
    const compound = normalizedSentence.match(
      new RegExp(`([A-Za-z0-9ァ-ヶー一-龥々・]{1,24}${escaped})`, 'i'),
    )
    if (compound && compound[1]) {
      return cleanCalendarTitle(compound[1])
    }
    const exact = normalizedSentence.match(new RegExp(escaped, 'i'))
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

function cleanCalendarDescription(raw: string): string | null {
  const cleaned = normalizeSpaces(raw)
    .replace(/^[\s、,。．:：\-]+/, '')
    .replace(/^(?:内容|詳細|説明)\s*(?:は|を|に|へ)?\s*[、,。．:：\-]*/i, '')
    .replace(/^(?:次(?:の)?|以下(?:の)?|下記(?:の)?|つぎの)\s*[、,。．:：\-]*/i, '')
    .replace(
      /^(?:(?:次(?:の)?|以下(?:の)?|下記(?:の)?|つぎの)\s*)?(?:言葉|文言|文章|テキスト|内容|詳細|説明)\s*(?:を|に|へ)?\s*(?:変更|修正|更新|変えて|して|書き換えて|置き換えて)\s*[、,。．:：\-]*/i,
      '',
    )
    .replace(
      /^(?:(?:次(?:の)?|以下(?:の)?|下記(?:の)?|つぎの)\s*)?(?:言葉|文言|文章|テキスト)\s*(?:を|に|へ)?\s*(?:変更|修正|更新|変えて|して|書き換えて|置き換えて)\s*[、,。．:：\-]*/i,
      '',
    )
    .replace(/^(?:言葉|文言|文章|テキスト)\s*(?:を|に|へ)?\s*(?:変更|修正|更新|変えて|して)\s*[、,。．:：\-]*/i, '')
    .replace(/\s*(?:と)?\s*(?:記載|記入|追記|追加|入れて|書いて|載せて|残して|メモして)(?:ください|下さい|して|お願いします|お願い|ほしい|欲しい)?\s*$/i, '')
    .replace(/\s*(?:に|へ)\s*(?:変更|修正|更新|して|変えて|書き換えて|置き換えて)(?:ください|下さい|ほしい|欲しい)?\s*$/i, '')
    .replace(/^(?:に|へ|を|は)\s*[、,。．:：\-]*/i, '')
    .replace(/^(?:「|『|“|\"|')+/, '')
    .replace(/(?:」|』|”|\"|')+$/g, '')
    .replace(/[\s]+$/g, '')
    .trim()
  if (!cleaned) return null
  if (/^(?:次(?:の)?|以下(?:の)?|下記(?:の)?|つぎの)$/i.test(cleaned)) return null
  if (/^(?:言葉|文言|文章|テキスト|内容|詳細|説明)$/i.test(cleaned)) return null
  const compact = compactSearchText(cleaned)
  if (!compact) return null
  return cleaned.length > 1000 ? cleaned.slice(0, 1000) : cleaned
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

  if (text.startsWith('予定変更')) {
    const body = text.replace(/^予定変更\s*/, '')
    const updateCommand = parseCalendarUpdateCommand(body)
    if (updateCommand.error) {
      return {
        matched: true,
        command: null,
        error: updateCommand.error,
      }
    }
    return { matched: true, command: updateCommand.command, error: null }
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

function parseCalendarUpdateCommand(bodyRaw: string): {
  command: CalendarUpdateCommand | null
  error: string | null
} {
  const body = normalizeSpaces(bodyRaw)
  if (!body) {
    return {
      command: null,
      error: [
        '形式エラーです。',
        '例: 予定変更 <event_id> | 時刻=19:00',
        '例: 予定確認のあと「1件目の時間を19:00に変更」',
      ].join('\n'),
    }
  }

  const parts = body.split('|').map((p) => p.trim()).filter((p) => p.length > 0)
  if (parts.length < 2) {
    return {
      command: null,
      error: [
        '変更内容を指定してください。',
        '例: 予定変更 <event_id> | 件名=試飲会(更新)',
        '例: 予定変更 <event_id> | 内容=店長のみの会議です',
        '例: 予定確認のあと「1件目の件名を試飲会(更新)に変更」',
      ].join('\n'),
    }
  }

  const eventId = parts[0]
  if (!/^[a-zA-Z0-9@._-]+$/.test(eventId)) {
    return { command: null, error: '変更対象の指定形式が不正です。もう一度「予定確認」を実行してから変更してください。' }
  }

  let title: string | undefined
  let date: string | undefined
  let time: string | undefined
  let durationMin: number | undefined
  let location: string | undefined
  let clearLocation = false
  let description: string | undefined
  let clearDescription = false

  for (let i = 1; i < parts.length; i += 1) {
    const segment = parts[i]
    const eqIdx = segment.indexOf('=')
    if (eqIdx <= 0) {
      return { command: null, error: `変更指定は key=value 形式で入力してください: ${segment}` }
    }
    const rawKey = segment.slice(0, eqIdx).trim().toLowerCase()
    const value = normalizeSpaces(segment.slice(eqIdx + 1)).trim()
    if (!value) {
      return { command: null, error: `値が空です: ${segment}` }
    }

    if (rawKey === '件名' || rawKey === 'タイトル' || rawKey === 'title') {
      title = cleanCalendarTitle(value)
      if (!title) return { command: null, error: '件名が不正です。' }
      continue
    }
    if (rawKey === '日付' || rawKey === 'date') {
      if (!isValidDate(value)) return { command: null, error: '日付は YYYY-MM-DD 形式で指定してください。' }
      date = value
      continue
    }
    if (rawKey === '時刻' || rawKey === '時間' || rawKey === 'time') {
      if (!isValidTime(value)) return { command: null, error: '時刻は HH:mm 形式（24時間）で指定してください。' }
      time = value
      continue
    }
    if (rawKey === '所要' || rawKey === '所要分' || rawKey === 'duration' || rawKey === 'durationmin') {
      const parsed = Number(value)
      if (!Number.isInteger(parsed) || parsed <= 0 || parsed > MAX_DURATION_MIN) {
        return { command: null, error: `所要時間は1〜${MAX_DURATION_MIN}分で指定してください。` }
      }
      durationMin = parsed
      continue
    }
    if (rawKey === '場所' || rawKey === 'location') {
      const compact = normalizeForRuleParsing(value).replace(/\s+/g, '')
      if (/^(なし|空|未設定|クリア|削除|消去)$/.test(compact)) {
        clearLocation = true
        location = undefined
      } else {
        const cleanedLocation = cleanCalendarLocation(value)
        if (!cleanedLocation) return { command: null, error: '場所の指定が不正です。' }
        location = cleanedLocation
        clearLocation = false
      }
      continue
    }
    if (rawKey === '内容' || rawKey === '詳細' || rawKey === '説明' || rawKey === 'description') {
      const compact = normalizeForRuleParsing(value).replace(/\s+/g, '')
      if (/^(なし|空|未設定|クリア|削除|消去)$/.test(compact)) {
        clearDescription = true
        description = undefined
      } else {
        const cleanedDescription = cleanCalendarDescription(value)
        if (!cleanedDescription) return { command: null, error: '内容の指定が不正です。' }
        description = cleanedDescription
        clearDescription = false
      }
      continue
    }
    return { command: null, error: `未対応の更新キーです: ${rawKey}` }
  }

  if (
    !title &&
    !date &&
    !time &&
    typeof durationMin === 'undefined' &&
    !location &&
    !clearLocation &&
    !description &&
    !clearDescription
  ) {
    return { command: null, error: '変更項目がありません。件名・日付・時刻・所要・場所・内容のいずれかを指定してください。' }
  }

  return {
    command: {
      kind: 'update',
      eventId,
      ...(title ? { title } : {}),
      ...(date ? { date } : {}),
      ...(time ? { time } : {}),
      ...(typeof durationMin === 'number' ? { durationMin } : {}),
      ...(location ? { location } : {}),
      ...(clearLocation ? { clearLocation: true } : {}),
      ...(description ? { description } : {}),
      ...(clearDescription ? { clearDescription: true } : {}),
    },
    error: null,
  }
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
  supabase: ReturnType<typeof createClient>,
  roomId: string,
  userId: string | null,
  sourceMeta?: CalendarSourceMeta,
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
        sourceMeta,
      )
    }
    if (parseResult.command.kind === 'update') {
      return await updateCalendarEventReply(
        parseResult.command,
        calendarEnvState.env,
      )
    }
    return await listCalendarEventsReply(parseResult.command, calendarEnvState.env, supabase, roomId, userId)
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
  sourceMeta?: CalendarSourceMeta,
): Promise<string> {
  const result = await createCalendarEvent(command, env, roomId, userId, undefined, sourceMeta)
  if (!result.ok) {
    return `予定登録に失敗しました。${result.error}`
  }
  const registeredDate = formatDateOnlyForLine(result.startDate, env.timezone)
  const registeredTime = `${formatTimeOnlyForLine(result.startDate, env.timezone)}-${formatTimeOnlyForLine(result.endDate, env.timezone)}`

  return [
    '予定を登録しました。',
    ...buildCalendarDetailTemplateLines({
      title: result.summary,
      date: registeredDate,
      time: registeredTime,
      location: command.location ?? null,
      content: null,
    }),
  ].join('\n')
}

async function updateCalendarEventReply(
  command: CalendarUpdateCommand,
  env: CalendarEnv,
): Promise<string> {
  const result = await updateCalendarEvent(command, env)
  if (!result.ok) {
    return `予定変更に失敗しました。${result.error}`
  }

  const detail = formatEventDetailBlock(result.event, env.timezone)
  const location = cleanCalendarLocation(String(result.event.location ?? '')) ?? null
  const description = sanitizeEventDescriptionForList(String(result.event.description ?? ''))
  return [
    '予定を変更しました。',
    ...buildCalendarDetailTemplateLines({
      title: detail.title,
      date: detail.date,
      time: detail.time,
      location,
      content: description || null,
    }),
  ].join('\n')
}

async function updateCalendarEvent(
  command: CalendarUpdateCommand,
  env: CalendarEnv,
): Promise<{ ok: true; event: GoogleCalendarEvent } | { ok: false; error: string }> {
  const accessToken = await fetchGoogleAccessToken(env)
  const eventUrl = `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(env.calendarId)}/events/${encodeURIComponent(command.eventId)}`
  const eventResponse = await fetch(eventUrl, {
    method: 'GET',
    headers: {
      'Authorization': `Bearer ${accessToken}`,
      'Content-Type': 'application/json',
    },
  })
  if (!eventResponse.ok) {
    const text = await eventResponse.text()
    if (eventResponse.status === 404) {
      return { ok: false, error: '指定した予定が見つかりません。もう一度「予定確認」を実行してから変更してください。' }
    }
    return { ok: false, error: `Google Calendar API error (${eventResponse.status}): ${text}` }
  }

  const existing = await eventResponse.json() as GoogleCalendarEvent
  const payload: Record<string, unknown> = {}
  if (command.title) payload.summary = command.title
  if (command.clearLocation) {
    payload.location = ''
  } else if (command.location) {
    payload.location = command.location
  }
  const existingDescriptionRaw = String(existing.description ?? '')
  if (command.clearDescription) {
    payload.description = ''
  } else if (command.description) {
    const userContent = stripCalendarSourceMetadataLines(command.description)
    payload.description = userContent
  }

  const needsScheduleUpdate =
    !!command.date
    || !!command.time
    || typeof command.durationMin === 'number'
  if (needsScheduleUpdate) {
    const existingStart = existing.start?.dateTime
    const existingEnd = existing.end?.dateTime
    if (!existingStart || !existingEnd) {
      return { ok: false, error: '終日予定は LINE からの時間変更に未対応です。Google カレンダーで編集してください。' }
    }
    const existingStartDate = new Date(existingStart)
    const existingEndDate = new Date(existingEnd)
    if (Number.isNaN(existingStartDate.getTime()) || Number.isNaN(existingEndDate.getTime())) {
      return { ok: false, error: '既存予定の日時が不正なため変更できません。' }
    }

    const fallbackDate = formatDateOnlyForLine(existingStartDate, env.timezone).replace(/\//g, '-')
    const fallbackTime = formatTimeOnlyForLine(existingStartDate, env.timezone)
    const fallbackDuration = Math.max(
      1,
      Math.min(
        MAX_DURATION_MIN,
        Math.round((existingEndDate.getTime() - existingStartDate.getTime()) / (60 * 1000)),
      ),
    )

    const nextDate = command.date ?? fallbackDate
    const nextTime = command.time ?? fallbackTime
    const nextDuration = typeof command.durationMin === 'number' ? command.durationMin : fallbackDuration
    if (!isValidDate(nextDate)) return { ok: false, error: '変更後の日付が不正です。' }
    if (!isValidTime(nextTime)) return { ok: false, error: '変更後の時刻が不正です。' }
    if (!Number.isInteger(nextDuration) || nextDuration <= 0 || nextDuration > MAX_DURATION_MIN) {
      return { ok: false, error: `変更後の所要時間は1〜${MAX_DURATION_MIN}分で指定してください。` }
    }

    const nextEnd = addMinutesToLocalDateTime(nextDate, nextTime, nextDuration)
    if (!nextEnd) return { ok: false, error: '変更後の終了時刻を計算できませんでした。' }
    payload.start = {
      dateTime: `${nextDate}T${nextTime}:00+09:00`,
      timeZone: CALENDAR_CREATE_TIMEZONE,
    }
    payload.end = {
      dateTime: `${nextEnd.date}T${nextEnd.time}:00+09:00`,
      timeZone: CALENDAR_CREATE_TIMEZONE,
    }
  }

  if (Object.keys(payload).length === 0) {
    return { ok: false, error: '変更項目がありません。' }
  }

  const updateResponse = await fetch(eventUrl, {
    method: 'PATCH',
    headers: {
      'Authorization': `Bearer ${accessToken}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  })
  if (!updateResponse.ok) {
    const text = await updateResponse.text()
    return { ok: false, error: `Google Calendar API error (${updateResponse.status}): ${text}` }
  }

  const updated = await updateResponse.json() as GoogleCalendarEvent
  return { ok: true, event: updated }
}

async function createCalendarEvent(
  command: CalendarCreateCommand,
  env: CalendarEnv,
  roomId: string,
  userId: string | null,
  providedAccessToken?: string,
  sourceMeta?: CalendarSourceMeta,
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
      description: buildCalendarSourceMetadataLines(roomId, userId, sourceMeta).join('\n'),
      extendedProperties: {
        private: buildCalendarSourceMetadataMap(roomId, userId, sourceMeta),
      },
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
  supabase: ReturnType<typeof createClient>,
  roomId: string,
  userId: string | null,
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

  await savePendingCalendarUpdateContext(supabase, roomId, userId, items, env.timezone)

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
  lines.push('')
  if (items.length === 1) {
    lines.push('この予定を変更する場合は、このメッセージに返信して「時間を19:00に変更」のように送ってください。')
  } else {
    lines.push('表示した予定を変更する場合は、このメッセージに返信して送ってください。')
    lines.push('例: 「2件目の時間を19:00に変更」「会議を店長会議に変更」')
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

function formatDateForCalendarTemplate(rawDate: string): string {
  const date = normalizeForRuleParsing(String(rawDate ?? '')).trim()
  if (!date) return '(日付不明)'
  if (isValidDate(date)) return date.replace(/-/g, '/')
  return date
}

function formatTimeRangeForCalendarTemplate(date: string, time: string, durationMin: number): string {
  const normalizedTime = normalizeForRuleParsing(String(time ?? '')).trim()
  if (!normalizedTime) return '(時間不明)'
  if (!isValidDate(date) || !isValidTime(normalizedTime)) return normalizedTime
  if (!Number.isInteger(durationMin) || durationMin <= 0 || durationMin > MAX_DURATION_MIN) return normalizedTime
  const end = addMinutesToLocalDateTime(date, normalizedTime, durationMin)
  if (!end) return normalizedTime
  if (end.date === date) return `${normalizedTime}-${end.time}`
  return `${normalizedTime}-${end.time}(翌日)`
}

function buildCalendarDetailTemplateLines(detail: {
  title: string
  date: string
  time: string
  location?: string | null
  content?: string | null
}): string[] {
  const title = cleanCalendarTitle(String(detail.title ?? ''))
  const date = normalizeForRuleParsing(String(detail.date ?? '')).trim() || '(日付不明)'
  const time = normalizeForRuleParsing(String(detail.time ?? '')).trim() || '(時間不明)'
  const locationRaw = String(detail.location ?? '')
  const location = cleanCalendarLocation(locationRaw) ?? normalizeInlineText(locationRaw)
  const content = normalizeInlineText(String(detail.content ?? ''))
  return [
    `件名: ${title || '予定'}`,
    `日付: ${date}`,
    `時間: ${time}`,
    `場所: ${location || '（未設定）'}`,
    `内容: ${content || '（内容なし）'}`,
  ]
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
    .filter((line) => !isCalendarSourceMetadataLine(line))

  const merged = normalizeInlineText(lines.join(' / '))
  const cleaned = stripCalendarSourceMetadataFragments(merged)
  if (!cleaned) return ''
  if (cleaned.length > 140) return `${cleaned.slice(0, 140)}...`
  return cleaned
}

function normalizeInlineText(raw: string): string {
  return normalizeSpaces(raw).replace(/\s+/g, ' ')
}

function isCalendarSourceMetadataLine(rawLine: string): boolean {
  const line = normalizeInlineText(String(rawLine ?? ''))
  if (!line) return false
  if (/^source\s*[:：=]\s*line-webhook\b/i.test(line)) return true
  if (
    /^LINE\s+(?:room_id|room_name|room_label|group_id|group_name|user_id|user_name|sender_name|poster_name|投稿者|送信者)\s*[:：=]/i
      .test(line)
  ) {
    return true
  }
  if (
    /^(?:room_id|user_id|group_id|room_name|group_name|sender_name|poster_name)\s*[:：=]/i
      .test(line)
  ) {
    return true
  }
  return false
}

function stripCalendarSourceMetadataFragments(text: string): string {
  const raw = normalizeInlineText(String(text ?? ''))
  if (!raw) return ''
  const cleaned = raw
    .replace(/(?:^|[\/\s])source\s*[:：=]\s*line-webhook(?:$|[\/\s])/ig, ' ')
    .replace(/(?:^|[\/\s])LINE\s+(?:room_id|room_name|room_label|group_id|group_name|user_id|user_name|sender_name|poster_name|投稿者|送信者)\s*[:：=]\s*[^\s/]+/ig, ' ')
    .replace(/(?:^|[\/\s])(?:room_id|user_id|group_id)\s*[:：=]\s*[^\s/]+/ig, ' ')
    .replace(/\s*\/\s*\/+\s*/g, ' / ')
    .replace(/\s{2,}/g, ' ')
    .replace(/^\s*\/\s*|\s*\/\s*$/g, '')
    .trim()
  return cleaned
}

function extractCalendarSourceMetadataLines(rawDescription: string): string[] {
  const unique = new Set<string>()
  const out: string[] = []
  const lines = String(rawDescription ?? '')
    .split(/\r?\n/)
    .map((line) => normalizeInlineText(line))
    .filter((line) => line.length > 0)
  for (const line of lines) {
    if (!isCalendarSourceMetadataLine(line)) continue
    if (unique.has(line)) continue
    unique.add(line)
    out.push(line)
  }
  return out
}

function stripCalendarSourceMetadataLines(rawDescription: string): string {
  const lines = String(rawDescription ?? '')
    .split(/\r?\n/)
    .map((line) => normalizeInlineText(line))
    .filter((line) => line.length > 0)
    .filter((line) => !isCalendarSourceMetadataLine(line))
  return lines.join('\n').trim()
}

function buildCalendarSourceMetadataLines(
  roomId: string,
  userId: string | null,
  sourceMeta?: CalendarSourceMeta,
): string[] {
  const roomName = normalizeDisplayName(sourceMeta?.roomName) ?? '（未取得）'
  const userName = normalizeDisplayName(sourceMeta?.userName) ?? '（未取得）'
  return [
    `LINE room_name: ${roomName}`,
    `LINE user_name: ${userName}`,
    'source: line-webhook',
  ]
}

function buildCalendarSourceMetadataMap(
  roomId: string,
  userId: string | null,
  sourceMeta?: CalendarSourceMeta,
): Record<string, string> {
  return {
    line_room_id: String(roomId ?? ''),
    line_user_id: String(userId ?? 'unknown'),
    line_room_name: normalizeDisplayName(sourceMeta?.roomName) ?? '',
    line_user_name: normalizeDisplayName(sourceMeta?.userName) ?? '',
    source: 'line-webhook',
  }
}

function composeCalendarDescriptionWithMetadata(
  content: string | null | undefined,
  metadataLines: string[],
): string {
  const body = normalizeInlineText(String(content ?? '')).trim()
  const metadata = extractCalendarSourceMetadataLines(metadataLines.join('\n'))
  if (!body) return metadata.join('\n')
  if (metadata.length === 0) return body
  return `${body}\n\n${metadata.join('\n')}`
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

function splitTextForLineReply(text: string, maxLength = 4900): string[] {
  const normalized = String(text ?? '').trim()
  if (!normalized) return ['（空メッセージ）']

  const chunks: string[] = []
  let current = ''
  const sourceLines = normalized.split('\n')

  const flush = () => {
    const trimmed = current.trim()
    if (trimmed) chunks.push(trimmed)
    current = ''
  }

  const appendLine = (line: string) => {
    if (!current) {
      current = line
      return
    }
    const candidate = `${current}\n${line}`
    if (candidate.length <= maxLength) {
      current = candidate
      return
    }
    flush()
    current = line
  }

  for (const rawLine of sourceLines) {
    let line = String(rawLine ?? '')
    if (line.length === 0) {
      appendLine('')
      continue
    }
    while (line.length > maxLength) {
      const segment = line.slice(0, maxLength)
      appendLine(segment)
      flush()
      line = line.slice(maxLength)
    }
    appendLine(line)
  }

  flush()
  return chunks.length > 0 ? chunks : ['（空メッセージ）']
}

async function replyLineMessage(
  replyToken: string,
  text: string | string[],
  channelAccessToken: string,
): Promise<{ ok: true; sentMessageIds: string[] } | { ok: false; error: string }> {
  const inputTexts = Array.isArray(text) ? text : [text]
  const preparedTexts = inputTexts
    .flatMap((item) => splitTextForLineReply(item, 4900))
    .filter((item) => item.length > 0)
  if (preparedTexts.length === 0) {
    preparedTexts.push('（空メッセージ）')
  }

  const maxReplyMessages = 5
  const replyTexts = preparedTexts.slice(0, maxReplyMessages)
  if (preparedTexts.length > maxReplyMessages) {
    const omitted = preparedTexts.length - maxReplyMessages
    const notice = `\n\n※表示上限のため残り${omitted}メッセージ分を省略しました。検索条件を絞ってください。`
    const last = replyTexts[maxReplyMessages - 1] ?? ''
    replyTexts[maxReplyMessages - 1] = `${last.slice(0, Math.max(0, 4900 - notice.length))}${notice}`
  }

  const response = await fetch('https://api.line.me/v2/bot/message/reply', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${channelAccessToken}`,
    },
    body: JSON.stringify({
      replyToken,
      messages: replyTexts.map((value) => ({ type: 'text', text: value.slice(0, 4900) })),
    }),
  })

  if (!response.ok) {
    const errText = await response.text()
    return { ok: false, error: `LINE reply API error (${response.status}): ${errText}` }
  }
  let sentMessageIds: string[] = []
  try {
    const payload = await response.json() as Record<string, unknown>
    const sentMessages = Array.isArray(payload?.sentMessages) ? payload.sentMessages : []
    sentMessageIds = normalizeLineMessageIds(sentMessages.map((item) => String((item as any)?.id ?? '')))
  } catch {
    sentMessageIds = []
  }
  return { ok: true, sentMessageIds }
}
