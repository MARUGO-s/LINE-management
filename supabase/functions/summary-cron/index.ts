import "jsr:@supabase/functions-js/edge-runtime.d.ts"
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2.44.0'
import { Groq } from "https://esm.sh/groq-sdk@0.5.0"

type MessageCleanupTiming = 'after_each_delivery' | 'end_of_day'
type LastDeliverySummaryMode = 'independent' | 'daily_rollup'
type MessageRetentionDays = 0 | 60 | 120 | 180 | 365 | 730 | 1095
type MessageUpdateResult = { affectedCount: number; error: Error | null }
type LineMessageRow = { id: string; room_id: string; content: string; created_at: string }
type RoomRuntimeSetting = {
  room_name: string | null
  delivery_hours: number[] | null
  is_enabled: boolean
  send_room_summary: boolean
  calendar_tomorrow_reminder_enabled: boolean
  gmail_reservation_alert_enabled: boolean
  message_cleanup_timing: MessageCleanupTiming | null
  last_delivery_summary_mode: LastDeliverySummaryMode | null
}
type RoomExecutionContext = {
  messageIds: string[]
}

type TomorrowReminderSettings = {
  enabled: boolean
  hours: number[]
  onlyIfEvents: boolean
  maxItems: number
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

type GoogleCalendarEvent = {
  summary?: string
  description?: string
  location?: string
  start?: { date?: string; dateTime?: string }
  end?: { date?: string; dateTime?: string }
}

type GmailAlertEnv = {
  enabled: boolean
  clientId: string
  clientSecret: string
  refreshToken: string
  query: string
  maxMessages: number
  fallbackTargetRoomId: string
}

type GmailAlertEnvState =
  | { ok: true; env: GmailAlertEnv }
  | { ok: false; missing: string[] }

type GmailMessageListItem = {
  id: string
  threadId: string | null
}

type GmailMessageAlert = {
  id: string
  threadId: string | null
  subject: string
  from: string
  snippet: string
  internalDateIso: string | null
}

const JST_OFFSET_MS = 9 * 60 * 60 * 1000
const CALENDAR_SCOPE = 'https://www.googleapis.com/auth/calendar'
const DEFAULT_TOMORROW_REMINDER_HOURS = [19]
const DEFAULT_TOMORROW_REMINDER_MAX_ITEMS = 20
const DEFAULT_MESSAGE_RETENTION_DAYS: MessageRetentionDays = 365
const DEFAULT_GMAIL_ALERT_QUERY = 'is:inbox is:unread newer_than:7d (予約 OR reservation OR booking)'
const DEFAULT_GMAIL_ALERT_MAX_MESSAGES = 5
const MAX_GMAIL_ALERT_MAX_MESSAGES = 20

Deno.serve(async (req) => {
  let supabase: ReturnType<typeof createClient> | null = null
  let jstHour = -1

  try {
    const supabaseUrl = Deno.env.get('SUPABASE_URL') ?? ''
    const supabaseKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') ?? ''
    supabase = createClient(supabaseUrl, supabaseKey)

    const lineAccessToken = Deno.env.get('LINE_CHANNEL_ACCESS_TOKEN') ?? ''
    const overallRoomId = Deno.env.get('LINE_OVERALL_ROOM_ID') ?? ''
    const groqApiKey = Deno.env.get('GROQ_API_KEY') ?? ''
    const groq = groqApiKey ? new Groq({ apiKey: groqApiKey }) : null

    // 0. Check current time and schedules
    // JST is UTC+9
    const now = new Date()
    jstHour = (now.getUTCHours() + 9) % 24
    const forceRun = isForceRun(req)
    console.log(`Current JST hour: ${jstHour}`)

    // Fetch global settings
    const { data: globalSettings, error: globalSettingsError } = await supabase
      .from('summary_settings')
      .select('delivery_hours, is_enabled, message_cleanup_timing, last_delivery_summary_mode, message_retention_days, calendar_tomorrow_reminder_enabled, calendar_tomorrow_reminder_hours, calendar_tomorrow_reminder_only_if_events, calendar_tomorrow_reminder_max_items')
      .eq('id', 1)
      .single();

    if (globalSettingsError) {
      console.error(`Error fetching global settings: ${globalSettingsError.message}`)
    }

    const globalEnabled = globalSettings?.is_enabled ?? true
    const globalHours = globalSettings?.delivery_hours ?? [12, 17, 23]
    const messageCleanupTiming = normalizeMessageCleanupTiming(globalSettings?.message_cleanup_timing)
    const lastDeliverySummaryMode = normalizeLastDeliverySummaryMode(globalSettings?.last_delivery_summary_mode)
    const messageRetentionDays = normalizeMessageRetentionDays(globalSettings?.message_retention_days)
    const tomorrowReminderSettings = normalizeTomorrowReminderSettings(globalSettings)
    const shouldSendOverall = globalEnabled && (forceRun || globalHours.includes(jstHour))
    const lastGlobalHour = getLastScheduledHour(globalHours)
    const isLastGlobalDeliverySlot = lastGlobalHour != null && jstHour === lastGlobalHour
    const jstDayRange = getJstDayRange(now)
    const shouldUseOverallDailyRollup = shouldSendOverall
      && lastDeliverySummaryMode === 'daily_rollup'
      && messageCleanupTiming === 'end_of_day'
      && isLastGlobalDeliverySlot

    if (lastDeliverySummaryMode === 'daily_rollup' && messageCleanupTiming !== 'end_of_day') {
      console.warn(
        'Invalid summary_settings combination detected. daily_rollup requires message_cleanup_timing=end_of_day. Falling back to independent mode.',
      )
    }

    try {
      if (messageRetentionDays > 0) {
        const pruneResult = await pruneMessagesByRetentionDays(supabase, messageRetentionDays, now)
        if (pruneResult.error) {
          console.error(`Failed to prune by retention (${messageRetentionDays} days):`, pruneResult.error.message)
        } else if (pruneResult.affectedCount > 0) {
          console.log(`Pruned ${pruneResult.affectedCount} LINE messages older than ${messageRetentionDays} days.`)
        }
      } else {
        console.log('Retention prune is disabled (message_retention_days=0).')
      }
    } catch (pruneErr) {
      console.error('Unexpected retention-prune error:', pruneErr)
    }

    try {
      await maybeSendTomorrowCalendarReminder({
        supabase,
        now,
        jstHour,
        lineAccessToken,
        overallRoomId,
        settings: tomorrowReminderSettings,
      })
    } catch (reminderErr) {
      console.error('Failed to process tomorrow calendar reminder:', reminderErr)
    }

    // Gmail reservation alerts are handled by the dedicated gmail-alert-cron function
    // (scheduled every minute) to provide near real-time delivery.

    // Fetch per-room settings
    const { data: roomSettingsList, error: roomSettingsError } = await supabase
      .from('room_summary_settings')
      .select('room_id, room_name, delivery_hours, is_enabled, send_room_summary, calendar_tomorrow_reminder_enabled, gmail_reservation_alert_enabled, message_cleanup_timing, last_delivery_summary_mode')

    if (roomSettingsError) {
      console.error(`Error fetching room settings: ${roomSettingsError.message}`)
    }

    const roomSettingsMap = new Map<string, RoomRuntimeSetting>()
    if (roomSettingsList) {
      for (const rs of roomSettingsList) {
        roomSettingsMap.set(rs.room_id, {
          room_name: normalizeOptionalRoomName(rs.room_name),
          delivery_hours: rs.delivery_hours,
          is_enabled: rs.is_enabled,
          send_room_summary: rs.send_room_summary === true,
          calendar_tomorrow_reminder_enabled: rs.calendar_tomorrow_reminder_enabled === true,
          gmail_reservation_alert_enabled: rs.gmail_reservation_alert_enabled === true,
          message_cleanup_timing: normalizeNullableMessageCleanupTiming(rs.message_cleanup_timing),
          last_delivery_summary_mode: normalizeNullableLastDeliverySummaryMode(rs.last_delivery_summary_mode),
        })
      }
    }

    // 1. Fetch target messages
    const [{ data: unprocessedMessages, error: unprocessedError }, { data: todayMessages, error: todayError }] = await Promise.all([
      supabase
        .from('line_messages')
        .select('id, room_id, content, created_at')
        .eq('processed', false)
        .order('created_at', { ascending: true }),
      supabase
        .from('line_messages')
        .select('id, room_id, content, created_at')
        .gte('created_at', jstDayRange.startIso)
        .lt('created_at', jstDayRange.endIso)
        .order('created_at', { ascending: true }),
    ])

    if (unprocessedError) {
      throw new Error(`Error fetching unprocessed messages: ${unprocessedError.message}`)
    }
    if (todayError) {
      throw new Error(`Error fetching day messages: ${todayError.message}`)
    }

    const queueCount = unprocessedMessages?.length ?? 0
    const dayMessageCount = todayMessages?.length ?? 0

    if (queueCount === 0 && dayMessageCount === 0) {
      await writeDeliveryLog(supabase, {
        jst_hour: jstHour,
        status: 'no_messages',
        reason: shouldUseOverallDailyRollup ? 'No messages found in current JST day window.' : 'No unprocessed messages.',
        should_send_overall: shouldSendOverall,
        rooms_targeted: 0,
        messages_in_queue: 0,
        messages_marked_processed: 0,
        line_send_attempted: false,
        line_send_success: false,
        target_room_id: overallRoomId || null,
        details: {
          force_run: forceRun,
          message_cleanup_timing: messageCleanupTiming,
          message_retention_days: messageRetentionDays,
          last_delivery_summary_mode: lastDeliverySummaryMode,
          using_daily_rollup_scope: shouldUseOverallDailyRollup,
        },
      })

      return new Response(JSON.stringify({ message: "No new messages to process." }), {
        status: 200,
        headers: { "Content-Type": "application/json" }
      })
    }

    // 2. Group by room_id and decide delivery targets
    const unprocessedByRoom = groupMessagesByRoom(unprocessedMessages ?? [])
    const todayByRoom = groupMessagesByRoom(todayMessages ?? [])
    const allRoomIds = new Set<string>([
      ...unprocessedByRoom.keys(),
      ...todayByRoom.keys(),
    ])

    const messagesByRoom: Record<string, string[]> = {}
    const roomsToSummarize: string[] = []
    const roomDeliveryTargets: string[] = []
    const roomContexts = new Map<string, RoomExecutionContext>()

    for (const roomId of allRoomIds) {
      const rs = roomSettingsMap.get(roomId)
      const roomEnabled = rs ? rs.is_enabled : true
      const roomHours = rs?.delivery_hours ?? globalHours
      if (!roomEnabled || !(forceRun || roomHours.includes(jstHour))) {
        continue
      }

      const effectiveCleanupTiming = rs?.message_cleanup_timing ?? messageCleanupTiming
      let effectiveSummaryMode = rs?.last_delivery_summary_mode ?? lastDeliverySummaryMode
      if (effectiveSummaryMode === 'daily_rollup' && effectiveCleanupTiming !== 'end_of_day') {
        effectiveSummaryMode = 'independent'
      }

      const roomLastHour = getLastScheduledHour(roomHours)
      const isRoomLastSlot = roomLastHour != null && roomLastHour === jstHour
      const shouldUseRoomDailyRollup = effectiveSummaryMode === 'daily_rollup'
        && effectiveCleanupTiming === 'end_of_day'
        && isRoomLastSlot

      const sourceMessages = shouldUseRoomDailyRollup
        ? (todayByRoom.get(roomId) ?? [])
        : (unprocessedByRoom.get(roomId) ?? [])
      if (sourceMessages.length === 0) {
        continue
      }

      roomsToSummarize.push(roomId)
      if (rs?.send_room_summary === true) {
        roomDeliveryTargets.push(roomId)
      }

      messagesByRoom[roomId] = sourceMessages.map((msg) => msg.content)
      roomContexts.set(roomId, {
        messageIds: sourceMessages.map((msg) => msg.id),
      })
    }

    const shouldSendRoomSummaries = roomDeliveryTargets.length > 0
    const shouldProcessAnyDelivery = shouldSendOverall || shouldSendRoomSummaries

    if (!shouldProcessAnyDelivery) {
      await writeDeliveryLog(supabase, {
        jst_hour: jstHour,
        status: 'not_scheduled',
        reason: 'Current hour is outside all enabled delivery schedules.',
        should_send_overall: false,
        rooms_targeted: 0,
        messages_in_queue: queueCount,
        messages_marked_processed: 0,
        line_send_attempted: false,
        line_send_success: false,
        target_room_id: overallRoomId || null,
        details: {
          scheduled_room_ids: roomsToSummarize,
          force_run: forceRun,
          message_cleanup_timing: messageCleanupTiming,
          last_delivery_summary_mode: lastDeliverySummaryMode,
          using_daily_rollup_scope: shouldUseOverallDailyRollup,
        },
      })

      return new Response(JSON.stringify({ message: "No delivery targets for this hour." }), {
        status: 200,
        headers: { "Content-Type": "application/json" }
      })
    }

    if (!lineAccessToken) {
      await writeDeliveryLog(supabase, {
        jst_hour: jstHour,
        status: 'line_config_missing',
        reason: 'LINE_CHANNEL_ACCESS_TOKEN is missing.',
        should_send_overall: shouldSendOverall,
        rooms_targeted: roomsToSummarize.length,
        messages_in_queue: queueCount,
        messages_marked_processed: 0,
        line_send_attempted: false,
        line_send_success: false,
        target_room_id: overallRoomId || roomDeliveryTargets[0] || null,
        details: {
          room_ids: roomsToSummarize,
          room_delivery_targets: roomDeliveryTargets,
          force_run: forceRun,
          message_cleanup_timing: messageCleanupTiming,
          last_delivery_summary_mode: lastDeliverySummaryMode,
          using_daily_rollup_scope: shouldUseOverallDailyRollup,
        },
      })

      return new Response(JSON.stringify({ error: "LINE delivery configuration is missing." }), {
        status: 500,
        headers: { "Content-Type": "application/json" }
      })
    }

    if (shouldSendOverall && !overallRoomId) {
      await writeDeliveryLog(supabase, {
        jst_hour: jstHour,
        status: 'line_config_missing',
        reason: 'LINE_OVERALL_ROOM_ID is missing while overall delivery is scheduled.',
        should_send_overall: true,
        rooms_targeted: roomsToSummarize.length,
        messages_in_queue: queueCount,
        messages_marked_processed: 0,
        line_send_attempted: false,
        line_send_success: false,
        target_room_id: null,
        details: {
          room_ids: roomsToSummarize,
          room_delivery_targets: roomDeliveryTargets,
          force_run: forceRun,
          message_cleanup_timing: messageCleanupTiming,
          last_delivery_summary_mode: lastDeliverySummaryMode,
          using_daily_rollup_scope: shouldUseOverallDailyRollup,
        },
      })

      return new Response(JSON.stringify({ error: "LINE delivery configuration is missing." }), {
        status: 500,
        headers: { "Content-Type": "application/json" }
      })
    }

    if (!groq) {
      await writeDeliveryLog(supabase, {
        jst_hour: jstHour,
        status: 'llm_config_missing',
        reason: 'GROQ_API_KEY is missing.',
        should_send_overall: shouldSendOverall,
        rooms_targeted: roomsToSummarize.length,
        messages_in_queue: queueCount,
        messages_marked_processed: 0,
        line_send_attempted: false,
        line_send_success: false,
        target_room_id: (shouldSendOverall ? overallRoomId : roomDeliveryTargets[0]) || null,
        details: {
          room_ids: roomsToSummarize,
          room_delivery_targets: roomDeliveryTargets,
          force_run: forceRun,
          message_cleanup_timing: messageCleanupTiming,
          last_delivery_summary_mode: lastDeliverySummaryMode,
          using_daily_rollup_scope: shouldUseOverallDailyRollup,
        },
      })

      return new Response(JSON.stringify({ error: "GROQ_API_KEY is missing." }), {
        status: 500,
        headers: { "Content-Type": "application/json" }
      })
    }

    const roomsForSummarySet = new Set<string>(roomDeliveryTargets)
    if (shouldSendOverall) {
      for (const roomId of roomsToSummarize) {
        roomsForSummarySet.add(roomId)
      }
    }
    const roomsForSummary = Array.from(roomsForSummarySet)
    const roomSummaries: Record<string, string> = {}

    // 3. Generate summary per scheduled room
    for (const roomId of roomsForSummary) {
      const contents = messagesByRoom[roomId]
      if (!contents || contents.length === 0) continue
      const roomText = contents.map((c, i) => `[${i + 1}]: ${c}`).join('\n')

      const response = await groq.chat.completions.create({
        model: "llama-3.3-70b-versatile",
        messages: [
          {
            role: "system", content: `あなたは優秀なAIアシスタントです。
以下のLINEのチャット履歴（発言内容）を読み、極めて簡潔で読みやすく要約してください。
【重要1】LINEでは「特定の過去の発言に対するリプライ（引用返信）」が使われるため、文脈が唐突に前後に飛ぶことがあります。会話のつながりを慎重に推測し、「誰がどの話題に対して返答や賛同をしているのか」を正確に汲み取って話の流れを整理してください。
【重要2】メッセージ内に「@〇〇」といったメンション（特定の相手への呼びかけや指定）が含まれている場合、それは発言者が特定の人物に何かを要求・連絡している重要なサインです。要約においても「誰宛てのメッセージ（指示・連絡等）か」が明確に伝わるように、メンションされた対象者を必ず拾い上げて記述してください。ただし、「@ALL」や「@All」など全員宛てのメンションだった場合は、特定の個人宛てとしてではなく、全体への連絡事項として通常の要約として扱ってください。
挨拶や意味のない雑談は思い切って削ぎ落とし、その1日の「決定事項」「重要な情報」「話の核心となる流れ」のみを抽出して、できるだけ短い文章でシャープに要約してください。ただし、短くまとめつつも「誰が見ても内容が正確に伝わる」ように表現を工夫してください。
また画像、動画、PDFなどのファイル、またはURLリンクが共有されていた場合は、要約を読んだ人が元のトークルームへ確認に行けるように「添付資料あり」「画像共有あり」「リンク共有あり」等の注意喚起を必ず含めてください。
必ず日本語で返信してください。` },
          { role: "user", content: `以下のメッセージリストを要約してください:\n\n${roomText}` }
        ]
      })

      const summary = response.choices[0].message?.content || "要約を生成できませんでした。"
      roomSummaries[roomId] = summary
    }

    if (Object.keys(roomSummaries).length === 0) {
      await writeDeliveryLog(supabase, {
        jst_hour: jstHour,
        status: 'no_room_summary',
        reason: 'No room summaries generated.',
        should_send_overall: shouldSendOverall,
        rooms_targeted: roomsToSummarize.length,
        messages_in_queue: queueCount,
        messages_marked_processed: 0,
        line_send_attempted: false,
        line_send_success: false,
        target_room_id: (shouldSendOverall ? overallRoomId : roomDeliveryTargets[0]) || null,
        details: {
          room_ids: roomsToSummarize,
          room_delivery_targets: roomDeliveryTargets,
          force_run: forceRun,
          message_cleanup_timing: messageCleanupTiming,
          last_delivery_summary_mode: lastDeliverySummaryMode,
          using_daily_rollup_scope: shouldUseOverallDailyRollup,
        },
      })

      return new Response(JSON.stringify({ message: "No room summaries generated." }), {
        status: 200,
        headers: { "Content-Type": "application/json" }
      })
    }

    let lineSendAttempted = false
    let lineSendSuccess = true
    let lineHttpStatus: number | null = null
    const successfulRoomDeliveries: string[] = []
    const failedRoomDeliveries: Array<{ room_id: string; status: number | null; error: string }> = []

    // 4. Deliver to the overall room when scheduled
    if (shouldSendOverall) {
      const summariesForOverall = roomsToSummarize.filter((roomId) => roomSummaries[roomId])
      if (summariesForOverall.length === 0) {
        await writeDeliveryLog(supabase, {
          jst_hour: jstHour,
          status: 'no_room_summary',
          reason: 'No summaries available for overall delivery.',
          should_send_overall: true,
          rooms_targeted: roomsToSummarize.length,
          messages_in_queue: queueCount,
          messages_marked_processed: 0,
          line_send_attempted: false,
          line_send_success: false,
          target_room_id: overallRoomId || null,
          details: {
            room_ids: roomsToSummarize,
            room_delivery_targets: roomDeliveryTargets,
            force_run: forceRun,
            message_cleanup_timing: messageCleanupTiming,
            last_delivery_summary_mode: lastDeliverySummaryMode,
            using_daily_rollup_scope: shouldUseOverallDailyRollup,
          },
        })

        return new Response(JSON.stringify({ message: "No summaries available for overall delivery." }), {
          status: 200,
          headers: { "Content-Type": "application/json" }
        })
      }

      const allSummariesText = summariesForOverall
        .map((roomId) => {
          const displayName = getRoomDisplayName(roomId, roomSettingsMap.get(roomId))
          return `■ ${displayName}\n${roomSummaries[roomId]}`
        })
        .join('\n\n')

      const overallResponse = await groq.chat.completions.create({
        model: "llama-3.3-70b-versatile",
        messages: [
          {
            role: "system", content: `あなたは優秀な全体の管理者AIです。
複数のLINEルームで行われた会話の「各ルームの要約」を受け取ります。
全てのルームの動きを俯瞰し、全体としてどのような話題が挙がっているかを統合したレポート（全体要約）を作成してください。
このレポートは、忙しい管理者がサッと目を通せるように、無駄な言葉を省いて極めて簡潔・シャープに記述してください。ただし、短くしすぎて意味が通じなくなることがないよう「各ルームでその日どんな話があり、何が決まったか」の核心部分はしっかりと伝わる表現に工夫してください。
また、各ルームの要約内に「@〇〇さん宛ての指示・連絡」のような特定の人物への名指し（メンション）が含まれていた場合は、それが誰宛てのものかが全体要約でも失われないように確実に記述してください。（※ただし「全員宛て（@ALL等）」の連絡事項に関しては、特定の個人宛てとして区別せず通常の要約内容として扱ってください。）
なお、各ルームの要約に「添付資料あり」「画像共有あり」「リンクあり」などの報告が含まれている場合は、全体要約でもそれが一目でわかるように（誰かが確認に行くべき情報として）特記してください。
【重要】出力時のフォーマット指定：
「■ {ルーム表示名}」の見出しで各ルームの要約ブロックを出力し、前のルームとの間に「必ず1行の空白（空行）」を挿入して、視覚的に読みやすく区切ってください。
必ず日本語で返信してください。` },
          { role: "user", content: `以下の各ルームの要約を統合し、全体レポートを作成してください:\n\n${allSummariesText}` }
        ]
      })

      const overallSummary = overallResponse.choices[0].message?.content || "全体要約を生成できませんでした。"
      const overallTitle = shouldUseOverallDailyRollup ? '【全体 1日まとめレポート】' : '【全体 定期要約レポート】'
      const overallSendResult = await sendLineMessage(
        overallRoomId,
        `${overallTitle}\n\n${overallSummary}`,
        lineAccessToken,
      )
      lineSendAttempted = true
      if (overallSendResult.status != null) lineHttpStatus = overallSendResult.status

      if (!overallSendResult.ok) {
        lineSendSuccess = false
        await writeDeliveryLog(supabase, {
          jst_hour: jstHour,
          status: 'line_send_failed',
          reason: overallSendResult.error,
          should_send_overall: true,
          rooms_targeted: roomsToSummarize.length,
          messages_in_queue: queueCount,
          messages_marked_processed: 0,
          line_send_attempted: true,
          line_send_success: false,
          line_http_status: overallSendResult.status ?? null,
          target_room_id: overallRoomId,
          details: {
            scope: 'overall',
            room_ids: roomsToSummarize,
            room_delivery_targets: roomDeliveryTargets,
            force_run: forceRun,
            message_cleanup_timing: messageCleanupTiming,
            last_delivery_summary_mode: lastDeliverySummaryMode,
            using_daily_rollup_scope: shouldUseOverallDailyRollup,
          },
        })

        return new Response(JSON.stringify({
          error: "LINE message delivery failed.",
          detail: overallSendResult.error,
        }), {
          status: 502,
          headers: { "Content-Type": "application/json" }
        })
      }
    }

    // 5. Deliver room summaries to each room (opt-in only)
    for (const roomId of roomDeliveryTargets) {
      const summary = roomSummaries[roomId]
      if (!summary) continue
      const roomSendResult = await sendLineMessage(
        roomId,
        `【このルーム 定期要約レポート】\n\n${summary}`,
        lineAccessToken,
      )
      lineSendAttempted = true
      if (roomSendResult.status != null) lineHttpStatus = roomSendResult.status

      if (!roomSendResult.ok) {
        lineSendSuccess = false
        failedRoomDeliveries.push({
          room_id: roomId,
          status: roomSendResult.status ?? null,
          error: roomSendResult.error,
        })
        continue
      }
      successfulRoomDeliveries.push(roomId)
    }

    if (!shouldSendOverall && roomDeliveryTargets.length > 0 && successfulRoomDeliveries.length === 0) {
      await writeDeliveryLog(supabase, {
        jst_hour: jstHour,
        status: 'line_send_failed',
        reason: 'Failed to deliver room summaries to all configured rooms.',
        should_send_overall: false,
        rooms_targeted: roomsToSummarize.length,
        messages_in_queue: queueCount,
        messages_marked_processed: 0,
        line_send_attempted: lineSendAttempted,
        line_send_success: false,
        line_http_status: lineHttpStatus,
        target_room_id: roomDeliveryTargets[0] || null,
        details: {
          room_ids: roomsToSummarize,
          room_delivery_targets: roomDeliveryTargets,
          room_delivery_failed: failedRoomDeliveries,
          force_run: forceRun,
          message_cleanup_timing: messageCleanupTiming,
          last_delivery_summary_mode: lastDeliverySummaryMode,
          using_daily_rollup_scope: shouldUseOverallDailyRollup,
        },
      })

      return new Response(JSON.stringify({
        error: "LINE message delivery failed for room summaries.",
        detail: failedRoomDeliveries,
      }), {
        status: 502,
        headers: { "Content-Type": "application/json" }
      })
    }

    const deliveredRoomIds = shouldSendOverall ? roomsToSummarize : successfulRoomDeliveries
    const targetRoomId = (shouldSendOverall ? overallRoomId : roomDeliveryTargets[0]) || null

    const markProcessedMessageIds = new Set<string>()

    for (const roomId of deliveredRoomIds) {
      const context = roomContexts.get(roomId)
      if (!context) continue
      for (const id of context.messageIds) markProcessedMessageIds.add(id)
    }

    let affectedCount = 0
    let markedProcessedCount = 0
    let cleanupAction: 'deleted' | 'marked_processed' | 'end_of_day_deleted' | 'mixed' | 'none' = 'none'

    const writeDbUpdateFailure = async (reason: string) => {
      await writeDeliveryLog(supabase, {
        jst_hour: jstHour,
        status: 'db_update_failed',
        reason,
        should_send_overall: shouldSendOverall,
        rooms_targeted: roomsToSummarize.length,
        messages_in_queue: queueCount,
        messages_marked_processed: 0,
        line_send_attempted: lineSendAttempted,
        line_send_success: lineSendSuccess,
        line_http_status: lineHttpStatus,
        target_room_id: targetRoomId,
        details: {
          room_ids: roomsToSummarize,
          room_delivery_targets: roomDeliveryTargets,
          room_delivery_success: successfulRoomDeliveries,
          room_delivery_failed: failedRoomDeliveries,
          force_run: forceRun,
          message_cleanup_timing: messageCleanupTiming,
          last_delivery_summary_mode: lastDeliverySummaryMode,
          using_daily_rollup_scope: shouldUseOverallDailyRollup,
          room_cleanup_breakdown: {
            mark_processed_message_ids: markProcessedMessageIds.size,
            message_retention_days: messageRetentionDays,
          },
        },
      })
    }

    if (markProcessedMessageIds.size > 0) {
      const result = await markMessagesProcessedByIds(supabase, Array.from(markProcessedMessageIds))
      if (result.error) {
        await writeDbUpdateFailure(result.error.message)
        throw new Error(`Error marking delivered messages as processed: ${result.error.message}`)
      }
      markedProcessedCount = result.affectedCount
      affectedCount += result.affectedCount
    }

    cleanupAction = markProcessedMessageIds.size > 0 ? 'marked_processed' : 'none'

    const hasRoomFailures = failedRoomDeliveries.length > 0
    const successReason = hasRoomFailures
      ? 'Overall delivery succeeded, but some room-summary deliveries failed.'
      : cleanupAction === 'marked_processed'
        ? 'Scheduled deliveries succeeded and delivered messages were marked as processed.'
        : 'Deliveries succeeded but there were no message rows to update.'

    await writeDeliveryLog(supabase, {
      jst_hour: jstHour,
      status: hasRoomFailures ? 'delivered_with_room_failures' : (affectedCount > 0 ? 'delivered' : 'delivered_no_messages_to_mark'),
      reason: successReason,
      should_send_overall: shouldSendOverall,
      rooms_targeted: roomsToSummarize.length,
      messages_in_queue: queueCount,
      messages_marked_processed: affectedCount,
      line_send_attempted: lineSendAttempted,
      line_send_success: lineSendSuccess,
      line_http_status: lineHttpStatus,
      target_room_id: targetRoomId,
      details: {
        room_ids: roomsToSummarize,
        room_delivery_targets: roomDeliveryTargets,
        room_delivery_success: successfulRoomDeliveries,
        room_delivery_failed: failedRoomDeliveries,
        force_run: forceRun,
        cleanup_action: cleanupAction,
        cleanup_marked_processed_count: markedProcessedCount,
        message_cleanup_timing: messageCleanupTiming,
        message_retention_days: messageRetentionDays,
        last_delivery_summary_mode: lastDeliverySummaryMode,
        using_daily_rollup_scope: shouldUseOverallDailyRollup,
      },
    })

    return new Response(JSON.stringify({
      success: true,
      delivered: true,
      roomsProcessed: deliveredRoomIds.length,
      overallDelivered: shouldSendOverall,
      messageUpdateCount: affectedCount,
      cleanupAction: cleanupAction,
      roomDelivery: {
        targeted: roomDeliveryTargets.length,
        success: successfulRoomDeliveries.length,
        failed: failedRoomDeliveries.length,
      },
    }), {
      headers: { 'Content-Type': 'application/json' },
      status: 200,
    })

  } catch (err: any) {
    console.error('Error processing cron job:', err)

    if (supabase && jstHour >= 0) {
      await writeDeliveryLog(supabase, {
        jst_hour: jstHour,
        status: 'runtime_error',
        reason: err?.message || 'Internal Server Error',
        should_send_overall: false,
        rooms_targeted: 0,
        messages_in_queue: 0,
        messages_marked_processed: 0,
        line_send_attempted: false,
        line_send_success: false,
        target_room_id: null,
      })
    }

    return new Response(JSON.stringify({ error: err.message || 'Internal Server Error' }), {
      status: 500,
      headers: { "Content-Type": "application/json" }
    })
  }
})

async function maybeSendTomorrowCalendarReminder(params: {
  supabase: ReturnType<typeof createClient>
  now: Date
  jstHour: number
  lineAccessToken: string
  overallRoomId: string
  settings: TomorrowReminderSettings
}): Promise<void> {
  const {
    supabase,
    now,
    jstHour,
    lineAccessToken,
    overallRoomId,
    settings,
  } = params

  if (!settings.enabled) return

  if (!lineAccessToken) {
    return
  }

  if (!settings.hours.includes(jstHour)) {
    return
  }

  const targetRoomIds = await resolveTomorrowReminderTargetRooms(supabase, overallRoomId)
  if (targetRoomIds.length === 0) {
    return
  }

  const calendarEnvState = loadCalendarEnv()
  if (!calendarEnvState.ok) {
    console.warn(`Tomorrow reminder skipped: missing calendar env (${calendarEnvState.missing.join(', ')})`)
    return
  }

  const todayJst = getTodayJstDateString(now)
  const tomorrowJst = addDaysToDateString(todayJst, 1)
  const jstDayRange = getJstDayRange(now)

  const pendingTargets: string[] = []
  for (const roomId of targetRoomIds) {
    const alreadySent = await hasSentTomorrowReminderForRoomHour(
      supabase,
      tomorrowJst,
      jstDayRange.startIso,
      jstDayRange.endIso,
      roomId,
      jstHour,
    )
    if (!alreadySent) {
      pendingTargets.push(roomId)
    }
  }
  if (pendingTargets.length === 0) {
    return
  }

  const accessToken = await fetchGoogleAccessToken(calendarEnvState.env)
  const events = await fetchCalendarEventsForJstDate(calendarEnvState.env, accessToken, tomorrowJst)
  if (settings.onlyIfEvents && events.length === 0) {
    return
  }

  const message = buildTomorrowReminderMessage(
    events,
    tomorrowJst,
    calendarEnvState.env.timezone,
    settings.maxItems,
  )

  for (const targetRoomId of pendingTargets) {
    const sendResult = await sendLineMessage(targetRoomId, message, lineAccessToken)
    if (!sendResult.ok) {
      await writeDeliveryLog(supabase, {
        jst_hour: jstHour,
        status: 'calendar_tomorrow_send_failed',
        reason: sendResult.error,
        should_send_overall: false,
        rooms_targeted: 1,
        messages_in_queue: 0,
        messages_marked_processed: 0,
        line_send_attempted: true,
        line_send_success: false,
        line_http_status: sendResult.status ?? null,
        target_room_id: targetRoomId,
        details: {
          target_date: tomorrowJst,
          event_count: events.length,
          reminder_hour: jstHour,
        },
      })
      continue
    }

    await writeDeliveryLog(supabase, {
      jst_hour: jstHour,
      status: 'calendar_tomorrow_sent',
      reason: events.length > 0 ? `Sent ${events.length} events for tomorrow.` : 'No events for tomorrow.',
      should_send_overall: false,
      rooms_targeted: 1,
      messages_in_queue: 0,
      messages_marked_processed: 0,
      line_send_attempted: true,
      line_send_success: true,
      line_http_status: sendResult.status ?? null,
      target_room_id: targetRoomId,
      details: {
        target_date: tomorrowJst,
        event_count: events.length,
        reminder_hour: jstHour,
      },
    })
  }
}

async function resolveTomorrowReminderTargetRooms(
  supabase: ReturnType<typeof createClient>,
  fallbackOverallRoomId: string,
): Promise<string[]> {
  const { data, error } = await supabase
    .from('room_summary_settings')
    .select('room_id, is_enabled, calendar_tomorrow_reminder_enabled')

  if (error) {
    console.error('Failed to fetch room settings for tomorrow reminder targets:', error.message)
    const fallback = String(fallbackOverallRoomId ?? '').trim()
    return fallback ? [fallback] : []
  }

  const rows = Array.isArray(data) ? data : []
  const enabledRoomIds = rows
    .filter((row: any) => row?.is_enabled !== false && row?.calendar_tomorrow_reminder_enabled === true)
    .map((row: any) => String(row?.room_id ?? '').trim())
    .filter((roomId: string) => roomId.length > 0)

  if (enabledRoomIds.length > 0) {
    return Array.from(new Set(enabledRoomIds))
  }

  if (rows.length > 0) {
    return []
  }

  const fallback = String(fallbackOverallRoomId ?? '').trim()
  return fallback ? [fallback] : []
}

async function hasSentTomorrowReminderForRoomHour(
  supabase: ReturnType<typeof createClient>,
  targetDate: string,
  dayStartIso: string,
  dayEndIso: string,
  targetRoomId: string,
  reminderHour: number,
): Promise<boolean> {
  const { data, error } = await supabase
    .from('summary_delivery_logs')
    .select('id, details')
    .eq('status', 'calendar_tomorrow_sent')
    .eq('target_room_id', targetRoomId)
    .gte('run_at', dayStartIso)
    .lt('run_at', dayEndIso)
    .limit(30)

  if (error) {
    console.error('Failed to check tomorrow reminder log:', error.message)
    return false
  }
  const rows = Array.isArray(data) ? data : []
  return rows.some((row: any) => {
    const target = String(row?.details?.target_date ?? '').trim()
    if (target !== targetDate) return false
    const loggedHour = Number(row?.details?.reminder_hour)
    if (!Number.isInteger(loggedHour)) return true
    return loggedHour === reminderHour
  })
}

async function maybeSendGmailReservationAlerts(params: {
  supabase: ReturnType<typeof createClient>
  now: Date
  jstHour: number
  lineAccessToken: string
  fallbackOverallRoomId: string
}): Promise<void> {
  const {
    supabase,
    now,
    jstHour,
    lineAccessToken,
    fallbackOverallRoomId,
  } = params

  const envState = loadGmailAlertEnv(fallbackOverallRoomId)
  if (!envState.ok) {
    console.warn(`Gmail alert skipped: missing env (${envState.missing.join(', ')})`)
    return
  }
  const env = envState.env
  if (!env.enabled) {
    return
  }
  if (!lineAccessToken) {
    console.warn('Gmail alert skipped: LINE_CHANNEL_ACCESS_TOKEN is missing.')
    return
  }

  const targetRoomIds = await resolveGmailAlertTargetRooms(supabase, env.fallbackTargetRoomId)
  if (targetRoomIds.length === 0) {
    console.warn('Gmail alert skipped: no target rooms are enabled.')
    return
  }

  const accessToken = await fetchGmailAccessTokenByRefreshToken(env)
  const listedMessages = await listGmailMessages(accessToken, env.query, env.maxMessages)
  if (listedMessages.length === 0) {
    return
  }

  const unnotifiedMessageIds = await filterUnnotifiedGmailMessageIds(
    supabase,
    listedMessages.map((message) => message.id),
  )
  if (unnotifiedMessageIds.length === 0) {
    return
  }

  const unnotifiedSet = new Set(unnotifiedMessageIds)
  const messagesToFetch = listedMessages.filter((message) => unnotifiedSet.has(message.id))
  const alerts: GmailMessageAlert[] = []
  for (const message of messagesToFetch) {
    const detail = await fetchGmailMessageAlert(accessToken, message.id)
    if (!detail) continue
    alerts.push(detail)
  }

  if (alerts.length === 0) {
    return
  }

  const lineText = buildGmailReservationAlertMessage(alerts)
  const successfulTargetRoomIds: string[] = []
  for (const targetRoomId of targetRoomIds) {
    const sendResult = await sendLineMessage(targetRoomId, lineText, lineAccessToken)
    if (!sendResult.ok) {
      await writeDeliveryLog(supabase, {
        jst_hour: jstHour,
        status: 'gmail_alert_send_failed',
        reason: sendResult.error,
        should_send_overall: false,
        rooms_targeted: 1,
        messages_in_queue: 0,
        messages_marked_processed: 0,
        line_send_attempted: true,
        line_send_success: false,
        line_http_status: sendResult.status ?? null,
        target_room_id: targetRoomId,
        details: {
          gmail_query: env.query,
          listed_count: listedMessages.length,
          unnotified_count: alerts.length,
          target_room_count: targetRoomIds.length,
        },
      })
      continue
    }

    successfulTargetRoomIds.push(targetRoomId)
    await writeDeliveryLog(supabase, {
      jst_hour: jstHour,
      status: 'gmail_alert_sent',
      reason: `Sent ${alerts.length} reservation email alerts.`,
      should_send_overall: false,
      rooms_targeted: 1,
      messages_in_queue: 0,
      messages_marked_processed: 0,
      line_send_attempted: true,
      line_send_success: true,
      line_http_status: sendResult.status ?? null,
      target_room_id: targetRoomId,
      details: {
        gmail_query: env.query,
        listed_count: listedMessages.length,
        unnotified_count: alerts.length,
        target_room_count: targetRoomIds.length,
      },
    })
  }

  if (successfulTargetRoomIds.length === 0) {
    return
  }

  await saveGmailReservationAlertLogs(supabase, alerts, successfulTargetRoomIds[0], now)
}

async function resolveGmailAlertTargetRooms(
  supabase: ReturnType<typeof createClient>,
  fallbackTargetRoomId: string,
): Promise<string[]> {
  const { data, error } = await supabase
    .from('room_summary_settings')
    .select('room_id, is_enabled, gmail_reservation_alert_enabled')

  if (error) {
    console.error('Failed to fetch room settings for Gmail alert targets:', error.message)
    const fallback = String(fallbackTargetRoomId ?? '').trim()
    return fallback ? [fallback] : []
  }

  const rows = Array.isArray(data) ? data : []
  const enabledRoomIds = rows
    .filter((row: any) => row?.is_enabled !== false && row?.gmail_reservation_alert_enabled === true)
    .map((row: any) => String(row?.room_id ?? '').trim())
    .filter((roomId: string) => roomId.length > 0)

  if (enabledRoomIds.length > 0) {
    return Array.from(new Set(enabledRoomIds))
  }

  if (rows.length > 0) {
    return []
  }

  const fallback = String(fallbackTargetRoomId ?? '').trim()
  return fallback ? [fallback] : []
}

function loadGmailAlertEnv(fallbackOverallRoomId: string): GmailAlertEnvState {
  const clientId = String(Deno.env.get('GMAIL_CLIENT_ID') ?? '').trim()
  const clientSecret = String(Deno.env.get('GMAIL_CLIENT_SECRET') ?? '').trim()
  const refreshToken = String(Deno.env.get('GMAIL_REFRESH_TOKEN') ?? '').trim()
  const query = String(Deno.env.get('GMAIL_ALERT_QUERY') ?? '').trim() || DEFAULT_GMAIL_ALERT_QUERY
  const fallbackTargetRoomId = String(Deno.env.get('LINE_GMAIL_ALERT_ROOM_ID') ?? '').trim() || String(fallbackOverallRoomId ?? '').trim()

  const hasAnyCredential = !!clientId || !!clientSecret || !!refreshToken
  const enabled = parseBooleanEnv(Deno.env.get('GMAIL_ALERT_ENABLED'), hasAnyCredential)
  const rawMaxMessages = Number(Deno.env.get('GMAIL_ALERT_MAX_MESSAGES') ?? DEFAULT_GMAIL_ALERT_MAX_MESSAGES)
  const maxMessages = Number.isInteger(rawMaxMessages) && rawMaxMessages >= 1 && rawMaxMessages <= MAX_GMAIL_ALERT_MAX_MESSAGES
    ? rawMaxMessages
    : DEFAULT_GMAIL_ALERT_MAX_MESSAGES

  if (!enabled) {
    return {
      ok: true,
      env: {
        enabled: false,
        clientId,
        clientSecret,
        refreshToken,
        query,
        maxMessages,
        fallbackTargetRoomId,
      },
    }
  }

  const missing: string[] = []
  if (!clientId) missing.push('GMAIL_CLIENT_ID')
  if (!clientSecret) missing.push('GMAIL_CLIENT_SECRET')
  if (!refreshToken) missing.push('GMAIL_REFRESH_TOKEN')
  if (missing.length > 0) {
    return { ok: false, missing }
  }

  return {
    ok: true,
    env: {
      enabled: true,
      clientId,
      clientSecret,
      refreshToken,
      query,
      maxMessages,
      fallbackTargetRoomId,
    },
  }
}

async function fetchGmailAccessTokenByRefreshToken(env: GmailAlertEnv): Promise<string> {
  const body = new URLSearchParams()
  body.set('client_id', env.clientId)
  body.set('client_secret', env.clientSecret)
  body.set('refresh_token', env.refreshToken)
  body.set('grant_type', 'refresh_token')

  const response = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: body.toString(),
  })

  if (!response.ok) {
    const text = await response.text()
    throw new Error(`Gmail OAuth token request failed (${response.status}): ${text}`)
  }

  const json = await response.json()
  const accessToken = String(json?.access_token ?? '')
  if (!accessToken) {
    throw new Error('Gmail OAuth token response missing access_token.')
  }
  return accessToken
}

async function listGmailMessages(
  accessToken: string,
  query: string,
  maxMessages: number,
): Promise<GmailMessageListItem[]> {
  const url = new URL('https://gmail.googleapis.com/gmail/v1/users/me/messages')
  url.searchParams.set('maxResults', String(maxMessages))
  url.searchParams.set('includeSpamTrash', 'false')
  if (query) {
    url.searchParams.set('q', query)
  }

  const response = await fetch(url.toString(), {
    method: 'GET',
    headers: {
      'Authorization': `Bearer ${accessToken}`,
      'Content-Type': 'application/json',
    },
  })

  if (!response.ok) {
    const text = await response.text()
    throw new Error(`Gmail messages.list failed (${response.status}): ${text}`)
  }

  const data = await response.json()
  const rows = Array.isArray(data?.messages) ? data.messages : []
  return rows
    .map((row: any) => ({
      id: String(row?.id ?? '').trim(),
      threadId: String(row?.threadId ?? '').trim() || null,
    }))
    .filter((row: GmailMessageListItem) => row.id.length > 0)
}

async function filterUnnotifiedGmailMessageIds(
  supabase: ReturnType<typeof createClient>,
  messageIds: string[],
): Promise<string[]> {
  if (messageIds.length === 0) {
    return []
  }

  const uniqueIds = Array.from(new Set(messageIds.map((value) => String(value ?? '').trim()).filter((value) => value.length > 0)))
  if (uniqueIds.length === 0) {
    return []
  }

  const { data, error } = await supabase
    .from('gmail_reservation_alert_logs')
    .select('gmail_message_id')
    .in('gmail_message_id', uniqueIds)

  if (error) {
    throw new Error(`Failed to query Gmail alert log table: ${error.message}`)
  }

  const existing = new Set<string>(
    (Array.isArray(data) ? data : [])
      .map((row: any) => String(row?.gmail_message_id ?? '').trim())
      .filter((value: string) => value.length > 0),
  )
  return uniqueIds.filter((id) => !existing.has(id))
}

async function fetchGmailMessageAlert(
  accessToken: string,
  messageId: string,
): Promise<GmailMessageAlert | null> {
  const normalizedMessageId = String(messageId ?? '').trim()
  if (!normalizedMessageId) {
    return null
  }

  const url = new URL(`https://gmail.googleapis.com/gmail/v1/users/me/messages/${encodeURIComponent(normalizedMessageId)}`)
  url.searchParams.set('format', 'metadata')
  url.searchParams.append('metadataHeaders', 'Subject')
  url.searchParams.append('metadataHeaders', 'From')
  url.searchParams.append('metadataHeaders', 'Date')

  const response = await fetch(url.toString(), {
    method: 'GET',
    headers: {
      'Authorization': `Bearer ${accessToken}`,
      'Content-Type': 'application/json',
    },
  })

  if (!response.ok) {
    const text = await response.text()
    throw new Error(`Gmail messages.get failed (${response.status}): ${text}`)
  }

  const data = await response.json()
  const payloadHeaders = Array.isArray(data?.payload?.headers) ? data.payload.headers : []
  const subject = normalizeInlineText(extractGmailHeader(payloadHeaders, 'subject')) || '(件名なし)'
  const from = normalizeInlineText(extractGmailHeader(payloadHeaders, 'from')) || '(送信元不明)'
  const snippet = normalizeInlineText(String(data?.snippet ?? ''))
  const internalDateMs = Number(data?.internalDate)
  const internalDateIso =
    Number.isFinite(internalDateMs) && internalDateMs > 0
      ? new Date(internalDateMs).toISOString()
      : null

  return {
    id: String(data?.id ?? normalizedMessageId),
    threadId: String(data?.threadId ?? '').trim() || null,
    subject,
    from,
    snippet,
    internalDateIso,
  }
}

function extractGmailHeader(
  headers: Array<{ name?: string; value?: string }>,
  headerName: string,
): string {
  const target = headerName.toLowerCase()
  for (const header of headers) {
    const name = String(header?.name ?? '').trim().toLowerCase()
    if (name !== target) continue
    return String(header?.value ?? '')
  }
  return ''
}

function buildGmailReservationAlertMessage(alerts: GmailMessageAlert[]): string {
  const lines: string[] = [`【予約メール通知】新着${alerts.length}件`]
  for (let i = 0; i < alerts.length; i += 1) {
    const alert = alerts[i]
    lines.push(`${i + 1}.`)
    lines.push(`  受信: ${formatGmailAlertReceivedAt(alert.internalDateIso)}`)
    lines.push(`  件名: ${alert.subject || '(件名なし)'}`)
    lines.push(`  送信元: ${alert.from || '(送信元不明)'}`)
    lines.push(`  内容: ${formatGmailAlertSnippet(alert.snippet)}`)
    if (i < alerts.length - 1) {
      lines.push('')
    }
  }
  return lines.join('\n').slice(0, 4900)
}

function formatGmailAlertReceivedAt(iso: string | null): string {
  if (!iso) return '(受信時刻不明)'
  const date = new Date(iso)
  if (Number.isNaN(date.getTime())) return '(受信時刻不明)'
  return `${formatDateOnlyForLine(date, 'Asia/Tokyo')} ${formatTimeOnlyForLine(date, 'Asia/Tokyo')}`
}

function formatGmailAlertSnippet(snippet: string): string {
  const normalized = normalizeInlineText(snippet)
  if (!normalized) return '（本文プレビューなし）'
  if (normalized.length <= 120) return normalized
  return `${normalized.slice(0, 120)}...`
}

async function saveGmailReservationAlertLogs(
  supabase: ReturnType<typeof createClient>,
  alerts: GmailMessageAlert[],
  targetRoomId: string,
  now: Date,
): Promise<void> {
  if (alerts.length === 0) return
  const lineSentAt = now.toISOString()
  const rows = alerts.map((alert) => ({
    gmail_message_id: alert.id,
    gmail_thread_id: alert.threadId,
    gmail_subject: alert.subject,
    gmail_from: alert.from,
    gmail_internal_date: alert.internalDateIso,
    line_target_room_id: targetRoomId,
    line_message_sent_at: lineSentAt,
  }))
  const { error } = await supabase
    .from('gmail_reservation_alert_logs')
    .upsert(rows, { onConflict: 'gmail_message_id' })
  if (error) {
    throw new Error(`Failed to save Gmail alert logs: ${error.message}`)
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
      serviceAccountPrivateKey: serviceAccountPrivateKey.replace(/\\n/g, '\n'),
      timezone,
    },
  }
}

async function fetchCalendarEventsForJstDate(
  env: CalendarEnv,
  accessToken: string,
  date: string,
): Promise<GoogleCalendarEvent[]> {
  const range = dayRangeFromJstDate(date)
  const url = new URL(`https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(env.calendarId)}/events`)
  url.searchParams.set('singleEvents', 'true')
  url.searchParams.set('orderBy', 'startTime')
  url.searchParams.set('timeMin', range.start.toISOString())
  url.searchParams.set('timeMax', range.end.toISOString())
  url.searchParams.set('maxResults', '100')
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
  const items: GoogleCalendarEvent[] = Array.isArray(data?.items) ? data.items : []
  return items
}

function buildTomorrowReminderMessage(
  events: GoogleCalendarEvent[],
  targetDate: string,
  timezone: string,
  maxItems: number,
): string {
  const targetDateLabel = targetDate.replace(/-/g, '/')
  const heading = `【明日の予定】\n${targetDateLabel}`
  if (events.length === 0) {
    return `${heading}\n（予定なし）`
  }

  const safeMaxItems =
    Number.isInteger(maxItems) && maxItems >= 1 && maxItems <= 50
      ? maxItems
      : DEFAULT_TOMORROW_REMINDER_MAX_ITEMS
  const shown = events.slice(0, safeMaxItems)
  const lines: string[] = [`${heading}（${events.length}件）`]
  for (let i = 0; i < shown.length; i += 1) {
    const detail = formatCalendarEventDetail(shown[i], timezone)
    lines.push(`${i + 1}.`)
    lines.push(`  日付: ${detail.date}`)
    lines.push(`  時間: ${detail.time}`)
    lines.push(`  予定: ${detail.title}`)
    lines.push(`  内容: ${detail.content}`)
    if (i < shown.length - 1) {
      lines.push('')
    }
  }
  if (events.length > safeMaxItems) {
    lines.push('')
    lines.push(`他 ${events.length - safeMaxItems} 件`)
  }
  return lines.join('\n').slice(0, 4900)
}

function formatCalendarEventDetail(
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

  const title = normalizeInlineText(String(event.summary ?? '(無題)'))
  const content = formatCalendarEventContent(event)
  return { date, time, title: title || '(無題)', content }
}

function formatCalendarEventContent(event: GoogleCalendarEvent): string {
  const pieces: string[] = []
  const location = normalizeInlineText(String(event.location ?? ''))
  if (location) pieces.push(location)

  const description = sanitizeCalendarDescription(String(event.description ?? ''))
  if (description) pieces.push(description)

  if (pieces.length === 0) return '（内容なし）'
  return pieces.join(' / ')
}

function sanitizeCalendarDescription(raw: string): string {
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
  return String(raw ?? '').replace(/\u3000/g, ' ').replace(/\s+/g, ' ').trim()
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

function getTodayJstDateString(base = new Date()): string {
  const jst = new Date(base.getTime() + JST_OFFSET_MS)
  const y = jst.getUTCFullYear()
  const m = String(jst.getUTCMonth() + 1).padStart(2, '0')
  const d = String(jst.getUTCDate()).padStart(2, '0')
  return `${y}-${m}-${d}`
}

function addDaysToDateString(date: string, days: number): string {
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

async function writeDeliveryLog(
  supabase: ReturnType<typeof createClient>,
  payload: {
    jst_hour: number
    status: string
    reason?: string
    should_send_overall: boolean
    rooms_targeted: number
    messages_in_queue: number
    messages_marked_processed: number
    line_send_attempted: boolean
    line_send_success: boolean
    line_http_status?: number | null
    target_room_id: string | null
    details?: Record<string, unknown>
  }
) {
  if (!shouldPersistDeliveryLog(payload.status, payload.details)) {
    return
  }
  try {
    const { error } = await supabase
      .from('summary_delivery_logs')
      .insert({
        ...payload,
        details: payload.details ?? {},
      })

    if (error) {
      console.error('Failed to insert summary_delivery_logs:', error.message)
      return
    }

    await pruneDeliveryLogs(supabase, 50)
  } catch (e) {
    console.error('Unexpected error while inserting summary_delivery_logs:', e)
  }
}

const nonActionableLogStatuses = new Set([
  'no_messages',
  'not_scheduled',
  'no_room_summary',
  'overall_schedule_skip',
])

function shouldPersistDeliveryLog(
  status: string,
  details?: Record<string, unknown>,
): boolean {
  const normalized = String(status || '').trim().toLowerCase()
  if (!normalized) return true
  if (isForceRunLogDetails(details)) return true
  return !nonActionableLogStatuses.has(normalized)
}

function isForceRunLogDetails(details: Record<string, unknown> | undefined): boolean {
  if (!details || typeof details !== 'object') return false
  return details.force_run === true
}

async function pruneDeliveryLogs(
  supabase: ReturnType<typeof createClient>,
  keepLatest: number,
) {
  if (!Number.isInteger(keepLatest) || keepLatest <= 0) return
  try {
    // Keep newest N rows by id and remove older logs.
    const cutoffIndex = keepLatest - 1
    const { data: cutoff, error: cutoffError } = await supabase
      .from('summary_delivery_logs')
      .select('id')
      .order('id', { ascending: false })
      .range(cutoffIndex, cutoffIndex)
      .maybeSingle()

    if (cutoffError || !cutoff?.id) {
      return
    }

    const { error: deleteError } = await supabase
      .from('summary_delivery_logs')
      .delete()
      .lt('id', cutoff.id)

    if (deleteError) {
      console.error('Failed to prune summary_delivery_logs:', deleteError.message)
    }
  } catch (e) {
    console.error('Unexpected error while pruning summary_delivery_logs:', e)
  }
}

async function sendLineMessage(to: string, text: string, token: string) {
  try {
    const response = await fetch('https://api.line.me/v2/bot/message/push', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${token}`
      },
      body: JSON.stringify({
        to: to,
        messages: [{ type: 'text', text: text }]
      })
    })

    if (!response.ok) {
      const errorText = await response.text()
      console.error(`Failed to send LINE message to ${to}. Status: ${response.status} Error: ${errorText}`)
      return { ok: false, status: response.status, error: errorText || `HTTP ${response.status}` }
    } else {
      console.log(`Successfully sent message to ${to}`)
      return { ok: true, status: response.status as number }
    }
  } catch (error) {
    console.error(`Network or fetch error while sending to ${to}:`, error)
    return { ok: false, error: error instanceof Error ? error.message : String(error) }
  }
}

function isForceRun(req: Request): boolean {
  const url = new URL(req.url)
  const raw = (url.searchParams.get('force') ?? '').trim().toLowerCase()
  return raw === '1' || raw === 'true' || raw === 'yes' || raw === 'on'
}

function normalizeMessageCleanupTiming(value: unknown): MessageCleanupTiming {
  return value === 'end_of_day' ? 'end_of_day' : 'after_each_delivery'
}

function normalizeLastDeliverySummaryMode(value: unknown): LastDeliverySummaryMode {
  return value === 'daily_rollup' ? 'daily_rollup' : 'independent'
}

function normalizeMessageRetentionDays(value: unknown): MessageRetentionDays {
  const days = Number(value)
  if (days === 0 || days === 60 || days === 120 || days === 180 || days === 365 || days === 730 || days === 1095) {
    return days
  }
  return DEFAULT_MESSAGE_RETENTION_DAYS
}

function parseBooleanEnv(value: string | undefined, fallback: boolean): boolean {
  if (value == null) return fallback
  const normalized = String(value).trim().toLowerCase()
  if (!normalized) return fallback
  if (normalized === '1' || normalized === 'true' || normalized === 'yes' || normalized === 'on') return true
  if (normalized === '0' || normalized === 'false' || normalized === 'no' || normalized === 'off') return false
  return fallback
}

function normalizeTomorrowReminderSettings(value: any): TomorrowReminderSettings {
  const enabled = value?.calendar_tomorrow_reminder_enabled !== false
  const rawHours = Array.isArray(value?.calendar_tomorrow_reminder_hours)
    ? value.calendar_tomorrow_reminder_hours
    : []
  const normalizedHours = Array.from(
    new Set(
      rawHours
        .map((hour: unknown) => Number(hour))
        .filter((hour) => Number.isInteger(hour) && hour >= 0 && hour <= 23),
    ),
  ).sort((a, b) => a - b)

  const onlyIfEvents = value?.calendar_tomorrow_reminder_only_if_events === true
  const rawMaxItems = Number(value?.calendar_tomorrow_reminder_max_items)
  const maxItems =
    Number.isInteger(rawMaxItems) && rawMaxItems >= 1 && rawMaxItems <= 50
      ? rawMaxItems
      : DEFAULT_TOMORROW_REMINDER_MAX_ITEMS

  return {
    enabled,
    hours: normalizedHours.length > 0 ? normalizedHours : [...DEFAULT_TOMORROW_REMINDER_HOURS],
    onlyIfEvents,
    maxItems,
  }
}

function normalizeNullableMessageCleanupTiming(value: unknown): MessageCleanupTiming | null {
  if (value == null || value === '') return null
  if (value === 'after_each_delivery' || value === 'end_of_day') return value
  return null
}

function normalizeNullableLastDeliverySummaryMode(value: unknown): LastDeliverySummaryMode | null {
  if (value == null || value === '') return null
  if (value === 'independent' || value === 'daily_rollup') return value
  return null
}

function normalizeOptionalRoomName(value: unknown): string | null {
  if (typeof value !== 'string') return null
  const trimmed = value.trim()
  return trimmed.length > 0 ? trimmed : null
}

function getRoomDisplayName(roomId: string, setting: RoomRuntimeSetting | undefined): string {
  return setting?.room_name ?? roomId
}

function groupMessagesByRoom(messages: LineMessageRow[]): Map<string, LineMessageRow[]> {
  const grouped = new Map<string, LineMessageRow[]>()
  for (const message of messages) {
    const list = grouped.get(message.room_id)
    if (list) {
      list.push(message)
    } else {
      grouped.set(message.room_id, [message])
    }
  }
  return grouped
}

function getLastScheduledHour(hours: unknown): number | null {
  if (!Array.isArray(hours) || hours.length === 0) return null
  const valid = hours
    .map((h) => Number(h))
    .filter((h) => Number.isInteger(h) && h >= 0 && h <= 23)
  if (valid.length === 0) return null
  return Math.max(...valid)
}

function getJstDayRange(base: Date): { startIso: string; endIso: string } {
  const offsetMs = 9 * 60 * 60 * 1000
  const jstMs = base.getTime() + offsetMs
  const jstDate = new Date(jstMs)
  const startUtcMs = Date.UTC(
    jstDate.getUTCFullYear(),
    jstDate.getUTCMonth(),
    jstDate.getUTCDate(),
    0,
    0,
    0,
    0,
  ) - offsetMs
  const endUtcMs = startUtcMs + 24 * 60 * 60 * 1000
  return {
    startIso: new Date(startUtcMs).toISOString(),
    endIso: new Date(endUtcMs).toISOString(),
  }
}

async function pruneMessagesByRetentionDays(
  supabase: ReturnType<typeof createClient>,
  retentionDays: MessageRetentionDays,
  now = new Date(),
): Promise<MessageUpdateResult> {
  if (!Number.isFinite(retentionDays) || retentionDays <= 0) {
    return { affectedCount: 0, error: null }
  }
  const cutoffIso = new Date(now.getTime() - retentionDays * 24 * 60 * 60 * 1000).toISOString()
  const { count, error: countError } = await supabase
    .from('line_messages')
    .select('id', { head: true, count: 'exact' })
    .lt('created_at', cutoffIso)
  if (countError) return { affectedCount: 0, error: new Error(countError.message) }
  if (!count || count <= 0) return { affectedCount: 0, error: null }

  const { error: deleteError } = await supabase
    .from('line_messages')
    .delete()
    .lt('created_at', cutoffIso)
  if (deleteError) return { affectedCount: 0, error: new Error(deleteError.message) }
  return { affectedCount: count, error: null }
}

async function markMessagesProcessedByIds(
  supabase: ReturnType<typeof createClient>,
  messageIds: string[],
): Promise<MessageUpdateResult> {
  if (messageIds.length === 0) return { affectedCount: 0, error: null }
  const { error } = await supabase
    .from('line_messages')
    .update({ processed: true })
    .in('id', messageIds)
  if (error) return { affectedCount: 0, error: new Error(error.message) }
  return { affectedCount: messageIds.length, error: null }
}
