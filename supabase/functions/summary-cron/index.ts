import "jsr:@supabase/functions-js/edge-runtime.d.ts"
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2.44.0'
import { Groq } from "https://esm.sh/groq-sdk@0.5.0"

type MessageCleanupTiming = 'after_each_delivery' | 'end_of_day'
type LastDeliverySummaryMode = 'independent' | 'daily_rollup'
type MessageUpdateResult = { affectedCount: number; error: Error | null }
type LineMessageRow = { id: string; room_id: string; content: string; created_at: string }
type RoomRuntimeSetting = {
  room_name: string | null
  delivery_hours: number[] | null
  is_enabled: boolean
  send_room_summary: boolean
  calendar_tomorrow_reminder_enabled: boolean
  message_cleanup_timing: MessageCleanupTiming | null
  last_delivery_summary_mode: LastDeliverySummaryMode | null
}
type RoomExecutionContext = {
  messageIds: string[]
  cleanupTiming: MessageCleanupTiming
  summaryMode: LastDeliverySummaryMode
  isDailyRollup: boolean
  isEndOfDayCleanup: boolean
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

const JST_OFFSET_MS = 9 * 60 * 60 * 1000
const CALENDAR_SCOPE = 'https://www.googleapis.com/auth/calendar'
const DEFAULT_TOMORROW_REMINDER_HOURS = [19]
const DEFAULT_TOMORROW_REMINDER_MAX_ITEMS = 20

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

    if (!groqApiKey) {
      throw new Error("Missing GROQ_API_KEY environment variable")
    }
    const groq = new Groq({ apiKey: groqApiKey })

    // 0. Check current time and schedules
    // JST is UTC+9
    const now = new Date()
    jstHour = (now.getUTCHours() + 9) % 24
    const forceRun = isForceRun(req)
    console.log(`Current JST hour: ${jstHour}`)

    // Fetch global settings
    const { data: globalSettings, error: globalSettingsError } = await supabase
      .from('summary_settings')
      .select('delivery_hours, is_enabled, message_cleanup_timing, last_delivery_summary_mode, calendar_tomorrow_reminder_enabled, calendar_tomorrow_reminder_hours, calendar_tomorrow_reminder_only_if_events, calendar_tomorrow_reminder_max_items')
      .eq('id', 1)
      .single();

    if (globalSettingsError) {
      console.error(`Error fetching global settings: ${globalSettingsError.message}`)
    }

    const globalEnabled = globalSettings?.is_enabled ?? true
    const globalHours = globalSettings?.delivery_hours ?? [12, 17, 23]
    const messageCleanupTiming = normalizeMessageCleanupTiming(globalSettings?.message_cleanup_timing)
    const lastDeliverySummaryMode = normalizeLastDeliverySummaryMode(globalSettings?.last_delivery_summary_mode)
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

    // Fetch per-room settings
    const { data: roomSettingsList, error: roomSettingsError } = await supabase
      .from('room_summary_settings')
      .select('room_id, room_name, delivery_hours, is_enabled, send_room_summary, calendar_tomorrow_reminder_enabled, message_cleanup_timing, last_delivery_summary_mode')

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
      let endOfDayPrunedCount = 0
      if (messageCleanupTiming === 'end_of_day' && shouldSendOverall && isLastGlobalDeliverySlot) {
        const pruneResult = await pruneProcessedMessagesForDay(supabase, jstDayRange.startIso, jstDayRange.endIso)
        if (pruneResult.error) {
          throw new Error(`Error pruning processed messages: ${pruneResult.error.message}`)
        }
        endOfDayPrunedCount = pruneResult.affectedCount
      }

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
          last_delivery_summary_mode: lastDeliverySummaryMode,
          using_daily_rollup_scope: shouldUseOverallDailyRollup,
          end_of_day_pruned_count: endOfDayPrunedCount,
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
        cleanupTiming: effectiveCleanupTiming,
        summaryMode: effectiveSummaryMode,
        isDailyRollup: shouldUseRoomDailyRollup,
        isEndOfDayCleanup: effectiveCleanupTiming === 'end_of_day' && isRoomLastSlot,
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

    const deleteMessageIds = new Set<string>()
    const markProcessedMessageIds = new Set<string>()
    const endOfDayDeleteRoomIds = new Set<string>()

    for (const roomId of deliveredRoomIds) {
      const context = roomContexts.get(roomId)
      if (!context) continue

      if (context.cleanupTiming === 'after_each_delivery') {
        for (const id of context.messageIds) deleteMessageIds.add(id)
        continue
      }

      if (context.isEndOfDayCleanup) {
        endOfDayDeleteRoomIds.add(roomId)
      } else {
        for (const id of context.messageIds) markProcessedMessageIds.add(id)
      }
    }

    let affectedCount = 0
    let deletedCount = 0
    let markedProcessedCount = 0
    let endOfDayDeletedCount = 0
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
            delete_message_ids: deleteMessageIds.size,
            mark_processed_message_ids: markProcessedMessageIds.size,
            end_of_day_room_ids: endOfDayDeleteRoomIds.size,
          },
        },
      })
    }

    if (deleteMessageIds.size > 0) {
      const result = await deleteMessagesByIds(supabase, Array.from(deleteMessageIds))
      if (result.error) {
        await writeDbUpdateFailure(result.error.message)
        throw new Error(`Error deleting delivered messages: ${result.error.message}`)
      }
      deletedCount = result.affectedCount
      affectedCount += result.affectedCount
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

    if (endOfDayDeleteRoomIds.size > 0) {
      const result = await deleteMessagesForDayByRooms(
        supabase,
        Array.from(endOfDayDeleteRoomIds),
        jstDayRange.startIso,
        jstDayRange.endIso,
      )
      if (result.error) {
        await writeDbUpdateFailure(result.error.message)
        throw new Error(`Error deleting end-of-day messages: ${result.error.message}`)
      }
      endOfDayDeletedCount = result.affectedCount
      affectedCount += result.affectedCount
    }

    const actionKinds = [
      deleteMessageIds.size > 0 ? 'deleted' : null,
      markProcessedMessageIds.size > 0 ? 'marked_processed' : null,
      endOfDayDeleteRoomIds.size > 0 ? 'end_of_day_deleted' : null,
    ].filter((value): value is 'deleted' | 'marked_processed' | 'end_of_day_deleted' => value != null)

    if (actionKinds.length === 0) {
      cleanupAction = 'none'
    } else if (actionKinds.length === 1) {
      cleanupAction = actionKinds[0]
    } else {
      cleanupAction = 'mixed'
    }

    const hasRoomFailures = failedRoomDeliveries.length > 0
    const successReason = hasRoomFailures
      ? 'Overall delivery succeeded, but some room-summary deliveries failed.'
      : cleanupAction === 'deleted'
        ? 'Scheduled deliveries succeeded and delivered messages were deleted.'
        : cleanupAction === 'marked_processed'
          ? 'Scheduled deliveries succeeded and delivered messages were marked as processed.'
          : cleanupAction === 'end_of_day_deleted'
            ? 'Scheduled deliveries succeeded and day-end messages were deleted.'
            : cleanupAction === 'mixed'
              ? 'Scheduled deliveries succeeded and room-specific cleanup actions were applied.'
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
        cleanup_deleted_count: deletedCount,
        cleanup_marked_processed_count: markedProcessedCount,
        cleanup_end_of_day_deleted_count: endOfDayDeletedCount,
        message_cleanup_timing: messageCleanupTiming,
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
  if (!shouldPersistDeliveryLog(payload.status)) {
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

function shouldPersistDeliveryLog(status: string): boolean {
  const normalized = String(status || '').trim().toLowerCase()
  if (!normalized) return true
  return !nonActionableLogStatuses.has(normalized)
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

async function deleteMessagesByIds(
  supabase: ReturnType<typeof createClient>,
  messageIds: string[],
): Promise<MessageUpdateResult> {
  if (messageIds.length === 0) return { affectedCount: 0, error: null }
  const { error } = await supabase
    .from('line_messages')
    .delete()
    .in('id', messageIds)
  if (error) return { affectedCount: 0, error: new Error(error.message) }
  return { affectedCount: messageIds.length, error: null }
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

async function deleteMessagesForDayByRooms(
  supabase: ReturnType<typeof createClient>,
  roomIds: string[],
  dayStartIso: string,
  dayEndIso: string,
): Promise<MessageUpdateResult> {
  if (roomIds.length === 0) return { affectedCount: 0, error: null }

  const { count, error: countError } = await supabase
    .from('line_messages')
    .select('id', { head: true, count: 'exact' })
    .in('room_id', roomIds)
    .gte('created_at', dayStartIso)
    .lt('created_at', dayEndIso)
  if (countError) return { affectedCount: 0, error: new Error(countError.message) }
  if (!count || count <= 0) return { affectedCount: 0, error: null }

  const { error: deleteError } = await supabase
    .from('line_messages')
    .delete()
    .in('room_id', roomIds)
    .gte('created_at', dayStartIso)
    .lt('created_at', dayEndIso)
  if (deleteError) return { affectedCount: 0, error: new Error(deleteError.message) }
  return { affectedCount: count, error: null }
}

async function pruneProcessedMessagesForDay(
  supabase: ReturnType<typeof createClient>,
  dayStartIso: string,
  dayEndIso: string,
): Promise<MessageUpdateResult> {
  const { count, error: countError } = await supabase
    .from('line_messages')
    .select('id', { head: true, count: 'exact' })
    .eq('processed', true)
    .gte('created_at', dayStartIso)
    .lt('created_at', dayEndIso)
  if (countError) return { affectedCount: 0, error: new Error(countError.message) }
  if (!count || count <= 0) return { affectedCount: 0, error: null }

  const { error: deleteError } = await supabase
    .from('line_messages')
    .delete()
    .eq('processed', true)
    .gte('created_at', dayStartIso)
    .lt('created_at', dayEndIso)
  if (deleteError) return { affectedCount: 0, error: new Error(deleteError.message) }
  return { affectedCount: count, error: null }
}
