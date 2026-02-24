import "jsr:@supabase/functions-js/edge-runtime.d.ts"
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2.44.0'
import { Groq } from "https://esm.sh/groq-sdk@0.5.0"

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
      .select('delivery_hours, is_enabled')
      .eq('id', 1)
      .single();

    if (globalSettingsError) {
      console.error(`Error fetching global settings: ${globalSettingsError.message}`)
    }

    const globalEnabled = globalSettings?.is_enabled ?? true
    const globalHours = globalSettings?.delivery_hours ?? [12, 17, 23]

    // Fetch per-room settings
    const { data: roomSettingsList, error: roomSettingsError } = await supabase
      .from('room_summary_settings')
      .select('room_id, delivery_hours, is_enabled, send_room_summary')

    if (roomSettingsError) {
      console.error(`Error fetching room settings: ${roomSettingsError.message}`)
    }

    const roomSettingsMap = new Map<string, { delivery_hours: number[] | null, is_enabled: boolean, send_room_summary: boolean }>()
    if (roomSettingsList) {
      for (const rs of roomSettingsList) {
        roomSettingsMap.set(rs.room_id, {
          delivery_hours: rs.delivery_hours,
          is_enabled: rs.is_enabled,
          send_room_summary: rs.send_room_summary === true,
        })
      }
    }

    // 1. Fetch unprocessed messages
    const { data: messages, error: fetchError } = await supabase
      .from('line_messages')
      .select('id, room_id, content, created_at')
      .eq('processed', false)
      .order('created_at', { ascending: true })

    if (fetchError) {
      throw new Error(`Error fetching messages: ${fetchError.message}`)
    }

    const queueCount = messages?.length ?? 0
    const shouldSendOverall = globalEnabled && (forceRun || globalHours.includes(jstHour))

    if (!messages || messages.length === 0) {
      await writeDeliveryLog(supabase, {
        jst_hour: jstHour,
        status: 'no_messages',
        reason: 'No unprocessed messages.',
        should_send_overall: shouldSendOverall,
        rooms_targeted: 0,
        messages_in_queue: 0,
        messages_marked_processed: 0,
        line_send_attempted: false,
        line_send_success: false,
        target_room_id: overallRoomId || null,
      })

      return new Response(JSON.stringify({ message: "No new messages to process." }), {
        status: 200,
        headers: { "Content-Type": "application/json" }
      })
    }

    // 2. Group by room_id and decide delivery targets
    const messagesByRoom: Record<string, string[]> = {}
    const roomsToSummarize: string[] = []
    const roomDeliveryTargets: string[] = []

    for (const msg of messages) {
      if (!messagesByRoom[msg.room_id]) {
        messagesByRoom[msg.room_id] = []

        const rs = roomSettingsMap.get(msg.room_id)
        const roomEnabled = rs ? rs.is_enabled : true
        const roomHours = rs?.delivery_hours ?? globalHours

        if (roomEnabled && (forceRun || roomHours.includes(jstHour))) {
          roomsToSummarize.push(msg.room_id)
          if (rs?.send_room_summary === true) {
            roomDeliveryTargets.push(msg.room_id)
          }
        }
      }
      messagesByRoom[msg.room_id].push(msg.content)
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
          },
        })

        return new Response(JSON.stringify({ message: "No summaries available for overall delivery." }), {
          status: 200,
          headers: { "Content-Type": "application/json" }
        })
      }

      const allSummariesText = summariesForOverall
        .map((roomId, index) => `■ ルーム${index + 1} (${roomId}):\n${roomSummaries[roomId]}`)
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
「■ ルーム1」などの各ルームの要約ブロックを出力する際は、前のルームとの間に「必ず1行の空白（空行）」を挿入して、視覚的に読みやすく区切ってください。
必ず日本語で返信してください。` },
          { role: "user", content: `以下の各ルームの要約を統合し、全体レポートを作成してください:\n\n${allSummariesText}` }
        ]
      })

      const overallSummary = overallResponse.choices[0].message?.content || "全体要約を生成できませんでした。"
      const overallSendResult = await sendLineMessage(
        overallRoomId,
        `【全体 定期要約レポート】\n\n${overallSummary}`,
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

    // 6. Delete delivered messages from line_messages
    const messagesToDelete = messages.filter((m) => deliveredRoomIds.includes(m.room_id))
    if (messagesToDelete.length > 0) {
      const messageIds = messagesToDelete.map((m) => m.id)
      const { error: deleteError } = await supabase
        .from('line_messages')
        .delete()
        .in('id', messageIds)

      if (deleteError) {
        await writeDeliveryLog(supabase, {
          jst_hour: jstHour,
          status: 'db_update_failed',
          reason: deleteError.message,
          should_send_overall: shouldSendOverall,
          rooms_targeted: roomsToSummarize.length,
          messages_in_queue: queueCount,
          messages_marked_processed: 0,
          line_send_attempted: lineSendAttempted,
          line_send_success: lineSendSuccess,
          line_http_status: lineHttpStatus,
          target_room_id: (shouldSendOverall ? overallRoomId : roomDeliveryTargets[0]) || null,
          details: {
            room_ids: roomsToSummarize,
            room_delivery_targets: roomDeliveryTargets,
            room_delivery_success: successfulRoomDeliveries,
            room_delivery_failed: failedRoomDeliveries,
            force_run: forceRun,
          },
        })
        throw new Error(`Error deleting delivered messages: ${deleteError.message}`)
      }

      const hasRoomFailures = failedRoomDeliveries.length > 0
      await writeDeliveryLog(supabase, {
        jst_hour: jstHour,
        status: hasRoomFailures ? 'delivered_with_room_failures' : 'delivered',
        reason: hasRoomFailures
          ? 'Overall delivery succeeded, but some room-summary deliveries failed.'
          : 'Scheduled deliveries succeeded and delivered messages were deleted.',
        should_send_overall: shouldSendOverall,
        rooms_targeted: roomsToSummarize.length,
        messages_in_queue: queueCount,
        messages_marked_processed: messageIds.length,
        line_send_attempted: lineSendAttempted,
        line_send_success: lineSendSuccess,
        line_http_status: lineHttpStatus,
        target_room_id: (shouldSendOverall ? overallRoomId : roomDeliveryTargets[0]) || null,
        details: {
          room_ids: roomsToSummarize,
          room_delivery_targets: roomDeliveryTargets,
          room_delivery_success: successfulRoomDeliveries,
          room_delivery_failed: failedRoomDeliveries,
          force_run: forceRun,
        },
      })

      return new Response(JSON.stringify({
        success: true,
        delivered: true,
        deletedCount: messageIds.length,
        roomsProcessed: deliveredRoomIds.length,
        overallDelivered: shouldSendOverall,
        roomDelivery: {
          targeted: roomDeliveryTargets.length,
          success: successfulRoomDeliveries.length,
          failed: failedRoomDeliveries.length,
        },
      }), {
        headers: { 'Content-Type': 'application/json' },
        status: 200,
      })
    }

    const hasRoomFailures = failedRoomDeliveries.length > 0
    await writeDeliveryLog(supabase, {
      jst_hour: jstHour,
      status: hasRoomFailures ? 'delivered_with_room_failures' : 'delivered_no_messages_to_mark',
      reason: hasRoomFailures
        ? 'Deliveries attempted, but there are room-summary failures and no rows to mark.'
        : 'Deliveries succeeded but there were no message rows to mark.',
      should_send_overall: shouldSendOverall,
      rooms_targeted: roomsToSummarize.length,
      messages_in_queue: queueCount,
      messages_marked_processed: 0,
      line_send_attempted: lineSendAttempted,
      line_send_success: lineSendSuccess,
      line_http_status: lineHttpStatus,
      target_room_id: (shouldSendOverall ? overallRoomId : roomDeliveryTargets[0]) || null,
      details: {
        room_ids: roomsToSummarize,
        room_delivery_targets: roomDeliveryTargets,
        room_delivery_success: successfulRoomDeliveries,
        room_delivery_failed: failedRoomDeliveries,
        force_run: forceRun,
      },
    })

    return new Response(JSON.stringify({ message: "Processed (nothing to mark)." }), {
      status: 200,
      headers: { "Content-Type": "application/json" }
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
