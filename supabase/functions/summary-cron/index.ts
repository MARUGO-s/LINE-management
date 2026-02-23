import "jsr:@supabase/functions-js/edge-runtime.d.ts"
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2.44.0'
import { OpenAI } from "https://esm.sh/openai@4.52.0"

Deno.serve(async (_req) => {
  try {
    const supabaseUrl = Deno.env.get('SUPABASE_URL') ?? ''
    const supabaseKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') ?? ''
    const supabase = createClient(supabaseUrl, supabaseKey)

    const lineAccessToken = Deno.env.get('LINE_CHANNEL_ACCESS_TOKEN') ?? ''
    const overallRoomId = Deno.env.get('LINE_OVERALL_ROOM_ID') ?? ''
    const openaiApiKey = Deno.env.get('OPENAI_API_KEY') ?? ''

    if (!openaiApiKey) {
      throw new Error("Missing OPENAI_API_KEY environment variable")
    }
    const openai = new OpenAI({ apiKey: openaiApiKey })

    // 1. Fetch unprocessed messages
    const { data: messages, error: fetchError } = await supabase
      .from('line_messages')
      .select('id, room_id, content, created_at')
      .eq('processed', false)
      .order('created_at', { ascending: true })

    if (fetchError) {
      throw new Error(`Error fetching messages: ${fetchError.message}`)
    }

    if (!messages || messages.length === 0) {
      return new Response(JSON.stringify({ message: "No new messages to process." }), {
        status: 200,
        headers: { "Content-Type": "application/json" }
      })
    }

    // 2. Group by room_id
    const messagesByRoom: Record<string, string[]> = {}
    for (const msg of messages) {
      if (!messagesByRoom[msg.room_id]) {
        messagesByRoom[msg.room_id] = []
      }
      messagesByRoom[msg.room_id].push(msg.content)
    }

    const roomSummaries: Record<string, string> = {}

    // 3. Generate summary per room and send back
    for (const [roomId, contents] of Object.entries(messagesByRoom)) {
      const roomText = contents.map((c, i) => `[${i + 1}]: ${c}`).join('\n')

      const response = await openai.chat.completions.create({
        model: "gpt-4o-mini",
        messages: [
          {
            role: "system", content: `あなたは優秀なAIアシスタントです。
以下のLINEのチャット履歴（発言内容）を読み、簡潔で分かりやすく箇条書きなどで要約してください。
挨拶や意味のないメッセージは省き、重要な情報や決定事項、話題の中心となった事柄を抽出してください。` },
          { role: "user", content: `以下のメッセージリストを要約してください:\n\n${roomText}` }
        ]
      })

      const summary = response.choices[0].message?.content || "要約を生成できませんでした。"
      roomSummaries[roomId] = summary

      // Send to the room if access token is available
      if (lineAccessToken) {
        await sendLineMessage(roomId, `【各ルーム 定期要約】\n\n${summary}`, lineAccessToken)
      } else {
        console.warn(`LINE_CHANNEL_ACCESS_TOKEN is missing. Skipping send to room ${roomId}`);
      }
    }

    // 4. Generate overall summary and send to the overall room
    if (overallRoomId && lineAccessToken && Object.keys(roomSummaries).length > 0) {
      // Create overall summary context
      const allSummariesText = Object.entries(roomSummaries)
        .map(([roomId, summary], index) => `■ ルーム${index + 1} (${roomId}):\n${summary}`)
        .join('\n\n')

      const overallResponse = await openai.chat.completions.create({
        model: "gpt-4o-mini",
        messages: [
          {
            role: "system", content: `あなたは優秀な全体の管理者AIです。
複数のLINEルームで行われた会話の「各ルームの要約」を受け取ります。
全てのルームの動きを俯瞰し、全体としてどのような話題が挙がっているかを簡潔に統合したレポート（全体要約）を作成してください。
必要に応じて重要な項目をピックアップし、全体像が把握しやすいように整理してください。` },
          { role: "user", content: `以下の各ルームの要約を統合し、全体レポートを作成してください:\n\n${allSummariesText}` }
        ]
      })

      const overallSummary = overallResponse.choices[0].message?.content || "全体要約を生成できませんでした。"
      await sendLineMessage(overallRoomId, `【全体 定期要約レポート】\n\n${overallSummary}`, lineAccessToken)
    } else if (!overallRoomId) {
      console.warn("LINE_OVERALL_ROOM_ID is not configured. Skipping overall summary send.");
    }

    // 5. Mark messages as processed
    const messageIds = messages.map(m => m.id)
    const { error: updateError } = await supabase
      .from('line_messages')
      .update({ processed: true })
      .in('id', messageIds)

    if (updateError) {
      throw new Error(`Error updating processed status: ${updateError.message}`)
    }

    return new Response(JSON.stringify({
      success: true,
      processedCount: messageIds.length,
      roomsProcessed: Object.keys(roomSummaries).length
    }), {
      headers: { 'Content-Type': 'application/json' },
      status: 200,
    })

  } catch (err: any) {
    console.error('Error processing cron job:', err)
    return new Response(JSON.stringify({ error: err.message || 'Internal Server Error' }), {
      status: 500,
      headers: { "Content-Type": "application/json" }
    })
  }
})

// Helper function to send LINE push message
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
    } else {
      console.log(`Successfully sent message to ${to}`)
    }
  } catch (error) {
    console.error(`Network or fetch error while sending to ${to}:`, error)
  }
}
