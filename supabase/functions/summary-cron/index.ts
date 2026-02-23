import "jsr:@supabase/functions-js/edge-runtime.d.ts"
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2.44.0'
import { Groq } from "https://esm.sh/groq-sdk@0.5.0"

Deno.serve(async (_req) => {
  try {
    const supabaseUrl = Deno.env.get('SUPABASE_URL') ?? ''
    const supabaseKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') ?? ''
    const supabase = createClient(supabaseUrl, supabaseKey)

    const lineAccessToken = Deno.env.get('LINE_CHANNEL_ACCESS_TOKEN') ?? ''
    const overallRoomId = Deno.env.get('LINE_OVERALL_ROOM_ID') ?? ''
    const groqApiKey = Deno.env.get('GROQ_API_KEY') ?? ''

    if (!groqApiKey) {
      throw new Error("Missing GROQ_API_KEY environment variable")
    }
    const groq = new Groq({ apiKey: groqApiKey })

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

      const response = await groq.chat.completions.create({
        model: "llama-3.3-70b-versatile", // Validated Groq model
        messages: [
          {
            role: "system", content: `あなたは優秀なAIアシスタントです。
以下のLINEのチャット履歴（発言内容）を読み、簡潔で分かりやすく要約してください。
【重要】LINEでは「特定の過去の発言に対するリプライ（引用返信）」が使われるため、文脈が唐突に前後に飛ぶことがあります。会話のつながりを慎重に推測し、「誰がどの話題に対して返答や賛同をしているのか」を正確に汲み取って話の流れを整理してください。
挨拶や意味のないメッセージは省きつつも、その1日の「話の流れ」「決定事項」「重要な情報」が、この要約だけを読めば漏れなく完全に把握できるように、詳細かつ具体的に記述してください。
また画像、動画、PDFなどのファイル、またはURLリンクが共有されていた場合は、要約を読んだ人が元のトークルームへ確認に行けるように「添付資料あり」「画像共有あり」「リンク共有あり」等の注意喚起を必ず含めてください。
添付資料の確認が不要な内容については、要約を読むだけで事足りるレベルで網羅的に情報を抽出してください。
必ず日本語で返信してください。` },
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

      const overallResponse = await groq.chat.completions.create({
        model: "llama-3.3-70b-versatile", // Validated Groq model
        messages: [
          {
            role: "system", content: `あなたは優秀な全体の管理者AIです。
複数のLINEルームで行われた会話の「各ルームの要約」を受け取ります。
全てのルームの動きを俯瞰し、全体としてどのような話題が挙がっているかを統合したレポート（全体要約）を作成してください。
このレポートだけを読めば、画像やPDF等を見に行かなくとも「各ルームでその日どんな話があり、何が決まったか」が漏れなく完全に把握できるように、詳細に記述してください。
なお、各ルームの要約に「添付資料あり」「画像共有あり」「リンクあり」などの報告が含まれている場合は、全体要約でもそれが一目でわかるように（誰かが確認に行くべき情報として）特記してください。
【重要】出力時のフォーマット指定：
「■ ルーム1」などの各ルームの要約ブロックを出力する際は、前のルームとの間に「必ず1行の空白（空行）」を挿入して、視覚的に読みやすく区切ってください。
必ず日本語で返信してください。` },
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
