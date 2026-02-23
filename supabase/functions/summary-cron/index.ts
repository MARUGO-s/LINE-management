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
以下のLINEのチャット履歴（発言内容）を読み、極めて簡潔で読みやすく要約してください。
【重要1】LINEでは「特定の過去の発言に対するリプライ（引用返信）」が使われるため、文脈が唐突に前後に飛ぶことがあります。会話のつながりを慎重に推測し、「誰がどの話題に対して返答や賛同をしているのか」を正確に汲み取って話の流れを整理してください。
【重要2】メッセージ内に「@〇〇」といったメンション（特定の相手への呼びかけや指定）が含まれている場合、それは発言者が特定の人物に何かを要求・連絡している重要なサインです。要約においても「誰宛てのメッセージ（指示・連絡等）か」が明確に伝わるように、メンションされた対象者を必ず拾い上げて記述してください。
挨拶や意味のない雑談は思い切って削ぎ落とし、その1日の「決定事項」「重要な情報」「話の核心となる流れ」のみを抽出して、できるだけ短い文章でシャープに要約してください。ただし、短くまとめつつも「誰が見ても内容が正確に伝わる」ように表現を工夫してください。
また画像、動画、PDFなどのファイル、またはURLリンクが共有されていた場合は、要約を読んだ人が元のトークルームへ確認に行けるように「添付資料あり」「画像共有あり」「リンク共有あり」等の注意喚起を必ず含めてください。
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
このレポートは、忙しい管理者がサッと目を通せるように、無駄な言葉を省いて極めて簡潔・シャープに記述してください。ただし、短くしすぎて意味が通じなくなることがないよう「各ルームでその日どんな話があり、何が決まったか」の核心部分はしっかりと伝わる表現に工夫してください。
また、各ルームの要約内に「@〇〇さん宛ての指示・連絡」のような特定の人物への名指し（メンション）が含まれていた場合は、それが誰宛てのものかが全体要約でも失われないように確実に記述してください。
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
