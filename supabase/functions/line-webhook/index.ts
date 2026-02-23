import "jsr:@supabase/functions-js/edge-runtime.d.ts"
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2.44.0'

Deno.serve(async (req) => {
  if (req.method !== 'POST') {
    return new Response('Method not allowed', { status: 405 })
  }

  try {
    const rawBody = await req.text()
    const signature = req.headers.get('x-line-signature')
    const lineChannelSecret = Deno.env.get('LINE_CHANNEL_SECRET')

    // LINE Signature Verification (if LINE_CHANNEL_SECRET is provided)
    if (lineChannelSecret && signature) {
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

    for (const event of events) {
      if (event.type === 'message' && event.message.type === 'text') {
        // Determine room/group ID or user ID as fallback
        const source = event.source
        const roomId = source.groupId || source.roomId || source.userId
        const userId = source.userId
        const content = event.message.text

        // Save message to target database
        const { error } = await supabase.from('line_messages').insert({
          room_id: roomId,
          user_id: userId,
          content: content,
          processed: false
        })

        if (error) {
          console.error('Failed to insert message:', error)
        } else {
          console.log(`Saved message from ${roomId}: ${content.substring(0, 30)}...`)
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
