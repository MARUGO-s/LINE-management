import "jsr:@supabase/functions-js/edge-runtime.d.ts"
import postgres from 'https://deno.land/x/postgresjs@v3.4.4/mod.js'

Deno.serve(async (_req) => {
  const dbUrl = Deno.env.get('SUPABASE_DB_URL') ?? ''

  if (!dbUrl) {
    return new Response(JSON.stringify({ error: 'SUPABASE_DB_URL not set' }), { status: 500 })
  }

  const sql = postgres(dbUrl, { ssl: 'require' })
  
  try {
    const verifyResult = await sql`
      SELECT
        CASE
          WHEN to_regprocedure('public.invoke_summary_cron()') IS NULL THEN NULL
          ELSE pg_get_functiondef(to_regprocedure('public.invoke_summary_cron()'))
        END AS summary_func_def,
        CASE
          WHEN to_regprocedure('public.invoke_gmail_alert_cron()') IS NULL THEN NULL
          ELSE pg_get_functiondef(to_regprocedure('public.invoke_gmail_alert_cron()'))
        END AS gmail_func_def
    `

    const cronStatus = await sql`
      SELECT jobid, jobname, schedule, active
      FROM cron.job
      ORDER BY jobid
    `

    const settingStatus = await sql`
      SELECT jsonb_build_object(
        'edge_function_url', current_setting('custom.edge_function_url', true),
        'cron_auth_token_set', CASE
          WHEN current_setting('custom.cron_auth_token', true) IS NOT NULL
           AND current_setting('custom.cron_auth_token', true) != ''
          THEN true ELSE false END,
        'legacy_service_role_key_set', CASE
          WHEN current_setting('custom.service_role_key', true) IS NOT NULL
           AND current_setting('custom.service_role_key', true) != ''
           AND current_setting('custom.service_role_key', true) != 'YOUR_SERVICE_ROLE_KEY'
          THEN true ELSE false END,
        'current_time_utc', now()
      ) as settings
    `

    const summarySettings = await sql`
      SELECT id, delivery_hours, is_enabled, updated_at
      FROM public.summary_settings
      WHERE id = 1
    `

    await sql.end()

    return new Response(JSON.stringify({
      success: true,
      message: 'Diagnostics fetched. No schema or function changes were applied.',
      settings: settingStatus[0]?.settings ?? {},
      summarySettings: summarySettings[0] ?? null,
      cronJobs: cronStatus,
      funcDefPreviewSummary: verifyResult[0]?.summary_func_def
        ? verifyResult[0].summary_func_def.substring(0, 300) + '...'
        : null,
      funcDefPreviewGmail: verifyResult[0]?.gmail_func_def
        ? verifyResult[0].gmail_func_def.substring(0, 300) + '...'
        : null,
    }, null, 2), {
      headers: { 'Content-Type': 'application/json' },
      status: 200,
    })
  } catch (e: unknown) {
    await sql.end()
    return new Response(JSON.stringify({ error: String(e) }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    })
  }
})
