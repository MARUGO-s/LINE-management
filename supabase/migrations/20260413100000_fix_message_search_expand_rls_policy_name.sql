-- PostgreSQL は識別子 63 バイトで切り捨てるため、長いポリシー名は意図せず短縮されていた。
-- 短い固定名に置き換え、DROP/CREATE を明確にする。

DROP POLICY IF EXISTS "sr_all_msg_search_expand_pending"
    ON public.message_search_expand_pending_confirmations;

DROP POLICY IF EXISTS "Service role can do everything on message_search_expand_pending"
    ON public.message_search_expand_pending_confirmations;

DROP POLICY IF EXISTS "Service role can do everything on message_search_expand_pending_confirmations"
    ON public.message_search_expand_pending_confirmations;

CREATE POLICY "sr_all_msg_search_expand_pending"
    ON public.message_search_expand_pending_confirmations
    FOR ALL
    USING (auth.jwt() ->> 'role' = 'service_role');
