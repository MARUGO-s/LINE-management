-- 役職ラベル「社長」→「代表取締役」への改名に伴う既存データの更新

update public.line_user_permissions
set assigned_job_title = '代表取締役'
where assigned_job_title = '社長';
