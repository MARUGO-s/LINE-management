/** ユーザー権限の役職ドロップダウン（運用定義） */
export const JOB_TITLE_OPTIONS = [
  "代表取締役",
  "取締役",
  "総務",
  "マネージャー",
  "アシスタントマネージャー",
  "店長",
  "店長補佐",
  "料理長",
  "一般社員",
] as const

const LABEL_SET = new Set<string>(JOB_TITLE_OPTIONS as unknown as string[])

export function isJobTitleLabel(value: string): boolean {
  return LABEL_SET.has(value)
}
