/** MARUGO GROUP 公式サイト「運営店舗」掲載ブランド（2026年時点の一覧に準拠） */
export const MARUGO_GROUP_STORE_OPTIONS = [
  "マルゴ",
  "マルゴ セカンド",
  "マルゴ グランデ",
  "サンナナイチ バル",
  "シェンロン&クラウディア",
  "クラウディア2",
  "ソバージュ",
  "バルぺロタ",
  "トラットリア ブリッコラ",
  "ヴィオレット",
  "マルゴ オット",
  "元祖どないや 新宿三丁目店",
  "マルゴ 四谷",
  "鮨こるり",
  "ビストロ サヴァサヴァ",
  "マルゴエス",
  "マルゴ 新橋",
  "マルゴ丸の内",
  "焼肉マルゴ",
  "エリックスバイエリックトロション",
  "ミタン",
  "マルゴ D",
] as const

const LABEL_SET = new Set<string>(MARUGO_GROUP_STORE_OPTIONS as unknown as string[])

export function isMarugoGroupStoreLabel(value: string): boolean {
  return LABEL_SET.has(value)
}
