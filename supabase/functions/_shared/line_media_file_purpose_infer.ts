/**
 * ファイル名・抽出テキスト先頭から業務カテゴリの短文ラベルを推定する（管理画面 media.html の inferFilePurpose と同じルール）。
 * LINE 返信では raw 抽出テキストではなくこの短文のみを出すために使う。
 * ルールを変える場合は media.html 側と揃えること。
 */
export function inferLineMediaFilePurposeLabel(
  fileName: string,
  mimeType: string,
  contentPreview: string,
): string {
  const text = (`${String(fileName || '')} ${String(contentPreview || '')}`).toLowerCase()
  if (!text.trim()) return ''

  const hasHaccpTopic = /haccp|hasap|ハサップ|衛生|点検/.test(text)
  const hasHaccpSchedule = /スケジュール|予定表|予定|実施日|点検実施日|日程|schedule/.test(text)
  if (hasHaccpTopic && hasHaccpSchedule) {
    return 'HACCP日程表（衛生点検スケジュール）資料の可能性が高いです。'
  }
  if (hasHaccpTopic) {
    return 'HACCP関連資料（衛生点検・衛生管理）の可能性が高いです。'
  }

  const categories: { key: string; label: string; keywords: string[] }[] = [
    {
      key: 'labor',
      label: 'アルバイト・労働条件・労務関連の資料',
      keywords: [
        '労働', '労務', '雇用', '就業', '労働条件', '労働相談', 'アルバイト', 'パート', '求人', '採用', '時給', '賃金',
        '最低賃金', '残業', '有給', '社会保険', '労働基準', '労基', 'labour', 'labor standards',
      ],
    },
    {
      key: 'shift',
      label: 'シフト・勤怠管理資料',
      keywords: ['シフト', '勤怠', '出勤', '退勤', '勤務', '勤務表', 'シフト表', '打刻', '勤怠表', 'タイムカード', '出退勤'],
    },
    {
      key: 'cost_inventory',
      label: '原価・棚卸管理資料',
      keywords: ['原価', '棚卸', '在庫', 'stock', '仕入', '単価', 'ロス率', '原価率', '在庫数', '棚卸表'],
    },
    {
      key: 'recipe',
      label: 'レシピ・仕込み資料',
      keywords: ['レシピ', '配合', '仕込み', '分量', '歩留', '手順', '材料', '調理', 'g', 'ml'],
    },
    {
      key: 'billing',
      label: '請求・支払管理資料',
      keywords: ['請求', 'invoice', '支払', '入金', '振込', '締日', '請求先', '支払期日', '買掛', '売掛'],
    },
    {
      key: 'sales',
      label: '売上・予実管理資料',
      keywords: ['売上', 'sales', '予算', '実績', '前年差', '目標', '客単価', '来客数', '日次売上', '月次売上'],
    },
    {
      key: 'procurement',
      label: '発注・納品管理資料',
      keywords: ['発注', '納品', '納品書', '発注書', '納入', '納品業者', '仕入先', '数量', 'リードタイム'],
    },
    {
      key: 'haccp',
      label: '衛生・HACCP関連資料',
      keywords: ['衛生', 'haccp', '温度管理', '消毒', '清掃', '点検表', '衛生管理', '冷蔵温度', '賞味期限', '消費期限'],
    },
    {
      key: 'manual',
      label: 'マニュアル・業務手順資料',
      keywords: ['マニュアル', '手順書', '運用手順', '業務フロー', 'オペレーション', '研修', '教育', 'ルール'],
    },
  ]

  const scoreCategory = (category: (typeof categories)[number]) =>
    category.keywords.reduce((total, keyword) => total + (text.includes(keyword) ? 1 : 0), 0)

  let best: { key: string; label: string; score: number } | null = null
  for (const category of categories) {
    const score = scoreCategory(category)
    if (score <= 0) continue
    if (!best || score > best.score) {
      best = { key: category.key, label: category.label, score }
    }
  }

  if (best && best.score >= 2) {
    return `${best.label}の可能性が高いです。`
  }
  if (best && best.score === 1 && String(mimeType || '').includes('spreadsheetml')) {
    return `${best.label}の可能性があります。`
  }
  if (String(mimeType || '').includes('spreadsheetml')) {
    return 'Excel台帳系ファイルの可能性があります（項目ベースで管理する資料）。'
  }
  return '業務資料ファイル（飲食店運用資料）の可能性があります。'
}
