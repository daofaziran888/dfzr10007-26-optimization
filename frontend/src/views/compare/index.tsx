import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/compare'
const columns = [
  "方案编号",
  "对比指标",
  "基准周期",
  "对比周期",
  "增长阈值",
  "告警配置",
  "对比维度",
  "方案状态"
]
const actions = [
  "启用方案",
  "暂停方案",
  "关闭方案"
]
const statuses = [
  "草稿",
  "已启用",
  "已暂停",
  "已关闭"
]
const stats = [
  {
    "label": "对比方案总数",
    "value": 0
  },
  {
    "label": "已启用方案",
    "value": 0
  },
  {
    "label": "已暂停方案",
    "value": 0
  }
]

type Row = Record<string, string | number | null>

export default function CompareView() {
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [errorMessage, setErrorMessage] = useState('')
  const [infoMessage, setInfoMessage] = useState('')
  const [filters, setFilters] = useState<Record<string, string>>({})
  const filterFields = useMemo(() => columns.slice(0, 3), [])

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams(filters).toString()
    try {
      const response = await request(`${ENDPOINT}?${query}`)
      if (!response.ok) {
        throw new Error('对比方案列表读取失败')
      }
      const payload = await response.json()
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '同环比分析列表读取失败')
    }
  }, [filters])

  useEffect(() => {
    void reload()
  }, [reload])

  async function runAction(action: string, row: Row) {
    setErrorMessage('')
    setInfoMessage('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}/actions`, {
        method: 'POST',
        body: JSON.stringify({ values: { action } }),
      })
      if (!response.ok) {
        throw new Error('同环比分析动作未生效，请稍后重试')
      }
      const payload = await response.json()
      if (payload.ok === false) {
        setErrorMessage(payload.message ?? '动作被业务规则拒绝')
        return
      }
      setInfoMessage(payload.message ?? '动作已生效')
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '同环比分析操作失败')
    }
  }

  // 增量批次：只处理新进入窗口的条目；重复批次号由后端台账幂等拦截
  async function ingestBatch() {
    const batchId = window.prompt('请输入增量批次号（重复批次不会重复累计）')
    if (!batchId) {
      return
    }
    const metric = window.prompt('请输入对比指标（如 COMP-0002 的「订单GMV」）')
    if (!metric) {
      return
    }
    const period = window.prompt('请输入周期（日 2026-09-15 / 月 2026-09 / 季 2026Q3 / 年 2026）')
    if (!period) {
      return
    }
    const amountText = window.prompt('请输入本批次增量数值')
    const amount = Number(amountText)
    if (amountText === null || Number.isNaN(amount)) {
      setErrorMessage('增量数值必须是数字')
      return
    }
    setErrorMessage('')
    setInfoMessage('')
    try {
      const response = await request(`${ENDPOINT}/ingest`, {
        method: 'POST',
        body: JSON.stringify({
          values: { batch_id: batchId, entries: [{ metric, period, amount }] },
        }),
      })
      const payload = await response.json()
      if (!response.ok) {
        setErrorMessage(payload.detail ?? '增量批次处理失败')
        return
      }
      const computed = payload.computed?.length ?? 0
      setInfoMessage(
        payload.duplicate
          ? `批次 ${batchId} 为重复批次，未重复累计`
          : `批次 ${batchId} 已入账，新增同环比结果 ${computed} 条`
      )
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '增量批次处理失败')
    }
  }

  // 阈值变更：只生成新版本，历史结果按当时阈值保留，越界会被后端拒绝
  async function changeThreshold(row: Row) {
    const next = window.prompt(`修改「${row.方案编号}」的增长阈值（如 12% 或 0.12，范围 (0%,100%]）`, String(row.增长阈值 ?? ''))
    if (next === null) {
      return
    }
    setErrorMessage('')
    setInfoMessage('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}/threshold`, {
        method: 'PUT',
        body: JSON.stringify({ 增长阈值: next.trim() }),
      })
      const payload = await response.json()
      if (!response.ok || payload.ok === false) {
        setErrorMessage(payload.message ?? payload.detail ?? '增长阈值修改失败')
        return
      }
      setInfoMessage(payload.message ?? '增长阈值已更新')
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '增长阈值修改失败')
    }
  }

  return (
    <section className="page" data-module="compare">
      <header className="page-head">
        <div>
          <h2>同环比分析管理</h2>
          <p className="page-desc">按对比指标与基准周期维护增量水位与阈值版本，增量批次只处理新进入窗口，历史结论按当时阈值保留。</p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => void ingestBatch()}>
            推送增量批次
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出同环比分析清单
          </button>
        </div>
      </header>

      <div className="stat-row">
        {stats.map((item) => (
          <article key={item.label} className="stat-card">
            <span className="stat-label">{item.label}</span>
            <strong className="stat-value">{item.value}</strong>
          </article>
        ))}
      </div>

      <form className="filter-bar" onSubmit={(event) => { event.preventDefault(); void reload() }}>
        {filterFields.map((field) => (
          <label key={field} className="filter-item">
            <span>{field}</span>
            <input
              value={filters[field] ?? ''}
              placeholder={`按${field}检索`}
              onChange={(event) => setFilters((prev) => ({ ...prev, [field]: event.target.value }))}
            />
          </label>
        ))}
        <button className="btn" type="submit">查询</button>
        <button className="btn ghost" type="button" onClick={() => setFilters({})}>重置条件</button>
      </form>

      {infoMessage && <p className="info-text">{infoMessage}</p>}

      <table className="data-table">
        <thead>
          <tr>
            {columns.map((column) => <th key={column}>{column}</th>)}
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={String(row.id)}>
              {columns.map((column) => <td key={column}>{row[column] ?? '—'}</td>)}
              <td className="row-actions">
                {actions.map((action) => (
                  <button key={action} className="link" type="button" onClick={() => void runAction(action, row)}>
                    {action}
                  </button>
                ))}
                <button className="link" type="button" onClick={() => void changeThreshold(row)}>
                  调整增长阈值
                </button>
              </td>
            </tr>
          ))}
          {rows.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={columns.length + 1}>暂无同环比分析数据，可先登记对比方案</td>
            </tr>
          )}
        </tbody>
      </table>

      <footer className="page-foot">
        <span>共 {total} 条同环比分析记录</span>
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}
