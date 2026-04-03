import { useState, useCallback, useMemo, useRef } from 'react'
import yaml from 'js-yaml'
import Navbar from '@/components/layout/Navbar'

// ─── Types ────────────────────────────────────────────────────────────────

interface KvPair {
  id: number
  key: string
  value: string
}

interface SourceData {
  id: number
  type: string
  path: string
  version: string
  refresh: string
  sourceId: string
  name: string
  authType: string
  authField: string
  authValue: string
  contentType: string
  pageSize: string
  headers: KvPair[]
  queryParams: KvPair[]
}

type OutputFormat = 'json' | 'yaml'

const DEFAULT_API_ROOT = 'https://ql-test-backend.kic.network'

let nextSourceId = 0
let nextKvId = 0

function createSource(): SourceData {
  return {
    id: nextSourceId++,
    type: '',
    path: '',
    version: '',
    refresh: '',
    sourceId: '',
    name: '',
    authType: '',
    authField: '',
    authValue: '',
    contentType: '',
    pageSize: '',
    headers: [],
    queryParams: [],
  }
}

// ─── Crypto helpers ───────────────────────────────────────────────────────

function b64ToBuffer(b64: string): ArrayBuffer {
  const binary = atob(b64)
  const buf = new Uint8Array(binary.length)
  for (let i = 0; i < binary.length; i++) buf[i] = binary.charCodeAt(i)
  return buf.buffer
}

function pkcs1ToSpki(pkcs1Buffer: ArrayBuffer): ArrayBuffer {
  const pkcs1 = new Uint8Array(pkcs1Buffer)
  const algId = [
    0x30, 0x0d, 0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01,
    0x01, 0x05, 0x00,
  ]
  function derLen(n: number): number[] {
    if (n < 0x80) return [n]
    if (n < 0x100) return [0x81, n]
    return [0x82, (n >> 8) & 0xff, n & 0xff]
  }
  const bsInner = [0x00, ...pkcs1]
  const bsHeader = [0x03, ...derLen(bsInner.length)]
  const inner = [...algId, ...bsHeader, ...bsInner]
  const spki = new Uint8Array([0x30, ...derLen(inner.length), ...inner])
  return spki.buffer
}

async function importPem(pem: string): Promise<CryptoKey> {
  let spkiBuffer: ArrayBuffer
  if (/BEGIN PUBLIC KEY/.test(pem)) {
    const b64 = pem.replace(/-----[^-]+-----|[\s]/g, '')
    spkiBuffer = b64ToBuffer(b64)
  } else if (/BEGIN RSA PUBLIC KEY/.test(pem)) {
    const b64 = pem.replace(/-----[^-]+-----|[\s]/g, '')
    spkiBuffer = pkcs1ToSpki(b64ToBuffer(b64))
  } else {
    throw new Error(
      'Unrecognised PEM format. Expected "BEGIN PUBLIC KEY" (SPKI) or "BEGIN RSA PUBLIC KEY" (PKCS#1).'
    )
  }
  return crypto.subtle.importKey('spki', spkiBuffer, { name: 'RSA-OAEP', hash: 'SHA-1' }, false, [
    'encrypt',
  ])
}

// ─── Manifest builder ─────────────────────────────────────────────────────

function buildManifest(
  meta: { schac: string; eterid: string; deqarid: string; did: string },
  sources: SourceData[]
) {
  const manifest: Record<string, unknown> = {}

  const metaObj = Object.fromEntries(
    Object.entries(meta).filter(([, v]) => v.trim())
  )
  if (Object.keys(metaObj).length) manifest.meta = metaObj

  const srcArr = sources
    .filter((s) => s.type && s.path.trim())
    .map((s) => {
      const src: Record<string, unknown> = { type: s.type, path: s.path.trim() }
      if (s.version.trim()) src.version = s.version.trim()
      if (s.sourceId.trim()) src.id = s.sourceId.trim()
      if (s.name.trim()) src.name = s.name.trim()
      if (s.refresh) src.refresh = Number(s.refresh)

      if (s.authType === 'httpheader') {
        const auth: Record<string, string> = { type: 'httpheader' }
        if (s.authField.trim()) auth.field = s.authField.trim()
        if (s.authValue.trim()) auth.value = s.authValue.trim()
        src.auth = auth
      }

      if (s.type === 'elm' && s.contentType) src.contentType = s.contentType
      if ((s.type === 'ooapi' || s.type === 'edu-api') && s.pageSize)
        src.pageSize = Number(s.pageSize)

      const headers = Object.fromEntries(
        s.headers.filter((h) => h.key.trim()).map((h) => [h.key.trim(), h.value.trim()])
      )
      if (Object.keys(headers).length) src.headers = headers

      const qp = Object.fromEntries(
        s.queryParams.filter((p) => p.key.trim()).map((p) => [p.key.trim(), p.value.trim()])
      )
      if (Object.keys(qp).length) src.queryParameters = qp

      return src
    })

  manifest.sources = srcArr
  return manifest
}

// ─── Sub-components ───────────────────────────────────────────────────────

function KvRows({
  items,
  label,
  onAdd,
  onRemove,
  onChange,
}: {
  items: KvPair[]
  label: string
  onAdd: () => void
  onRemove: (id: number) => void
  onChange: (id: number, field: 'key' | 'value', val: string) => void
}) {
  return (
    <div className="mt-4 pt-4 border-t border-gray-100">
      <h4 className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-2">
        {label}
      </h4>
      {items.map((item) => (
        <div key={item.id} className="grid grid-cols-[1fr_1fr_auto] gap-1.5 items-center mb-1.5">
          <input
            type="text"
            className="w-full px-2.5 py-1.5 border border-gray-300 rounded text-sm focus:outline-none focus:border-blue-600 focus:ring-1 focus:ring-blue-600/10"
            placeholder="Name"
            value={item.key}
            onChange={(e) => onChange(item.id, 'key', e.target.value)}
          />
          <input
            type="text"
            className="w-full px-2.5 py-1.5 border border-gray-300 rounded text-sm focus:outline-none focus:border-blue-600 focus:ring-1 focus:ring-blue-600/10"
            placeholder="Value"
            value={item.value}
            onChange={(e) => onChange(item.id, 'value', e.target.value)}
          />
          <button
            className="text-gray-400 hover:text-red-600 hover:bg-red-50 px-1.5 py-0.5 rounded text-lg leading-none"
            onClick={() => onRemove(item.id)}
            title="Remove"
          >
            &times;
          </button>
        </div>
      ))}
      <button
        className="inline-flex items-center gap-1.5 px-2 py-1 rounded text-xs font-medium bg-gray-100 text-gray-700 border border-gray-200 hover:bg-gray-200 w-full justify-center mt-1"
        onClick={onAdd}
      >
        + Add {label.replace(/s$/, '')}
      </button>
    </div>
  )
}

function SourceCard({
  source,
  index,
  onChange,
  onRemove,
  onEncrypt,
  encryptStatus,
}: {
  source: SourceData
  index: number
  onChange: (id: number, updates: Partial<SourceData>) => void
  onRemove: (id: number) => void
  onEncrypt: (id: number) => void
  encryptStatus: { type: string; msg: string } | null
}) {
  const update = (updates: Partial<SourceData>) => onChange(source.id, updates)

  const addKv = (field: 'headers' | 'queryParams') => {
    update({ [field]: [...source[field], { id: nextKvId++, key: '', value: '' }] })
  }

  const removeKv = (field: 'headers' | 'queryParams', kvId: number) => {
    update({ [field]: source[field].filter((kv) => kv.id !== kvId) })
  }

  const changeKv = (
    field: 'headers' | 'queryParams',
    kvId: number,
    kvField: 'key' | 'value',
    val: string
  ) => {
    update({
      [field]: source[field].map((kv) => (kv.id === kvId ? { ...kv, [kvField]: val } : kv)),
    })
  }

  return (
    <div className="border border-gray-200 rounded-lg bg-white mb-3 overflow-hidden">
      <div className="flex justify-between items-center px-4 py-2.5 bg-gray-50 border-b border-gray-200">
        <span className="text-sm font-semibold text-gray-700">Source {index + 1}</span>
        <button
          className="inline-flex items-center gap-1.5 px-2 py-1 rounded text-xs font-medium bg-red-50 text-red-600 border border-red-300 hover:bg-red-100"
          onClick={() => onRemove(source.id)}
        >
          Remove
        </button>
      </div>
      <div className="p-4">
        <div className="grid grid-cols-2 gap-2.5 max-sm:grid-cols-1">
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">
              Type <span className="text-red-600">*</span>
            </label>
            <select
              className="w-full px-2.5 py-1.5 border border-gray-300 rounded text-sm focus:outline-none focus:border-blue-600 focus:ring-1 focus:ring-blue-600/10"
              value={source.type}
              onChange={(e) => update({ type: e.target.value, contentType: '', pageSize: '' })}
            >
              <option value="">-- select --</option>
              <option value="elm">ELM (European Learning Model)</option>
              <option value="ooapi">OOAPI</option>
              <option value="edu-api">Edu-API</option>
              <option value="occapi">OCCAPI</option>
            </select>
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">
              Path (URL) <span className="text-red-600">*</span>
            </label>
            <input
              type="url"
              className="w-full px-2.5 py-1.5 border border-gray-300 rounded text-sm focus:outline-none focus:border-blue-600 focus:ring-1 focus:ring-blue-600/10"
              placeholder="https://..."
              value={source.path}
              onChange={(e) => update({ path: e.target.value })}
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">Version</label>
            <input
              type="text"
              className="w-full px-2.5 py-1.5 border border-gray-300 rounded text-sm focus:outline-none focus:border-blue-600 focus:ring-1 focus:ring-blue-600/10"
              value={source.version}
              onChange={(e) => update({ version: e.target.value })}
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">
              Refresh <span className="font-normal text-gray-400">(hours)</span>
            </label>
            <input
              type="number"
              className="w-full px-2.5 py-1.5 border border-gray-300 rounded text-sm focus:outline-none focus:border-blue-600 focus:ring-1 focus:ring-blue-600/10"
              placeholder="e.g. 24"
              min={1}
              value={source.refresh}
              onChange={(e) => update({ refresh: e.target.value })}
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">ID</label>
            <input
              type="text"
              className="w-full px-2.5 py-1.5 border border-gray-300 rounded text-sm focus:outline-none focus:border-blue-600 focus:ring-1 focus:ring-blue-600/10"
              placeholder="Optional identifier"
              value={source.sourceId}
              onChange={(e) => update({ sourceId: e.target.value })}
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">Name</label>
            <input
              type="text"
              className="w-full px-2.5 py-1.5 border border-gray-300 rounded text-sm focus:outline-none focus:border-blue-600 focus:ring-1 focus:ring-blue-600/10"
              placeholder="Human-readable description"
              value={source.name}
              onChange={(e) => update({ name: e.target.value })}
            />
          </div>
        </div>

        {/* Type-specific options */}
        {source.type === 'elm' && (
          <div className="mt-4 pt-4 border-t border-gray-100">
            <h4 className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-2">
              ELM Options
            </h4>
            <div className="max-w-xs">
              <label className="block text-xs font-medium text-gray-700 mb-1">
                Content-Type Override
              </label>
              <select
                className="w-full px-2.5 py-1.5 border border-gray-300 rounded text-sm focus:outline-none focus:border-blue-600 focus:ring-1 focus:ring-blue-600/10"
                value={source.contentType}
                onChange={(e) => update({ contentType: e.target.value })}
              >
                <option value="">-- use server-provided content type --</option>
                <option value="application/rdf+xml">application/rdf+xml (RDF/XML)</option>
                <option value="text/turtle">text/turtle (Turtle)</option>
                <option value="application/ld+json">application/ld+json (JSON-LD)</option>
              </select>
              <p className="text-xs text-gray-400 mt-1">
                Only set this if the server returns an incorrect Content-Type header.
              </p>
            </div>
          </div>
        )}

        {(source.type === 'ooapi' || source.type === 'edu-api') && (
          <div className="mt-4 pt-4 border-t border-gray-100">
            <h4 className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-2">
              {source.type === 'ooapi' ? 'OOAPI' : 'Edu-API'} Options
            </h4>
            <div className="max-w-[200px]">
              <label className="block text-xs font-medium text-gray-700 mb-1">Page Size</label>
              {source.type === 'ooapi' ? (
                <select
                  className="w-full px-2.5 py-1.5 border border-gray-300 rounded text-sm focus:outline-none focus:border-blue-600 focus:ring-1 focus:ring-blue-600/10"
                  value={source.pageSize}
                  onChange={(e) => update({ pageSize: e.target.value })}
                >
                  <option value="">-- default (250) --</option>
                  <option value="10">10</option>
                  <option value="20">20</option>
                  <option value="50">50</option>
                  <option value="100">100</option>
                  <option value="250">250</option>
                </select>
              ) : (
                <input
                  type="number"
                  className="w-full px-2.5 py-1.5 border border-gray-300 rounded text-sm focus:outline-none focus:border-blue-600 focus:ring-1 focus:ring-blue-600/10"
                  min={1}
                  placeholder="default 500"
                  value={source.pageSize}
                  onChange={(e) => update({ pageSize: e.target.value })}
                />
              )}
            </div>
          </div>
        )}

        {/* Authentication */}
        <div className="mt-4 pt-4 border-t border-gray-100">
          <h4 className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-2">
            Authentication
          </h4>
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">Auth Type</label>
            <select
              className="w-full px-2.5 py-1.5 border border-gray-300 rounded text-sm focus:outline-none focus:border-blue-600 focus:ring-1 focus:ring-blue-600/10"
              value={source.authType}
              onChange={(e) => update({ authType: e.target.value })}
            >
              <option value="">None (IP-based / no auth)</option>
              <option value="httpheader">HTTP Header</option>
            </select>
          </div>
          {source.authType === 'httpheader' && (
            <div className="mt-2 space-y-2">
              <div>
                <label className="block text-xs font-medium text-gray-700 mb-1">
                  Header Field Name
                </label>
                <input
                  type="text"
                  className="w-full px-2.5 py-1.5 border border-gray-300 rounded text-sm focus:outline-none focus:border-blue-600 focus:ring-1 focus:ring-blue-600/10"
                  placeholder="e.g. X-Api-Key, Authorization"
                  value={source.authField}
                  onChange={(e) => update({ authField: e.target.value })}
                />
              </div>
              <div>
                <label className="block text-xs font-medium text-gray-700 mb-1">
                  Header Value
                </label>
                <div className="flex gap-2 items-stretch">
                  <input
                    type="text"
                    className="flex-1 px-2.5 py-1.5 border border-gray-300 rounded text-sm focus:outline-none focus:border-blue-600 focus:ring-1 focus:ring-blue-600/10"
                    placeholder="Plain-text or encrypted value"
                    value={source.authValue}
                    onChange={(e) => update({ authValue: e.target.value })}
                  />
                  <button
                    className="inline-flex items-center gap-1.5 px-2 py-1 rounded text-xs font-medium bg-gray-100 text-gray-700 border border-gray-200 hover:bg-gray-200"
                    onClick={() => onEncrypt(source.id)}
                    title="Encrypt with aggregator public key"
                  >
                    Encrypt
                  </button>
                </div>
                <p className="text-xs text-gray-400 mt-1">
                  Click <strong>Encrypt</strong> to replace the plain-text value with an RSA-OAEP
                  encrypted, Base64-encoded ciphertext using the aggregator's public key.
                </p>
                {encryptStatus && (
                  <div
                    className={`text-xs mt-1 px-2.5 py-1.5 rounded ${
                      encryptStatus.type === 'success'
                        ? 'bg-green-50 text-green-600'
                        : encryptStatus.type === 'error'
                          ? 'bg-red-50 text-red-600'
                          : 'bg-blue-50 text-blue-800'
                    }`}
                  >
                    {encryptStatus.msg}
                  </div>
                )}
              </div>
            </div>
          )}
        </div>

        {/* Additional HTTP Headers */}
        <KvRows
          items={source.headers}
          label="Additional HTTP Headers"
          onAdd={() => addKv('headers')}
          onRemove={(kvId) => removeKv('headers', kvId)}
          onChange={(kvId, field, val) => changeKv('headers', kvId, field, val)}
        />

        {/* Query Parameters */}
        <KvRows
          items={source.queryParams}
          label="Query Parameters"
          onAdd={() => addKv('queryParams')}
          onRemove={(kvId) => removeKv('queryParams', kvId)}
          onChange={(kvId, field, val) => changeKv('queryParams', kvId, field, val)}
        />
      </div>
    </div>
  )
}

// ─── Main page ────────────────────────────────────────────────────────────

export default function ManifestBuilderPage() {
  const [meta, setMeta] = useState({ schac: '', eterid: '', deqarid: '', did: '' })
  const [sources, setSources] = useState<SourceData[]>([createSource()])
  const [format, setFormat] = useState<OutputFormat>('json')
  const [apiRoot, setApiRoot] = useState(DEFAULT_API_ROOT)
  const [keyStatus, setKeyStatus] = useState<{ type: string; msg: string } | null>(null)
  const [encryptStatuses, setEncryptStatuses] = useState<
    Record<number, { type: string; msg: string }>
  >({})
  const [copyLabel, setCopyLabel] = useState('Copy')

  const cachedKeyRef = useRef<CryptoKey | null>(null)

  const updateSource = useCallback((id: number, updates: Partial<SourceData>) => {
    setSources((prev) => prev.map((s) => (s.id === id ? { ...s, ...updates } : s)))
  }, [])

  const removeSource = useCallback((id: number) => {
    setSources((prev) => prev.filter((s) => s.id !== id))
  }, [])

  const addSource = useCallback(() => {
    setSources((prev) => [...prev, createSource()])
  }, [])

  // Manifest output
  const manifest = useMemo(() => buildManifest(meta, sources), [meta, sources])

  const outputText = useMemo(() => {
    try {
      if (format === 'json') {
        return JSON.stringify(manifest, null, 2)
      }
      return yaml.dump(manifest, {
        lineWidth: 120,
        quotingType: "'",
        forceQuotes: false,
        noCompatMode: true,
      })
    } catch (e) {
      return `# Error generating output:\n# ${(e as Error).message}`
    }
  }, [manifest, format])

  // Fetch public key
  const fetchAndCacheKey = useCallback(async (): Promise<CryptoKey | null> => {
    const root = apiRoot.trim().replace(/\/+$/, '')
    if (!root) {
      setKeyStatus({ type: 'error', msg: 'Please enter an aggregator API root URL.' })
      return null
    }
    setKeyStatus({ type: 'info', msg: 'Fetching public key...' })
    try {
      const resp = await fetch(`${root}/api/v1/public-key/pem`)
      if (!resp.ok) throw new Error(`HTTP ${resp.status} ${resp.statusText}`)
      const pem = (await resp.text()).trim()
      const key = await importPem(pem)
      cachedKeyRef.current = key
      setKeyStatus({ type: 'success', msg: 'Public key loaded successfully.' })
      return key
    } catch (e) {
      cachedKeyRef.current = null
      setKeyStatus({ type: 'error', msg: `Failed: ${(e as Error).message}` })
      return null
    }
  }, [apiRoot])

  // Encrypt a source's auth value
  const encryptSourceValue = useCallback(
    async (sourceId: number) => {
      const source = sources.find((s) => s.id === sourceId)
      if (!source) return
      const raw = source.authValue.trim()
      if (!raw) {
        setEncryptStatuses((prev) => ({
          ...prev,
          [sourceId]: { type: 'error', msg: 'Enter a plain-text value first.' },
        }))
        return
      }

      let key = cachedKeyRef.current
      if (!key) {
        key = await fetchAndCacheKey()
        if (!key) return
      }

      setEncryptStatuses((prev) => ({
        ...prev,
        [sourceId]: { type: 'info', msg: 'Encrypting...' },
      }))

      try {
        const plaintext = new TextEncoder().encode(raw)
        const cipherBuf = await crypto.subtle.encrypt({ name: 'RSA-OAEP' }, key, plaintext)
        const bytes = new Uint8Array(cipherBuf)
        let binary = ''
        for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i])
        const encrypted = btoa(binary)

        setSources((prev) =>
          prev.map((s) => (s.id === sourceId ? { ...s, authValue: encrypted } : s))
        )
        setEncryptStatuses((prev) => ({
          ...prev,
          [sourceId]: {
            type: 'success',
            msg: 'Value encrypted (RSA-OAEP/SHA-1). Plain text replaced.',
          },
        }))
      } catch (e) {
        setEncryptStatuses((prev) => ({
          ...prev,
          [sourceId]: { type: 'error', msg: `Encryption failed: ${(e as Error).message}` },
        }))
      }
    },
    [sources, fetchAndCacheKey]
  )

  const copyOutput = useCallback(() => {
    navigator.clipboard.writeText(outputText).then(() => {
      setCopyLabel('Copied!')
      setTimeout(() => setCopyLabel('Copy'), 1600)
    })
  }, [outputText])

  const downloadOutput = useCallback(() => {
    const ext = format === 'json' ? 'json' : 'yaml'
    const mime = format === 'json' ? 'application/json' : 'text/yaml'
    const blob = new Blob([outputText], { type: `${mime};charset=utf-8` })
    const url = URL.createObjectURL(blob)
    const a = Object.assign(document.createElement('a'), {
      href: url,
      download: `quality-link-manifest.${ext}`,
    })
    a.click()
    URL.revokeObjectURL(url)
  }, [outputText, format])

  return (
    <div className="min-h-screen bg-gray-50 flex flex-col">
      <Navbar />

      {/* Hero header */}
      <div className="bg-primary text-white px-8 py-4 shadow-md">
        <div className="max-w-7xl mx-auto">
          <h1 className="text-lg font-semibold">QualityLink Manifest Creator</h1>
          <p className="text-sm text-white/80 mt-0.5">
            Build a standardised manifest file per the QualityLink Discovery &amp; Data Exchange
            specifications
          </p>
        </div>
      </div>

      {/* Main layout */}
      <div className="max-w-7xl mx-auto w-full grid grid-cols-[1fr_420px] gap-6 p-6 items-start max-lg:grid-cols-1">
        {/* Left: Form */}
        <div>
          {/* Institutional metadata */}
          <div className="bg-white rounded-lg shadow-sm p-5 mb-4">
            <h2 className="text-sm font-semibold text-gray-700 mb-4 pb-2 border-b border-gray-200 flex items-baseline gap-2">
              Institutional Metadata
              <span className="text-xs font-normal text-gray-400">optional</span>
            </h2>
            <div className="grid grid-cols-2 gap-2.5 max-sm:grid-cols-1">
              <div>
                <label className="block text-xs font-medium text-gray-700 mb-1">SCHAC code</label>
                <input
                  type="text"
                  className="w-full px-2.5 py-1.5 border border-gray-300 rounded text-sm focus:outline-none focus:border-blue-600 focus:ring-1 focus:ring-blue-600/10"
                  placeholder="example.ac.at"
                  value={meta.schac}
                  onChange={(e) => setMeta((m) => ({ ...m, schac: e.target.value }))}
                />
              </div>
              <div>
                <label className="block text-xs font-medium text-gray-700 mb-1">ETER ID</label>
                <input
                  type="text"
                  className="w-full px-2.5 py-1.5 border border-gray-300 rounded text-sm focus:outline-none focus:border-blue-600 focus:ring-1 focus:ring-blue-600/10"
                  placeholder="AT4711"
                  value={meta.eterid}
                  onChange={(e) => setMeta((m) => ({ ...m, eterid: e.target.value }))}
                />
              </div>
              <div>
                <label className="block text-xs font-medium text-gray-700 mb-1">DEQAR ID</label>
                <input
                  type="text"
                  className="w-full px-2.5 py-1.5 border border-gray-300 rounded text-sm focus:outline-none focus:border-blue-600 focus:ring-1 focus:ring-blue-600/10"
                  placeholder="DEQARINST0815"
                  value={meta.deqarid}
                  onChange={(e) => setMeta((m) => ({ ...m, deqarid: e.target.value }))}
                />
              </div>
              <div>
                <label className="block text-xs font-medium text-gray-700 mb-1">DID</label>
                <input
                  type="text"
                  className="w-full px-2.5 py-1.5 border border-gray-300 rounded text-sm focus:outline-none focus:border-blue-600 focus:ring-1 focus:ring-blue-600/10"
                  placeholder="did:example:123"
                  value={meta.did}
                  onChange={(e) => setMeta((m) => ({ ...m, did: e.target.value }))}
                />
              </div>
            </div>
          </div>

          {/* Data sources */}
          <div className="bg-white rounded-lg shadow-sm p-5 mb-4">
            <h2 className="text-sm font-semibold text-gray-700 mb-4 pb-2 border-b border-gray-200">
              Data Sources
            </h2>
            {sources.map((source, i) => (
              <SourceCard
                key={source.id}
                source={source}
                index={i}
                onChange={updateSource}
                onRemove={removeSource}
                onEncrypt={encryptSourceValue}
                encryptStatus={encryptStatuses[source.id] ?? null}
              />
            ))}
            <button
              className="w-full inline-flex items-center justify-center gap-1.5 px-3.5 py-2 rounded text-sm font-medium bg-blue-600 text-white hover:bg-blue-700 mt-1"
              onClick={addSource}
            >
              + Add Data Source
            </button>
          </div>

          {/* Encryption settings */}
          <div className="bg-white rounded-lg shadow-sm p-5 mb-4">
            <h2 className="text-sm font-semibold text-gray-700 mb-4 pb-2 border-b border-gray-200">
              Encryption Settings
            </h2>
            <div className="text-xs text-gray-500 px-3 py-2 bg-blue-50 border-l-[3px] border-blue-600 rounded-r mb-3">
              Encrypt HTTP header values with the aggregator's RSA public key so secrets are not
              stored in plain text in the manifest. The encrypted value will be Base64-encoded and
              placed in the <code className="text-xs">auth.value</code> field.
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-700 mb-1">
                Aggregator API Root
              </label>
              <div className="flex gap-2 items-stretch">
                <input
                  type="url"
                  className="flex-1 px-2.5 py-1.5 border border-gray-300 rounded text-sm focus:outline-none focus:border-blue-600 focus:ring-1 focus:ring-blue-600/10"
                  placeholder="https://aggregator.example.org"
                  value={apiRoot}
                  onChange={(e) => setApiRoot(e.target.value)}
                />
                <button
                  className="inline-flex items-center gap-1.5 px-3.5 py-1.5 rounded text-sm font-medium bg-gray-100 text-gray-700 border border-gray-200 hover:bg-gray-200"
                  onClick={fetchAndCacheKey}
                >
                  Fetch Key
                </button>
              </div>
              <p className="text-xs text-gray-400 mt-1">
                Public key will be fetched from{' '}
                <code className="text-xs">{'{root}'}/api/v1/public-key/pem</code>. The API must
                allow cross-origin requests (CORS) from this page's origin.
              </p>
              {keyStatus && (
                <div
                  className={`text-xs mt-2 px-2.5 py-1.5 rounded ${
                    keyStatus.type === 'success'
                      ? 'bg-green-50 text-green-600'
                      : keyStatus.type === 'error'
                        ? 'bg-red-50 text-red-600'
                        : 'bg-blue-50 text-blue-800'
                  }`}
                >
                  {keyStatus.msg}
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Right: Output */}
        <div className="sticky top-6">
          <div className="bg-white rounded-lg shadow-sm p-5">
            <h2 className="text-sm font-semibold text-gray-700 mb-4 pb-2 border-b border-gray-200">
              Output
            </h2>
            <div className="flex gap-1 mb-3">
              <button
                className={`px-3 py-1 rounded text-sm font-medium border transition-colors ${
                  format === 'json'
                    ? 'bg-blue-600 text-white border-blue-600'
                    : 'bg-white text-gray-700 border-gray-200 hover:bg-gray-100'
                }`}
                onClick={() => setFormat('json')}
              >
                JSON
              </button>
              <button
                className={`px-3 py-1 rounded text-sm font-medium border transition-colors ${
                  format === 'yaml'
                    ? 'bg-blue-600 text-white border-blue-600'
                    : 'bg-white text-gray-700 border-gray-200 hover:bg-gray-100'
                }`}
                onClick={() => setFormat('yaml')}
              >
                YAML
              </button>
            </div>
            <pre className="bg-slate-900 text-slate-300 font-mono text-[0.8rem] leading-relaxed p-4 rounded-md overflow-auto max-h-[calc(100vh-280px)] min-h-[200px] whitespace-pre tab-size-2">
              {outputText}
            </pre>
            <div className="flex gap-2 mt-3 flex-wrap">
              <button
                className="inline-flex items-center gap-1.5 px-3.5 py-1.5 rounded text-sm font-medium bg-gray-100 text-gray-700 border border-gray-200 hover:bg-gray-200"
                onClick={copyOutput}
              >
                {copyLabel}
              </button>
              <button
                className="inline-flex items-center gap-1.5 px-3.5 py-1.5 rounded text-sm font-medium bg-blue-600 text-white hover:bg-blue-700"
                onClick={downloadOutput}
              >
                Download
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
