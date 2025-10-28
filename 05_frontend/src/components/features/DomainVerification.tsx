import { motion } from 'framer-motion'
import Badge from '../ui/Badge'
import { CheckCircle, XCircle, HelpCircle, RotateCw } from 'lucide-react'
import { Link } from 'react-router-dom'

interface DomainMethod {
  domain: string
  method: string
  status: 'found' | 'not_found' | 'valid' | 'not_searched'
  message?: string
  manifestPath?: string
}

interface DomainVerificationProps {
  methods: DomainMethod[]
  onRefresh: () => void
  isRefreshing?: boolean
  isDisabled?: boolean
}

export default function DomainVerification({
  methods,
  onRefresh,
  isRefreshing = false,
  isDisabled = false,
}: DomainVerificationProps) {
  // Get unique domains for the subtitle
  const uniqueDomains = [...new Set(methods.map(m => m.domain))].join(', ')

  return (
    <section>
      <div className="mb-8">
        <h2 className="text-2xl font-semibold text-gray-900 mb-1">
          Domain search order
        </h2>
        <p className="text-sm text-gray-500">
          {methods.length} domains found: {uniqueDomains.split(',').map((domain, i) => (
            <span key={i}>
              {i > 0 && ', '}
              <span className="text-gray-900 underline">{domain.trim()}</span>
            </span>
          ))}
        </p>
      </div>

      <div className="bg-white rounded-lg overflow-hidden">
        {/* Timeline with methods */}
        <div className="flex flex-col gap-5">
          {methods.map((method, index) => (
            <div key={index} className="flex gap-7">
              {/* Timeline column with dot and line */}
              <div className="flex flex-col items-center shrink-0 w-3.5 relative mt-[10px]">
                {/* Dot aligned with domain name */}
                <div 
                  className={`
                    w-3.5 h-3.5 rounded-full shrink-0 relative
                    ${method.status === 'not_found' ? 'bg-red-800' : 
                      method.status === 'found' || method.status === 'valid' ? 'bg-brand-base' : 
                      'bg-gray-100'}
                  `}
                  style={{
                    boxShadow: method.status === 'not_searched' 
                      ? 'inset 0 0 0 3px #f3f4f6' 
                      : 'inset 0 0 0 3px #e5e7eb'
                  }}
                />
                {/* Connecting line - fills remaining space in row */}
                {index < methods.length - 1 && (
                  <div 
                    className={`w-0.5 absolute left-1/2 -translate-x-1/2 ${
                      method.status === 'not_searched' || methods[index + 1]?.status === 'not_searched'
                        ? 'bg-gray-100'
                        : 'bg-gray-200'
                    }`}
                    style={{
                      top: '14px',
                      bottom: '-30px'
                    }}
                  />
                )}
              </div>

              {/* Content column */}
              <div className="flex-1 flex flex-col gap-2">
                {/* Method row */}
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-4">
                    <span className="text-base font-medium text-gray-900">
                      {method.domain}
                    </span>
                    <span className="text-sm text-gray-900">
                      {method.method}
                    </span>
                  </div>
                  {method.status === 'not_found' && (
                    <Badge variant="error">
                      <XCircle className="w-2.5 h-2.5" />
                      Manifest not found
                    </Badge>
                  )}
                  {method.status === 'found' && (
                    <Badge variant="success">
                      <CheckCircle className="w-2.5 h-2.5" />
                      Manifest found
                    </Badge>
                  )}
                  {method.status === 'valid' && (
                    <Badge variant="success">
                      <CheckCircle className="w-2.5 h-2.5" />
                      Manifest found
                    </Badge>
                  )}
                  {method.status === 'not_searched' && (
                    <Badge className="bg-gray-100 text-gray-500 border-gray-100">
                      Not searched
                    </Badge>
                  )}
                </div>

                {/* Expanded details for valid manifests */}
                {method.status === 'valid' && method.manifestPath && (
                  <motion.div
                    initial={{ opacity: 0, height: 0 }}
                    animate={{ opacity: 1, height: 'auto' }}
                    transition={{ duration: 0.2 }}
                    className="bg-brand-50 rounded-lg p-4 flex flex-col gap-1.5 mt-2"
                  >
                    <div className="flex items-center gap-2">
                      <CheckCircle className="w-4.5 h-4.5 text-brand-600" />
                      <span className="text-sm font-medium text-brand-600">
                        Manifest file is valid
                      </span>
                    </div>
                    <p className="text-sm text-gray-900">
                      Found at: <span className="underline">{method.manifestPath}</span>
                    </p>
                  </motion.div>
                )}
              </div>
            </div>
          ))}
        </div>

        {/* Actions footer */}
        <div className="flex items-center gap-8 pt-8 pb-0">
          <button
            onClick={onRefresh}
            disabled={isRefreshing || isDisabled}
            className="bg-brand-base text-white px-5 py-2.5 rounded-lg text-sm font-medium hover:opacity-90 disabled:opacity-50 flex items-center gap-2 transition-opacity"
          >
            <RotateCw className={`w-3.5 h-3.5 ${isRefreshing ? 'animate-spin' : ''}`} />
            Refresh
          </button>
          <div className="flex items-center gap-2">
            <HelpCircle className="w-4.5 h-4.5 text-gray-500" />
            <p className="text-sm text-gray-500">
              Was your manifest file not found?{' '}
              <Link to="/help" className="text-brand-base font-semibold underline hover:opacity-80">
                Visit the help page
              </Link>
            </p>
          </div>
        </div>
      </div>
    </section>
  )
}

