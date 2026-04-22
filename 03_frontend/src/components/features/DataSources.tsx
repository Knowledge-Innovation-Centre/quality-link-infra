import { File, Eye, ChevronDown, RotateCw, Download, FileText } from 'lucide-react'
import { useState, useRef, useEffect } from 'react'
import { AnimatePresence, motion } from 'framer-motion'

interface DataFile {
  filename: string
  timestamp: string
  isPushed?: boolean
  pushDate?: string
  fullPath?: string
  status?: string
  logFilePath?: string | null
}

interface DataSource {
  id: string
  sourceId?: string | null
  name: string
  source_name?: string
  type: string
  pushed: string
  files: DataFile[]
  sourcePath?: string
  availableDates?: string[]
  selectedDate?: string | null
}

interface DataSourcesProps {
  dataSources: DataSource[]
  onPreviewJson: (filename: string, fullPath?: string) => void
  onDownload: (filename: string, fullPath?: string) => void
  onRefresh?: (sourceId: string) => void
  onExpand?: (sourceId: string, sourcePath: string) => void
  onDateChange?: (sourceId: string, sourcePath: string, date: string) => void
  isRefreshing?: boolean
}

export default function DataSources({
  dataSources,
  onPreviewJson,
  onDownload,
  onRefresh,
  onExpand,
  onDateChange,
  isRefreshing = false
}: DataSourcesProps) {
  const [expandedSources, setExpandedSources] = useState<Set<string>>(new Set())
  const [openDropdownId, setOpenDropdownId] = useState<string | null>(null)
  const dropdownRefs = useRef<Record<string, HTMLDivElement | null>>({})

  // Truncate UUID for display
  const truncateUuid = (uuid: string) => {
    return uuid.length > 8 ? `${uuid.substring(0, 8)}...` : uuid
  }

  const toggleExpand = (id: string, sourcePath?: string) => {
    const isCurrentlyExpanded = expandedSources.has(id)

    setExpandedSources(prev => {
      const newSet = new Set(prev)
      if (newSet.has(id)) {
        newSet.delete(id)
      } else {
        newSet.add(id)
      }
      return newSet
    })

    // Call onExpand when expanding (not collapsing)
    if (!isCurrentlyExpanded && onExpand && sourcePath) {
      onExpand(id, sourcePath)
    }
  }

  const isExpanded = (id: string) => expandedSources.has(id)

  const toggleDropdown = (id: string) => {
    setOpenDropdownId(prev => prev === id ? null : id)
  }

  const isDropdownOpen = (id: string) => openDropdownId === id

  // Close dropdown when clicking outside
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (openDropdownId) {
        const dropdownRef = dropdownRefs.current[openDropdownId]
        if (dropdownRef && !dropdownRef.contains(event.target as Node)) {
          setOpenDropdownId(null)
        }
      }
    }

    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [openDropdownId])

  return (
    <section className="flex flex-col gap-8">
      {/* Header */}
      <div className="bg-white flex flex-col gap-1">
        <div>
          <h2 className="text-2xl font-semibold text-gray-900">Data sources</h2>
          <p className="text-sm text-gray-500">The following data sources were found in the manifest file</p>
        </div>
      </div>

      {/* Data Sources Table */}
      <div className="flex flex-col gap-4">
        <div className="border border-gray-200 rounded-lg overflow-x-auto">
          <div className="min-w-[560px]">
          {/* Table Header */}
          <div className="flex bg-gray-50 w-full">
            <div className="px-4 py-4 w-[22%]">
              <p className="text-xs font-semibold text-gray-500 uppercase">ID</p>
            </div>
            <div className="px-4 py-4 w-[34%]">
              <p className="text-xs font-semibold text-gray-500 uppercase">Name</p>
            </div>
            <div className="px-4 py-4 w-[14%]">
              <p className="text-xs font-semibold text-gray-500 uppercase">Type</p>
            </div>
            <div className="px-4 py-4 w-[22%]">
              <p className="text-xs font-semibold text-gray-500 uppercase">PUSHED</p>
            </div>
            <div className="px-4 py-4 w-[8%]" />
          </div>

          {/* Table Body */}
          {dataSources.map((source) => (
            <div key={source.id}>
              {/* Main Row */}
              <div className={`flex items-center border-t border-gray-200 w-full ${isExpanded(source.id) ? 'bg-gray-50' : 'bg-white'}`}>
                <div className="px-4 py-4 w-[22%]">
                  <p className="text-sm text-gray-900 truncate" title={source.sourceId || source.id}>
                    {source.sourceId || truncateUuid(source.id)}
                  </p>
                </div>
                <div className="px-4 py-4 w-[34%]">
                  <p className="text-sm text-gray-900 truncate">{source.source_name}</p>
                </div>
                <div className="px-4 py-4 w-[14%]">
                  <div className="bg-gray-100 px-2.5 py-0.5 rounded-md inline-block">
                    <p className="text-sm font-semibold text-gray-800">{source.type}</p>
                  </div>
                </div>
                <div className="px-4 py-4 w-[22%]">
                  <p className="text-sm text-gray-900">{source.pushed}</p>
                </div>
                <div className="px-4 py-4 w-[8%] flex justify-end">
                  <button
                    onClick={() => toggleExpand(source.id, source.sourcePath)}
                    className="p-0.5 hover:bg-gray-200 rounded transition-colors"
                  >
                    <motion.div
                      animate={{ rotate: isExpanded(source.id) ? 180 : 0 }}
                      transition={{ duration: 0.3, ease: 'easeInOut' }}
                    >
                      <ChevronDown className="w-3.5 h-3.5 text-gray-600" />
                    </motion.div>
                  </button>
                </div>
              </div>

              {/* Expanded Content */}
              <AnimatePresence initial={false}>
                {isExpanded(source.id) && (
                  <motion.div
                    initial={{ height: 0, opacity: 0 }}
                    animate={{ height: 'auto', opacity: 1 }}
                    exit={{ height: 0, opacity: 0 }}
                    transition={{ duration: 0.3, ease: 'easeInOut' }}
                    className="overflow-hidden"
                  >
                    <div className="bg-gray-50 border-t border-gray-200 px-4 py-4">
                  <div className="bg-white border border-gray-200 rounded-lg p-4 flex flex-col gap-8">
                    {/* Datalake Section */}
                    <div className="flex flex-col gap-3">
                      {/* Date selector and refresh button */}
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-1.5">
                          <span className="text-sm text-gray-900">Datalake from</span>
                          {source.availableDates && source.availableDates.length > 0 && (
                            <div className="relative" ref={(el) => { dropdownRefs.current[source.id] = el }}>
                              <button
                                onClick={() => toggleDropdown(source.id)}
                                className="flex items-center gap-2 px-3 py-1 text-sm font-semibold text-brand-base hover:bg-brand-50 rounded transition-colors"
                              >
                                <span>{source.selectedDate || 'Select date'}</span>
                                <motion.div
                                  animate={{ rotate: isDropdownOpen(source.id) ? 180 : 0 }}
                                  transition={{ duration: 0.2 }}
                                >
                                  <ChevronDown className="w-3 h-3" />
                                </motion.div>
                              </button>

                              <AnimatePresence>
                                {isDropdownOpen(source.id) && (
                                  <motion.div
                                    initial={{ opacity: 0, y: -10 }}
                                    animate={{ opacity: 1, y: 0 }}
                                    exit={{ opacity: 0, y: -10 }}
                                    transition={{ duration: 0.2 }}
                                    className="absolute top-full left-0 mt-1 bg-white border border-gray-200 rounded-lg shadow-lg z-10 min-w-[150px] max-h-[300px] overflow-y-auto"
                                  >
                                    {source.availableDates.map((date) => (
                                      <button
                                        key={date}
                                        onClick={(e) => {
                                          e.stopPropagation()
                                          if (onDateChange && source.sourcePath) {
                                            onDateChange(source.id, source.sourcePath, date)
                                          }
                                          setOpenDropdownId(null)
                                        }}
                                        className={`w-full text-left px-4 py-2 text-sm hover:bg-gray-50 transition-colors first:rounded-t-lg last:rounded-b-lg ${
                                          source.selectedDate === date ? 'bg-brand-50 text-brand-base font-semibold' : 'text-gray-900'
                                        }`}
                                      >
                                        {date}
                                      </button>
                                    ))}
                                  </motion.div>
                                )}
                              </AnimatePresence>
                            </div>
                          )}
                        </div>
                        {onRefresh && source.sourcePath && (
                          <button
                            onClick={() => onRefresh(source.id)}
                            disabled={isRefreshing}
                            className="bg-brand-base text-white px-3 py-2 rounded-lg text-xs font-medium hover:opacity-90 disabled:opacity-50 flex items-center gap-2 transition-opacity"
                          >
                            <RotateCw className={`w-3.5 h-3.5 ${isRefreshing ? 'animate-spin' : ''}`} />
                            Refetch data source
                          </button>
                        )}
                      </div>

                      {/* Files Table */}
                      <div className="rounded-lg border border-gray-200 overflow-x-auto">
                        <table className="w-full table-fixed min-w-[640px]">
                          <thead>
                            <tr className="bg-white">
                              <th className="text-left px-4 py-4 text-xs font-semibold text-gray-500 uppercase border-b border-gray-200 w-[40%]">
                                FILE
                              </th>
                              <th className="text-left px-4 py-4 text-xs font-semibold text-gray-500 uppercase border-b border-gray-200 w-[20%]">
                                FETCHED
                              </th>
                              <th className="text-center px-4 py-4 text-xs font-semibold text-gray-500 uppercase border-b border-gray-200 w-[40%]">
                                ACTIONS
                              </th>
                            </tr>
                          </thead>
                          <tbody>
                            {source.files.map((file, fileIndex) => {
                              const isHighlighted = file.isPushed
                              return (
                                <tr
                                  key={fileIndex}
                                  className={`${isHighlighted ? 'bg-[#f2f7fb] border-brand-base' : 'bg-white'} ${fileIndex < source.files.length - 1 ? 'border-b border-gray-200' : ''}`}
                                >
                                  <td className={`px-4 py-4 ${isHighlighted ? 'border-l-2 border-brand-base' : ''}`}>
                                    <div className="flex items-center gap-4 flex-wrap">
                                      <div className="flex items-center gap-2 min-w-0">
                                        <File className={`w-4 h-4 flex-shrink-0 ${isHighlighted ? 'text-brand-base' : 'text-gray-500'}`} />
                                        <span className="text-sm font-medium text-gray-900 truncate">{file.filename}</span>
                                      </div>
                                      {file.isPushed ? (
                                        <div className="bg-brand-base text-white px-2.5 py-0.5 rounded-md text-xs font-medium">
                                          Latest pushed
                                        </div>
                                      ) : file.status === 'running' ? (
                                        <div className="bg-yellow-100 text-yellow-800 px-2.5 py-0.5 rounded-md text-xs font-medium">
                                          Being processed
                                        </div>
                                      ) : file.status && file.status !== 'success' ? (
                                        <div className="bg-red-100 text-red-800 px-2.5 py-0.5 rounded-md text-xs font-medium">
                                          {file.status.charAt(0).toUpperCase() + file.status.slice(1)}
                                        </div>
                                      ) : null}
                                    </div>
                                  </td>
                                  <td className="px-4 py-4">
                                    <span className="text-sm text-gray-900">{file.timestamp}</span>
                                  </td>
                                  <td className={`px-4 py-4 ${isHighlighted ? 'border-r-2 border-brand-base' : ''}`}>
                                    <div className="flex justify-center gap-2">
                                      {file.logFilePath && (
                                        <button
                                          onClick={() => onPreviewJson(file.logFilePath!.split('/').pop() || 'log.txt', file.logFilePath!)}
                                          className="bg-white border border-gray-200 text-gray-900 px-3 py-1.5 rounded-lg text-xs font-medium hover:bg-gray-50 flex items-center gap-2 transition-colors"
                                          title="Log"
                                        >
                                          <FileText className="w-3.5 h-3.5" />
                                          <span className="hidden md:inline">Log</span>
                                        </button>
                                      )}
                                      <button
                                        onClick={() => onPreviewJson(file.filename, file.fullPath)}
                                        className="bg-white border border-gray-200 text-gray-900 px-3 py-1.5 rounded-lg text-xs font-medium hover:bg-gray-50 flex items-center gap-2 transition-colors"
                                        title="Preview"
                                      >
                                        <Eye className="w-3.5 h-3.5" />
                                        <span className="hidden md:inline">Preview</span>
                                      </button>
                                      <button
                                        onClick={() => onDownload(file.filename, file.fullPath)}
                                        className="bg-white border border-gray-200 text-gray-900 px-3 py-1.5 rounded-lg text-xs font-medium hover:bg-gray-50 flex items-center gap-2 transition-colors"
                                        title="Download"
                                      >
                                        <Download className="w-3.5 h-3.5" />
                                        <span className="hidden md:inline">Download</span>
                                      </button>
                                    </div>
                                  </td>
                                </tr>
                              )
                            })}
                          </tbody>
                        </table>
                      </div>

                    </div>
                  </div>
                </div>
                  </motion.div>
                )}
              </AnimatePresence>
            </div>
          ))}
          </div>
        </div>
      </div>
    </section>
  )
}
