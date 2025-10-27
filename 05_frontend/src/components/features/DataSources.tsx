import { File, Eye, ChevronDown, ChevronUp, HelpCircle, RotateCw } from 'lucide-react'
import { useState } from 'react'

interface DataFile {
  filename: string
  timestamp: string
  isPushed?: boolean
  pushDate?: string
}

interface DataSource {
  id: string
  name: string
  type: string
  pushed: string
  latestFile: string
  files: DataFile[]
  latestPushedFile?: DataFile
  sourcePath?: string
}

interface DataSourcesProps {
  dataSources: DataSource[]
  onPreviewJson: (filename: string, sourcePath?: string) => void
  onRefresh?: () => void
  isRefreshing?: boolean
}

export default function DataSources({
  dataSources,
  onPreviewJson,
  onRefresh,
  isRefreshing = false
}: DataSourcesProps) {
  const [expandedSources, setExpandedSources] = useState<Set<string>>(new Set())
  const [selectedDates, setSelectedDates] = useState<Record<string, string>>({})

  // Truncate UUID for display
  const truncateUuid = (uuid: string) => {
    return uuid.length > 8 ? `${uuid.substring(0, 8)}...` : uuid
  }

  const toggleExpand = (id: string) => {
    setExpandedSources(prev => {
      const newSet = new Set(prev)
      if (newSet.has(id)) {
        newSet.delete(id)
      } else {
        newSet.add(id)
      }
      return newSet
    })
  }

  const isExpanded = (id: string) => expandedSources.has(id)

  const getSelectedDate = (id: string) => selectedDates[id] || 'Today'

  const setDateForSource = (id: string, date: string) => {
    setSelectedDates(prev => ({ ...prev, [id]: date }))
  }

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
        <div className="border border-gray-200 rounded-lg overflow-hidden">
          {/* Table Header */}
          <div className="flex bg-gray-50">
            <div className="px-4 py-4 w-[140px]">
              <p className="text-xs font-semibold text-gray-500 uppercase">ID</p>
            </div>
            <div className="px-4 py-4 w-[100px]">
              <p className="text-xs font-semibold text-gray-500 uppercase">Type</p>
            </div>
            <div className="px-4 py-4 w-[134px]">
              <p className="text-xs font-semibold text-gray-500 uppercase">PUSHED</p>
            </div>
            <div className="flex-1 px-4 py-4">
              <p className="text-xs font-semibold text-gray-500 uppercase">latest pushed file</p>
            </div>
          </div>

          {/* Table Body */}
          {dataSources.map((source, index) => (
            <div key={source.id}>
              {/* Main Row */}
              <div className={`flex items-center border-t border-gray-200 ${isExpanded(source.id) ? 'bg-gray-50' : 'bg-white'}`}>
                <div className="px-4 py-4 w-[140px]">
                  <p className="text-sm text-gray-900" title={source.id}>
                    {truncateUuid(source.id)}
                  </p>
                </div>
                <div className="px-4 py-4 w-[100px]">
                  <div className="bg-gray-100 px-2.5 py-0.5 rounded-md inline-block">
                    <p className="text-sm font-semibold text-gray-800">{source.type}</p>
                  </div>
                </div>
                <div className="px-4 py-4 w-[134px]">
                  <p className="text-sm text-gray-900">{source.pushed}</p>
                </div>
                <div className="flex-1 px-4 py-4 flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <File className="w-4 h-4 text-gray-500" />
                    <p className="text-sm font-medium text-gray-900">{source.latestFile}</p>
                  </div>
                  <button
                    onClick={() => toggleExpand(source.id)}
                    className="p-0.5 hover:bg-gray-200 rounded transition-colors"
                  >
                    {isExpanded(source.id) ? (
                      <ChevronUp className="w-3.5 h-3.5 text-gray-600" />
                    ) : (
                      <ChevronDown className="w-3.5 h-3.5 text-gray-600" />
                    )}
                  </button>
                </div>
              </div>

              {/* Expanded Content */}
              {isExpanded(source.id) && (
                <div className="bg-gray-50 border-t border-gray-200 px-4 py-4">
                  <div className="bg-white border border-gray-200 rounded-lg p-4 flex flex-col gap-8">
                    {/* Datalake Section */}
                    <div className="flex flex-col gap-3">
                      {/* Date selector and refresh button */}
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-1.5">
                          <span className="text-sm text-gray-900">Datalake from</span>
                          <div className="relative">
                            <button className="flex items-center gap-2 px-3 py-1 text-sm font-semibold text-brand-base hover:bg-brand-50 rounded transition-colors">
                              <span>{getSelectedDate(source.id)}</span>
                              <ChevronDown className="w-3 h-3" />
                            </button>
                          </div>
                        </div>
                        {onRefresh && (
                          <button
                            onClick={onRefresh}
                            disabled={isRefreshing}
                            className="bg-brand-base text-white px-3 py-2 rounded-lg text-xs font-medium hover:opacity-90 disabled:opacity-50 flex items-center gap-2 transition-opacity"
                          >
                            <RotateCw className={`w-3.5 h-3.5 ${isRefreshing ? 'animate-spin' : ''}`} />
                            Refetch data source
                          </button>
                        )}
                      </div>

                      {/* Files Table */}
                      <div className="rounded-lg overflow-hidden border border-gray-200">
                        <table className="w-full">
                          <thead>
                            <tr className="bg-white">
                              <th className="text-left px-4 py-4 text-xs font-semibold text-gray-500 uppercase border-b border-gray-200">
                                FILE
                              </th>
                              <th className="text-left px-4 py-4 text-xs font-semibold text-gray-500 uppercase border-b border-gray-200 w-[260px]">
                                FETCHED
                              </th>
                              <th className="text-center px-4 py-4 text-xs font-semibold text-gray-500 uppercase border-b border-gray-200">
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
                                    <div className="flex items-center gap-4">
                                      <div className="flex items-center gap-2">
                                        <File className={`w-4 h-4 ${isHighlighted ? 'text-brand-base' : 'text-gray-500'}`} />
                                        <span className="text-sm font-medium text-gray-900">{file.filename}</span>
                                      </div>
                                      {file.isPushed && file.pushDate && (
                                        <div className="bg-brand-base text-white px-2.5 py-0.5 rounded-md text-xs font-medium">
                                          This file will be pushed tonight at 2 AM
                                        </div>
                                      )}
                                    </div>
                                  </td>
                                  <td className="px-4 py-4">
                                    <span className="text-sm text-gray-900">{file.timestamp}</span>
                                  </td>
                                  <td className={`px-4 py-4 ${isHighlighted ? 'border-r-2 border-brand-base' : ''}`}>
                                    <div className="flex justify-center">
                                      <button
                                        onClick={() => onPreviewJson(file.filename, source.sourcePath)}
                                        className="bg-white border border-gray-200 text-gray-900 px-3 py-1.5 rounded-lg text-xs font-medium hover:bg-gray-50 flex items-center gap-2 transition-colors"
                                      >
                                        <Eye className="w-3.5 h-3.5" />
                                        Preview
                                      </button>
                                    </div>
                                  </td>
                                </tr>
                              )
                            })}
                          </tbody>
                        </table>
                      </div>

                      {/* Info message */}
                      <div className="flex items-center gap-2">
                        <HelpCircle className="w-4.5 h-4.5 text-gray-500 flex-shrink-0" />
                        <p className="text-sm text-gray-500">
                          The latest fetched file gets pushed at 2 AM each day. Other files get archived.
                        </p>
                      </div>
                    </div>

                    {/* Latest pushed file */}
                    {source.latestPushedFile && (
                      <div className="flex flex-col gap-2">
                        <h3 className="text-sm text-gray-900">Latest pushed file</h3>
                        <div className="border border-gray-200 rounded-lg overflow-hidden">
                          <table className="w-full">
                            <tbody>
                              <tr className="bg-white">
                                <td className="px-4 py-4">
                                  <div className="flex items-center gap-2">
                                    <File className="w-4 h-4 text-brand-base" />
                                    <span className="text-sm font-medium text-gray-900">{source.latestPushedFile.filename}</span>
                                  </div>
                                </td>
                                <td className="px-4 py-4 w-[260px]">
                                  <span className="text-sm text-gray-900">{source.latestPushedFile.timestamp}</span>
                                </td>
                                <td className="px-4 py-4">
                                  <div className="flex justify-center">
                                    <button
                                      onClick={() => onPreviewJson(source.latestPushedFile!.filename, source.sourcePath)}
                                      className="bg-white border border-gray-200 text-gray-900 px-3 py-1.5 rounded-lg text-xs font-medium hover:bg-gray-50 flex items-center gap-2 transition-colors"
                                    >
                                      <Eye className="w-3.5 h-3.5" />
                                      Preview JSON
                                    </button>
                                  </div>
                                </td>
                              </tr>
                            </tbody>
                          </table>
                        </div>
                      </div>
                    )}
                  </div>
                </div>
              )}
            </div>
          ))}
        </div>

        {/* Pagination Footer */}
        <div className="bg-white flex items-center justify-between py-2">
          <div>
            <p className="text-sm text-gray-500">
              Showing <span className="font-semibold text-gray-900">1-{dataSources.length}</span> of{' '}
              <span className="font-semibold text-gray-900">{dataSources.length}</span>
            </p>
          </div>
          <div className="flex items-center gap-2">
            <button className="bg-white border border-gray-200 text-turquoise-900 px-3 py-1.5 rounded-lg text-xs font-medium hover:bg-gray-50 transition-colors">
              Previous
            </button>
            <p className="text-xs text-turquoise-900">
              Page <span className="font-semibold">1</span> of <span className="font-semibold">1</span>
            </p>
            <button className="bg-white border border-gray-200 text-turquoise-900 px-3 py-1.5 rounded-lg text-xs font-medium hover:bg-gray-50 transition-colors">
              Next
            </button>
          </div>
        </div>
      </div>
    </section>
  )
}
