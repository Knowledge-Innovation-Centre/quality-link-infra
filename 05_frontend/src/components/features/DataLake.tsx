import { File, Eye, ChevronDown, HelpCircle, RotateCw } from 'lucide-react'
import { useState } from 'react'

interface DataFile {
  filename: string
  timestamp: string
  isPushed?: boolean
  pushDate?: string
}

interface DataLakeProps {
  files: DataFile[]
  latestFile?: DataFile
  onPreviewJson: (filename: string) => void
  onRefresh?: () => void
  isRefreshing?: boolean
}

export default function DataLake({ files, latestFile, onPreviewJson, onRefresh, isRefreshing = false }: DataLakeProps) {
  const [selectedDate, setSelectedDate] = useState('20 Sep 2025')
  
  return (
    <section className="flex flex-col gap-8">
      {/* Header */}
      <div className="bg-white flex flex-col gap-1 overflow-clip rounded-t-lg">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-2xl font-semibold text-gray-900">Datalakes</h2>
            <p className="text-sm text-gray-500">Lorem ipsum</p>
          </div>
          {onRefresh && (
            <button
              onClick={onRefresh}
              disabled={isRefreshing}
              className="bg-brand-base text-white px-5 py-2.5 rounded-lg text-sm font-medium hover:opacity-90 disabled:opacity-50 flex items-center gap-2 transition-opacity"
            >
              <RotateCw className={`w-3.5 h-3.5 ${isRefreshing ? 'animate-spin' : ''}`} />
              Refresh Data
            </button>
          )}
        </div>
      </div>

      {/* Main Content */}
      <div className="flex flex-col gap-12">
        {/* Datalake from date picker and table */}
        <div className="flex flex-col gap-3">
          {/* Date selector */}
          <div className="flex items-center gap-1.5">
            <span className="text-base font-semibold text-gray-900">Datalake from</span>
            <div className="relative">
              <select
                value={selectedDate}
                onChange={(e) => setSelectedDate(e.target.value)}
                className="appearance-none bg-gray-50 border border-gray-300 rounded-lg px-4 py-2 pr-8 text-sm text-gray-900 focus:outline-none focus:ring-2 focus:ring-brand-base focus:border-transparent cursor-pointer"
              >
                <option>20 Sep 2025</option>
                <option>19 Sep 2025</option>
                <option>18 Sep 2025</option>
              </select>
              <ChevronDown className="absolute right-2 top-1/2 -translate-y-1/2 w-3 h-3 text-gray-500 pointer-events-none" />
            </div>
          </div>

          {/* Table */}
          <div className="rounded-lg overflow-hidden">
            <table className="w-full">
              <thead>
                <tr className="bg-gray-50">
                  <th className="text-left px-4 py-4 text-xs font-semibold text-gray-500 uppercase border-t border-l border-gray-200 rounded-tl-lg">
                    FILE
                  </th>
                  <th className="text-left px-4 py-4 text-xs font-semibold text-gray-500 uppercase border-t border-gray-200 w-[260px]">
                    FETCHED
                  </th>
                  <th className="text-center px-4 py-4 text-xs font-semibold text-gray-500 uppercase border-t border-r border-gray-200 rounded-tr-lg">
                    ACTIONS
                  </th>
                </tr>
              </thead>
              <tbody>
                {files.map((file, index) => {
                  const isHighlighted = file.isPushed
                  return (
                    <tr key={index} className={isHighlighted ? 'bg-[#f2f7fb]' : 'bg-white'}>
                      {/* File column - left, top, bottom borders */}
                      <td 
                        className={`
                          px-4 py-4 relative
                          ${index === files.length - 1 ? 'rounded-bl-lg' : ''}
                        `}
                      >
                        <div 
                          className={`
                            absolute inset-0 pointer-events-none
                            ${isHighlighted 
                              ? 'border-brand-base border-l border-t border-b' 
                              : 'border-gray-200 border-l border-t'}
                            ${index === files.length - 1 ? 'rounded-bl-lg' : ''}
                          `}
                        />
                        <div className="flex items-center gap-4 relative">
                          <div className="flex items-center gap-2">
                            <File className={`w-4 h-4 ${isHighlighted ? 'text-brand-base' : 'text-gray-500'}`} />
                            <span className="text-sm font-medium text-gray-900">{file.filename}</span>
                          </div>
                          {file.isPushed && (
                            <div className="bg-brand-base text-white px-2.5 py-0.5 rounded-md text-xs font-medium">
                              This file was pushed on {file.pushDate}
                            </div>
                          )}
                        </div>
                      </td>
                      {/* Fetched column - top, bottom borders only */}
                      <td className="px-4 py-4 relative">
                        <div 
                          className={`
                            absolute inset-0 pointer-events-none
                            ${isHighlighted 
                              ? 'border-brand-base border-t border-b' 
                              : 'border-gray-200 border-t'}
                          `}
                        />
                        <span className="text-sm text-gray-900 relative">{file.timestamp}</span>
                      </td>
                      {/* Actions column - right, top, bottom borders */}
                      <td 
                        className={`
                          px-4 py-4 relative
                          ${index === files.length - 1 ? 'rounded-br-lg' : ''}
                        `}
                      >
                        <div 
                          className={`
                            absolute inset-0 pointer-events-none
                            ${isHighlighted 
                              ? 'border-brand-base border-r border-t border-b' 
                              : 'border-gray-200 border-r border-t'}
                            ${index === files.length - 1 ? 'rounded-br-lg' : ''}
                          `}
                        />
                        <div className="flex justify-center relative">
                          <button
                            onClick={() => onPreviewJson(file.filename)}
                            className="bg-white border border-gray-200 text-gray-900 px-3 py-1.5 rounded-lg text-xs font-medium hover:bg-gray-50 flex items-center gap-2 transition-colors"
                          >
                            <Eye className="w-3.5 h-3.5" />
                            Preview JSON
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
        {latestFile && (
          <div className="flex flex-col gap-2">
            <h3 className="text-base font-semibold text-gray-900">Latest pushed file</h3>
            <div className="border border-gray-200 rounded-lg overflow-hidden">
              <table className="w-full">
                <tbody>
                  <tr className="bg-white">
                    <td className="px-4 py-4 rounded-tl-lg rounded-bl-lg">
                      <div className="flex items-center gap-2">
                        <File className="w-4 h-4 text-brand-base" />
                        <span className="text-sm font-medium text-gray-900">{latestFile.filename}</span>
                      </div>
                    </td>
                    <td className="px-4 py-4 w-[260px]">
                      <span className="text-sm text-gray-900">{latestFile.timestamp}</span>
                    </td>
                    <td className="px-4 py-4 rounded-tr-lg rounded-br-lg">
                      <div className="flex justify-center">
                        <button
                          onClick={() => onPreviewJson(latestFile.filename)}
                          className="bg-white border border-gray-200 text-gray-900 px-3 py-1.5 rounded-lg text-xs font-medium hover:bg-gray-50 flex items-center gap-2 transition-colors"
                        >
                          <Eye className="w-3.5 h-3.5" />
                          Preview
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
    </section>
  )
}

