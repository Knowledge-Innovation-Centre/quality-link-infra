import { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import { providersApi } from '../api'
import type { GetProviderResponse, DatalakeFile } from '../types'
import Navbar from '@/components/layout/Navbar'
import Footer from '@/components/layout/Footer'
import InstitutionHero from '@/components/features/InstitutionHero'
import Badge from '@/components/ui/Badge'
import Modal from '@/components/ui/Modal'
import DomainVerification from '@/components/features/DomainVerification'
import DataSources from '@/components/features/DataSources'
import { useToast } from '@/components/ui/Toast'
import { HelpCircle } from 'lucide-react'

export default function DashboardPage() {
  const { providerUuid } = useParams()
  const { showToast, updateToast } = useToast()
  const [isJsonModalOpen, setIsJsonModalOpen] = useState(false)
  const [selectedJson, setSelectedJson] = useState('')
  const [isRefreshing, setIsRefreshing] = useState(false)
  const [isDataLakeRefreshing, setIsDataLakeRefreshing] = useState(false)
  const [providerData, setProviderData] = useState<GetProviderResponse | null>(null)
  const [providerLoading, setProviderLoading] = useState(true)
  const [providerError, setProviderError] = useState<string | null>(null)
  const [datalakeFiles, setDatalakeFiles] = useState<Record<string, DatalakeFile[]>>({})

  // Fetch provider data on component mount
  useEffect(() => {
    const fetchProviderData = async () => {
      if (!providerUuid) {
        setProviderError('No provider ID provided')
        setProviderLoading(false)
        return
      }

      try {
        setProviderLoading(true)
        setProviderError(null)
        const response = await providersApi.getProvider({ provider_uuid: providerUuid })
        setProviderData(response)

        // Fetch datalake files for each source
        const filesPromises = response.sources.map(async (source) => {
          try {
            const filesResponse = await providersApi.getDatalakeFiles({
              provider_uuid: response.provider.provider_uuid,
              source_version_uuid: response.source_version.source_version_uuid,
              source_uuid: source.source_uuid,
            })
            return { sourceUuid: source.source_uuid, files: filesResponse.files }
          } catch (err) {
            console.error(`Error fetching datalake files for source ${source.source_uuid}:`, err)
            return { sourceUuid: source.source_uuid, files: [] }
          }
        })

        const filesResults = await Promise.all(filesPromises)
        const filesMap: Record<string, DatalakeFile[]> = {}
        filesResults.forEach(result => {
          filesMap[result.sourceUuid] = result.files
        })
        setDatalakeFiles(filesMap)
      } catch (err) {
        setProviderError(err instanceof Error ? err.message : 'Failed to fetch provider data')
        console.error('Error fetching provider:', err)
      } finally {
        setProviderLoading(false)
      }
    }

    fetchProviderData()
  }, [providerUuid])

  // Generate identifiers from provider data
  const identifiers = providerData?.provider.metadata.identifiers.map(id =>
    `${id.resource}: ${id.identifier}`
  ) || []

  // Map manifest_json to domain verification methods
  const websiteDomain = providerData?.provider.metadata.website_link
    ? new URL(providerData.provider.metadata.website_link).hostname
    : 'N/A'

  const domainMethods = providerData?.provider.manifest_json.map(manifest => {
    const hasManifest = manifest.domain !== null
    return {
      domain: websiteDomain,
      method: manifest.type === '.well-known' ? '.well-known' : 'DNS TXT',
      status: hasManifest ? 'valid' as const : 'not_found' as const,
      message: hasManifest ? 'Manifest found' : 'Manifest not found',
      manifestPath: hasManifest && manifest.type === '.well-known'
        ? `${providerData.provider.metadata.website_link}.well-known/${manifest.domain}`
        : undefined,
    }
  }) || []

  const handleRefreshDiscovery = () => {
    setIsRefreshing(true)
    const loadingToastId = showToast({
      type: 'loading',
      title: 'Refreshing...',
      message: 'Looking for the manifest file...',
      isLoading: true,
      showProgress: true,
      progress: 0,
    })
    
    // Simulate API call with progress updates
    let progress = 0
    let hasCompleted = false
    
    const progressInterval = setInterval(() => {
      if (hasCompleted) return
      
      if (progress < 90) {
        progress += Math.random() * 15
        if (progress > 90) progress = 90 // Cap at 90%
        updateToast(loadingToastId, { progress })
      } else if (progress >= 90 && !hasCompleted) {
        hasCompleted = true
        clearInterval(progressInterval)
        
        // Complete to 100%
        progress = 100
        updateToast(loadingToastId, { progress })
        
        // Wait a moment, then transform to success view
        setTimeout(() => {
          setIsRefreshing(false)
          updateToast(loadingToastId, {
            isLoading: false,
            isComplete: true,
            title: 'Refresh complete',
            message: 'Domain verification completed successfully',
          })
        }, 500)
      }
    }, 200)
  }

  const handleRefreshDataLake = () => {
    setIsDataLakeRefreshing(true)
    const loadingToastId = showToast({
      type: 'loading',
      title: 'Refreshing...',
      message: 'Fetching latest files from data sources...',
      isLoading: true,
      showProgress: true,
      progress: 0,
    })
    
    // Simulate API call with progress updates
    let progress = 0
    let hasCompleted = false
    
    const progressInterval = setInterval(() => {
      if (hasCompleted) return
      
      if (progress < 90) {
        progress += Math.random() * 15
        if (progress > 90) progress = 90 // Cap at 90%
        updateToast(loadingToastId, { progress })
      } else if (progress >= 90 && !hasCompleted) {
        hasCompleted = true
        clearInterval(progressInterval)
        
        // Complete to 100%
        progress = 100
        updateToast(loadingToastId, { progress })
        
        // Wait a moment, then transform to success view
        setTimeout(() => {
          setIsDataLakeRefreshing(false)
          updateToast(loadingToastId, {
            isLoading: false,
            isComplete: true,
            title: 'Refresh complete',
            message: 'Data sources refreshed successfully',
          })
        }, 500)
      }
    }, 200)
  }

  const handleViewJson = async (filename: string, sourcePath?: string) => {
    setSelectedJson(filename)
    setIsJsonModalOpen(true)

    if (sourcePath) {
      const loadingToastId = showToast({
        type: 'loading',
        title: 'Loading data',
        message: `Fetching ${filename}...`,
        isLoading: true,
      })

      try {
        const response = await fetch(sourcePath)
        const data = await response.text()
        setSelectedJson(data)
        updateToast(loadingToastId, {
          type: 'success',
          title: 'Data loaded',
          message: 'File content retrieved successfully',
          isLoading: false,
        })
      } catch (error) {
        updateToast(loadingToastId, {
          type: 'error',
          title: 'Failed to load',
          message: error instanceof Error ? error.message : 'Could not fetch file content',
          isLoading: false,
        })
        setSelectedJson(`Error loading file: ${error instanceof Error ? error.message : 'Unknown error'}`)
      }
    } else {
      showToast({
        type: 'info',
        title: 'No source path',
        message: 'Source path not available for this file',
      })
    }
  }

  // Map sources from API to data sources format
  const dataSources = providerData?.sources.map(source => {
    const sourceUrl = new URL(source.source_path)
    const sourceName = sourceUrl.pathname.split('/').pop() || source.source_path
    const createdDate = new Date(source.created_at)

    // Get datalake files for this source
    const sourceFiles = datalakeFiles[source.source_uuid] || []

    // Map datalake files to the expected format
    const mappedFiles = sourceFiles.map(file => {
      const fileDate = new Date(file.last_modified)
      return {
        filename: file.filename,
        timestamp: fileDate.toLocaleDateString('en-GB', {
          day: 'numeric',
          month: 'short',
          year: 'numeric',
          hour: '2-digit',
          minute: '2-digit'
        }),
        isPushed: true,
        pushDate: fileDate.toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' }),
      }
    })

    // Get latest file
    const latestFile = sourceFiles.length > 0
      ? sourceFiles.sort((a, b) => new Date(b.last_modified).getTime() - new Date(a.last_modified).getTime())[0]
      : null

    return {
      id: source.source_uuid,
      name: sourceName,
      type: source.source_type.toUpperCase(),
      pushed: createdDate.toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' }),
      latestFile: latestFile?.filename || sourceName,
      files: mappedFiles.length > 0 ? mappedFiles : [],
      latestPushedFile: latestFile ? {
        filename: latestFile.filename,
        timestamp: `Pushed ${new Date(latestFile.last_modified).toLocaleDateString('en-GB', {
          day: 'numeric',
          month: 'short',
          year: 'numeric',
          hour: '2-digit',
          minute: '2-digit'
        })}`,
      } : undefined,
      sourcePath: source.source_path,
    }
  }) || []

  return (
    <div className="min-h-screen bg-white flex flex-col">
      <Navbar />

      {/* Hero Section */}
      {providerLoading ? (
        <div className="bg-gray-100 py-16 flex justify-center">
          <div className="text-gray-600">Loading provider information...</div>
        </div>
      ) : providerError ? (
        <div className="bg-red-50 py-16 flex justify-center">
          <div className="text-red-600">Error: {providerError}</div>
        </div>
      ) : providerData ? (
        <InstitutionHero
          name={providerData.provider.provider_name}
          nameLocal={providerData.provider.metadata.names[0]?.name_official || providerData.provider.provider_name}
          website={providerData.provider.metadata.website_link}
          lastUpdated={providerData.provider.last_manifest_pull
            ? new Date(providerData.provider.last_manifest_pull).toLocaleDateString('en-GB', {
                day: '2-digit',
                month: '2-digit',
                year: 'numeric',
                hour: '2-digit',
                minute: '2-digit'
              })
            : new Date(providerData.provider.created_at).toLocaleDateString('en-GB', {
                day: '2-digit',
                month: '2-digit',
                year: 'numeric',
                hour: '2-digit',
                minute: '2-digit'
              })
          }
        />
      ) : null}

      {/* Main Content */}
      <main className="flex-1 bg-white">
        <div className="flex flex-col items-center px-20 pt-[60px] pb-20">
          <div className="w-[1000px] space-y-8">
            {/* Known Identifiers */}
            <section>
              <div className="mb-6">
                <h2 className="text-2xl font-bold text-gray-900 mb-2">
                  Known identifiers
                </h2>
                <p className="text-gray-600">
                  Official codes and identifiers used to reference this institution in other databases
                </p>
              </div>
              <div className="flex flex-wrap gap-3">
                {identifiers.map((identifier, index) => (
                  <Badge key={index} variant="default">
                    {identifier}
                  </Badge>
                ))}
              </div>
              <div className="mt-4 flex items-start gap-2">
                <HelpCircle className="w-5 h-5 text-turquoise-base flex-shrink-0 mt-0.5" />
                <p className="text-sm text-gray-600">
                  Do you notice any errors? Visit the{' '}
                  <Link to="/help" className="text-turquoise-600 hover:underline">
                    help page
                  </Link>{' '}
                  for information where we source this data from and how to report errors.
                </p>
              </div>
            </section>

            <hr className="border-gray-200" />

            {/* Domain Verification */}
            <DomainVerification
              methods={domainMethods}
              onRefresh={handleRefreshDiscovery}
              isRefreshing={isRefreshing}
            />

            <hr className="border-gray-200" />

            {/* Data Sources */}
            <DataSources
              dataSources={dataSources}
              onPreviewJson={handleViewJson}
              onRefresh={handleRefreshDataLake}
              isRefreshing={isDataLakeRefreshing}
            />
          </div>
        </div>
      </main>

      <Footer />

      {/* JSON Preview Modal */}
      <Modal
        isOpen={isJsonModalOpen}
        onClose={() => setIsJsonModalOpen(false)}
        title="Source File Preview"
        size="xl"
      >
        <div className="bg-gray-900 text-gray-100 rounded-lg p-6 overflow-x-auto max-h-[600px]">
          <pre className="text-sm font-mono whitespace-pre-wrap break-words">{selectedJson}</pre>
        </div>
      </Modal>
    </div>
  )
}


