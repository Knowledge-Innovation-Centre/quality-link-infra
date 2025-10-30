export interface Institution {
  id: string
  name: string
  identifiers: Identifier[]
  domains: Domain[]
}

export interface Identifier {
  type: string
  value: string
}

export interface Domain {
  domain: string
  methods: DomainMethod[]
}

export interface DomainMethod {
  type: 'DNS_TXT' | 'WELL_KNOWN'
  status: 'FOUND' | 'NOT_FOUND' | 'CHECKING' | 'VALID' | 'INVALID'
  message?: string
  manifestPath?: string
}

export interface DataFile {
  id: string
  filename: string
  timestamp: string
  status: 'active' | 'archived'
  size?: string
}

export interface LearningOpportunity {
  id: string
  name: string
  opportunityCount?: number
  previousCount?: number
}

export type BadgeVariant = 'default' | 'success' | 'warning' | 'error' | 'info'
export type ButtonVariant = 'primary' | 'secondary' | 'outline' | 'ghost'
export type ButtonSize = 'sm' | 'md' | 'lg'

// Provider API Types
export interface Provider {
  provider_uuid: string
  provider_name: string
  deqar_id: string
  eter_id: number
}

export interface GetAllProvidersParams {
  search_provider?: string
  page?: number
  page_size?: number
}

export interface GetAllProvidersResponse {
  response: Provider[]
  total: number
  page: number
  page_size: number
  total_pages: number
}

export interface GetProviderParams {
  provider_uuid: string
}

export interface ProviderLocation {
  lat: number
  city: string
  long: number
  country: {
    id: number
    name_english: string
    ehea_is_member: boolean
    iso_3166_alpha2: string
    iso_3166_alpha3: string
  }
  country_valid_to: string | null
  country_verified: boolean
  country_valid_from: string
}

export interface ProviderName {
  acronym: string
  name_english: string
  name_official: string
  name_valid_to: string | null
  name_versions: any[]
  name_official_transliterated: string
}

export interface ProviderIdentifier {
  agency: string | null
  resource: string
  identifier: string
}

export interface ProviderMetadata {
  id: number
  city: string[]
  names: ProviderName[]
  country: string[]
  eter_id: string
  part_of: any[]
  deqar_id: string
  includes: any[]
  locations: ProviderLocation[]
  identifiers: ProviderIdentifier[]
  closure_date: string | null
  date_created: string
  name_primary: string
  website_link: string
  founding_date: string
  is_other_provider: boolean
  organization_type: string | null
  permalink?: string
  qf_ehea_levels?: string[]
}

export interface ManifestMethod {
  type: string
  domain: string | null
}

export interface SourceJson {
  path: string
  type: string
  version: string
  source_uuid?: string
}

export interface ProviderDetails {
  provider_uuid: string
  deqar_id: string
  eter_id: number
  metadata: ProviderMetadata
  manifest_json: ManifestMethod[]
  name_concat: string
  provider_name: string
  last_deqar_pull: string | null
  last_manifest_pull: string | null
  created_at: string
  updated_at: string
}

export interface SourceVersion {
  source_version_uuid: string
  provider_uuid: string
  version_date: string
  version_id: number
  source_json: SourceJson[]
  source_uuid_json: SourceJson[]
  created_at: string
  updated_at: string
}

export interface Source {
  source_uuid: string
  source_version_uuid: string
  source_path: string
  source_name?: string
  source_type: string
  source_version: string
  created_at: string
  updated_at: string
}

export interface GetProviderResponse {
  provider: ProviderDetails
  source_version: SourceVersion
  sources: Source[]
}

export interface DatalakeFile {
  full_path: string
  filename: string
  size: number
  last_modified: string
}

export interface GetDatalakeFilesParams {
  provider_uuid: string
  source_version_uuid: string
  source_uuid: string
  source_path: string
}

export interface GetDatalakeFilesResponse {
  status: string
  message: string
  params: {
    provider_uuid: string
    source_version_uuid: string
    source_uuid: string
    date: string
    date_source: string
  }
  files: DatalakeFile[]
  count: number
}

export interface PullManifestParams {
  provider_uuid: string
}

export interface PullManifestResponse {
  status: string
  provider_uuid: string
  domain: string
  manifest_url: string | null
  manifest_found: boolean
  manifest_json: ManifestMethod[]
  sources_processed: boolean
  new_source_version_created: boolean
}


