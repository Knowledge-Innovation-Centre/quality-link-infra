import { apiClient } from './client';
import { API_CONFIG } from './config';
import type {
  GetAllProvidersParams,
  GetAllProvidersResponse,
  GetProviderParams,
  GetProviderResponse,
  GetDatalakeFilesParams,
  GetDatalakeFilesResponse,
  PullManifestParams,
  PullManifestResponse,
} from '../types';

/**
 * Provider API Service
 * Handles all provider-related API calls
 */
export class ProvidersApi {
  private basePath = API_CONFIG.ENDPOINTS.QUALITY_LINK;

  /**
   * Get all providers with optional search and pagination
   * @param params - Search and pagination parameters
   * @returns Promise with providers list
   */
  async getAllProviders(params: GetAllProvidersParams = {}): Promise<GetAllProvidersResponse> {
    const endpoint = `${this.basePath}/get_all_providers`;
    
    // Set default values
    const queryParams = {
      page: 1,
      page_size: 10,
      ...params,
    };

    return apiClient.get<GetAllProvidersResponse>(endpoint, queryParams);
  }

  /**
   * Get a specific provider by UUID
   * @param params - Provider UUID parameter
   * @returns Promise with provider details
   */
  async getProvider(params: GetProviderParams): Promise<GetProviderResponse> {
    const endpoint = `${this.basePath}/get_provider`;
    
    return apiClient.get<GetProviderResponse>(endpoint, {
      provider_uuid: params.provider_uuid,
    });
  }

  /**
   * Search providers with a search term
   * @param searchTerm - The search term to filter providers
   * @param page - Page number (default: 1)
   * @param pageSize - Number of items per page (default: 10)
   * @returns Promise with filtered providers
   */
  async searchProviders(
    searchTerm: string,
    page: number = 1,
    pageSize: number = 10
  ): Promise<GetAllProvidersResponse> {
    return this.getAllProviders({
      search_provider: searchTerm,
      page,
      page_size: pageSize,
    });
  }

  /**
   * Get datalake files for a specific source
   * @param params - Provider UUID, source version UUID, source UUID, and source path
   * @returns Promise with datalake files list
   */
  async getDatalakeFiles(params: GetDatalakeFilesParams): Promise<GetDatalakeFilesResponse> {
    const endpoint = `${this.basePath}/list_datalake_files`;

    return apiClient.get<GetDatalakeFilesResponse>(endpoint, {
      provider_uuid: params.provider_uuid,
      source_version_uuid: params.source_version_uuid,
      source_uuid: params.source_uuid,
      source_path: params.source_path,
    });
  }

  /**
   * Pull and refresh the manifest for a provider
   * @param params - Provider UUID
   * @returns Promise with pull manifest results
   */
  async pullManifest(params: PullManifestParams): Promise<PullManifestResponse> {
    const endpoint = `${this.basePath}/pull_manifest?provider_uuid=${params.provider_uuid}`;

    return apiClient.post<PullManifestResponse>(endpoint);
  }

  /**
   * Queue provider data for fetching from source
   * @param params - Provider UUID, source version UUID, source UUID, and source path
   * @returns Promise with queue response
   */
  async queueProviderData(params: {
    provider_uuid: string;
    source_version_uuid: string;
    source_uuid: string;
    source_path: string;
  }): Promise<{ status: string; message: string }> {
    const queryParams = new URLSearchParams({
      provider_uuid: params.provider_uuid,
      source_version_uuid: params.source_version_uuid,
      source_uuid: params.source_uuid,
      source_path: params.source_path,
    });
    const endpoint = `${this.basePath}/queue_provider_data?${queryParams.toString()}`;

    return apiClient.post<{ status: string; message: string }>(endpoint);
  }
}

// Export singleton instance
export const providersApi = new ProvidersApi();
