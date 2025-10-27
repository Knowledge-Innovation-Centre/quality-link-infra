import { apiClient } from './client';
import { API_CONFIG } from './config';
import type {
  GetAllProvidersParams,
  GetAllProvidersResponse,
  GetProviderParams,
  GetProviderResponse,
  GetDatalakeFilesParams,
  GetDatalakeFilesResponse,
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
   * @param params - Provider UUID, source version UUID, and source UUID
   * @returns Promise with datalake files list
   */
  async getDatalakeFiles(params: GetDatalakeFilesParams): Promise<GetDatalakeFilesResponse> {
    const endpoint = `${this.basePath}/list_datalake_files`;

    return apiClient.get<GetDatalakeFilesResponse>(endpoint, {
      provider_uuid: params.provider_uuid,
      source_version_uuid: params.source_version_uuid,
      source_uuid: params.source_uuid,
    });
  }
}

// Export singleton instance
export const providersApi = new ProvidersApi();
