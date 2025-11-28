import { useState, useEffect, useCallback } from 'react';
import { providersApi } from '../api';
import type {
  Provider,
  GetAllProvidersParams,
  GetAllProvidersResponse,
  GetProviderResponse,
} from '../types';

// Hook for managing providers list
export function useProviders(initialParams: GetAllProvidersParams = {}) {
  const [providers, setProviders] = useState<Provider[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [pagination, setPagination] = useState({
    total: 0,
    page: 1,
    pageSize: 10,
    totalPages: 0,
  });

  const fetchProviders = useCallback(async (params: GetAllProvidersParams = {}) => {
    setLoading(true);
    setError(null);
    
    try {
      const response: GetAllProvidersResponse = await providersApi.getAllProviders({
        ...initialParams,
        ...params,
      });
      
      setProviders(response.response);
      setPagination({
        total: response.response.length,
        page: params.page || initialParams.page || 1,
        pageSize: params.page_size || initialParams.page_size || 10,
        totalPages: Math.ceil(response.response.length / (params.page_size || initialParams.page_size || 10)),
      });
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Failed to fetch providers';
      setError(errorMessage);
      console.error('Error fetching providers:', err);
    } finally {
      setLoading(false);
    }
  }, [initialParams]);

  const searchProviders = useCallback(async (searchTerm: string, page: number = 1) => {
    await fetchProviders({
      search_provider: searchTerm,
      page,
      page_size: pagination.pageSize,
    });
  }, [fetchProviders, pagination.pageSize]);

  const loadPage = useCallback(async (page: number) => {
    await fetchProviders({
      page,
      page_size: pagination.pageSize,
    });
  }, [fetchProviders, pagination.pageSize]);

  useEffect(() => {
    fetchProviders();
  }, [fetchProviders]);

  return {
    providers,
    loading,
    error,
    pagination,
    fetchProviders,
    searchProviders,
    loadPage,
    refetch: () => fetchProviders(),
  };
}

// Hook for managing a single provider
export function useProvider(providerUuid: string | null) {
  const [provider, setProvider] = useState<GetProviderResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchProvider = useCallback(async (uuid: string) => {
    setLoading(true);
    setError(null);
    
    try {
      const response = await providersApi.getProvider({ provider_uuid: uuid });
      setProvider(response);
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Failed to fetch provider';
      setError(errorMessage);
      console.error('Error fetching provider:', err);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (providerUuid) {
      fetchProvider(providerUuid);
    } else {
      setProvider(null);
      setError(null);
    }
  }, [providerUuid, fetchProvider]);

  return {
    provider,
    loading,
    error,
    refetch: providerUuid ? () => fetchProvider(providerUuid) : undefined,
  };
}
