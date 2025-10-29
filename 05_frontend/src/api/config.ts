// API Configuration
export const API_CONFIG = {
  // In development, use proxy to bypass CORS
  // In production, use direct API URL
  BASE_URL: import.meta.env.DEV
    ? '/api'
    : 'https://i08ggsggwokooc84coo4c08o.serverfarm.knowledgeinnovation.eu',
  ENDPOINTS: {
    QUALITY_LINK: '',
  },
  DEFAULT_HEADERS: {
    'accept': 'application/json',
    'Content-Type': 'application/json',
  },
} as const;

// API Response wrapper type
export interface ApiResponse<T> {
  data: T;
  success: boolean;
  message?: string;
}

// Error handling
export class ApiError extends Error {
  constructor(
    message: string,
    public status?: number,
    public response?: any
  ) {
    super(message);
    this.name = 'ApiError';
  }
}
