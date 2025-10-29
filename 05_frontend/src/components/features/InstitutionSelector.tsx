import { useState, useEffect, useRef } from "react";
import { providersApi } from "../../api";
import Spinner from "../ui/Spinner";
import type { Provider } from "../../types";

interface InstitutionSelectorProps {
  onSelect: (providerUuid: string) => void;
}

export default function InstitutionSelector({ onSelect }: InstitutionSelectorProps) {
  const [searchTerm, setSearchTerm] = useState("");
  const [selectedProvider, setSelectedProvider] = useState<Provider | null>(null);
  const [captchaVerified, setCaptchaVerified] = useState(false);
  const [providers, setProviders] = useState<Provider[]>([]);
  const [loading, setLoading] = useState(false);
  const [showDropdown, setShowDropdown] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const dropdownRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  // Debounced search effect
  useEffect(() => {
    if (searchTerm.length < 2) {
      setProviders([]);
      setShowDropdown(false);
      return;
    }

    // Don't search if we have a selected provider and the search term matches it
    if (selectedProvider && searchTerm === selectedProvider.provider_name) {
      return;
    }

    const timeoutId = setTimeout(async () => {
      setLoading(true);
      setError(null);

      try {
        const response = await providersApi.searchProviders(searchTerm, 1, 20);
        setProviders(response.response);
        setShowDropdown(true);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Search failed");
        setProviders([]);
        setShowDropdown(false);
      } finally {
        setLoading(false);
      }
    }, 300); // 300ms debounce

    return () => clearTimeout(timeoutId);
  }, [searchTerm, selectedProvider]);

  // Close dropdown when clicking outside
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setShowDropdown(false);
      }
    };

    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  const handleProviderSelect = (provider: Provider) => {
    setSelectedProvider(provider);
    setSearchTerm(provider.provider_name);
    setShowDropdown(false);
  };

  const handleSubmit = () => {
    if (selectedProvider && captchaVerified) {
      onSelect(selectedProvider.provider_uuid);
    }
  };

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    setSearchTerm(value);

    // Clear selected provider if user types something different
    if (selectedProvider && value !== selectedProvider.provider_name) {
      setSelectedProvider(null);
    }
  };

  const handleInputFocus = () => {
    if (providers.length > 0) {
      setShowDropdown(true);
    }
  };

  return (
    <div className="flex flex-col gap-6">
      <div className="flex flex-col gap-4">
        <div>
          <h2 className="text-3xl font-bold text-gray-900 mb-4">Enter the dashboard</h2>
          <p className="text-gray-700 leading-relaxed">This dashboard allows you to check if your education institution's data sources were discovered and harvested by the QualityLink aggregator.</p>
        </div>

        {/* reCAPTCHA placeholder */}
        <div className="border-2 border-gray-300 rounded-lg p-4 bg-white shadow-sm inline-block">
          <label className="flex items-center gap-3 cursor-pointer">
            <div className="relative">
              <input
                type="checkbox"
                checked={captchaVerified}
                onChange={(e) => setCaptchaVerified(e.target.checked)}
                className="w-6 h-6 rounded border-2 border-gray-400 cursor-pointer accent-turquoise-base hover:border-turquoise-base transition-colors"
              />
            </div>
            <span className="text-gray-800 font-medium select-none">I'm not a robot</span>
          </label>
        </div>

        <div className="flex flex-col gap-3">
          <label className="text-base font-medium text-gray-900">Select your institution to start:</label>

          {/* Search Input with Dropdown */}
          <div className="relative" ref={dropdownRef}>
            <div className="flex gap-4">
              <div className="flex-1 relative">
                <input
                  ref={inputRef}
                  type="text"
                  placeholder="Search for your institution..."
                  value={searchTerm}
                  onChange={handleInputChange}
                  onFocus={handleInputFocus}
                  className="w-full px-4 py-2.5 rounded-lg border border-gray-200 focus:outline-none focus:ring-2 focus:ring-turquoise-base pr-10"
                />

                {/* Loading spinner */}
                {loading && (
                  <div className="absolute right-3 top-1/2 transform -translate-y-1/2">
                    <Spinner size="sm" />
                  </div>
                )}
              </div>

              <button
                onClick={handleSubmit}
                disabled={!selectedProvider || !captchaVerified}
                className="bg-primary-dark text-white px-5 py-2.5 rounded-lg font-medium text-sm hover:bg-turquoise-900 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
              >
                Continue
              </button>
            </div>

            {/* Dropdown */}
            {showDropdown && (
              <div className="absolute z-10 w-full mt-1 bg-white border border-gray-200 rounded-lg shadow-lg max-h-60 overflow-y-auto">
                {providers.length === 0 && !loading && (
                  <div className="px-4 py-3 text-gray-500 text-sm">{searchTerm.length < 2 ? "Type at least 2 characters to search..." : "No institutions found"}</div>
                )}

                {providers.map((provider) => (
                  <button
                    key={provider.provider_uuid}
                    onClick={() => handleProviderSelect(provider)}
                    className="w-full px-4 py-3 text-left hover:bg-gray-50 focus:bg-gray-50 focus:outline-none border-b border-gray-100 last:border-b-0"
                  >
                    <div className="font-medium text-gray-900 mb-1.5">{provider.provider_name}</div>
                    <div className="flex gap-4 text-xs text-gray-600">
                      <div className="flex gap-1">
                        <span className="font-medium">DEQAR ID:</span>
                        <span>{provider.deqar_id}</span>
                      </div>
                      <div className="flex gap-1">
                        <span className="font-medium">ETER ID:</span>
                        <span>{provider.eter_id}</span>
                      </div>
                    </div>
                  </button>
                ))}
              </div>
            )}
          </div>

          {/* Error Display */}
          {error && <div className="text-red-600 text-sm">Error: {error}</div>}

          {/* Selected Institution Display */}
          {selectedProvider && (
            <div className="mt-2 p-3 bg-green-50 border border-green-200 rounded-lg">
              <div className="text-sm text-green-800">
                <div className="font-semibold mb-1">Selected Institution:</div>
                <div className="font-medium">{selectedProvider.provider_name}</div>
                <div className="flex gap-4 mt-1 text-xs">
                  <span>DEQAR ID: {selectedProvider.deqar_id}</span>
                  <span>ETER ID: {selectedProvider.eter_id}</span>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
