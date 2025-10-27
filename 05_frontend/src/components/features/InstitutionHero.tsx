import { ExternalLink } from 'lucide-react'

interface InstitutionHeroProps {
  name: string
  nameLocal?: string
  website?: string
  lastUpdated?: string
  className?: string
}

export default function InstitutionHero({
  name,
  nameLocal,
  website,
  lastUpdated,
  className = '',
}: InstitutionHeroProps) {
  return (
    <div className={`bg-gray-50 border-b border-gray-200 relative ${className}`}>
      {/* Dotted background pattern - gray/50 background with gray/200 dots */}
      <div 
        className="absolute inset-0 pointer-events-none"
        style={{
          backgroundImage: `radial-gradient(circle, #e5e7eb 1px, transparent 1px)`,
          backgroundSize: '16px 16px',
        }}
      />
      
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-16 relative">
        <div className="max-w-4xl mx-auto">
          <div className="flex flex-col lg:flex-row lg:items-start lg:justify-between gap-10">
            {/* Left: Institution Name */}
            <div className="flex flex-col gap-2 flex-1">
              <h1 className="text-4xl font-bold text-gray-900 leading-tight">
                {name}
              </h1>
              {nameLocal && (
                <p className="text-xl text-gray-500 leading-tight">
                  {nameLocal}
                </p>
              )}
            </div>

            {/* Right: Metadata */}
            <div className="flex flex-col sm:flex-row gap-8 shrink-0">
              {/* Last Updated */}
              {lastUpdated && (
                <div className="flex flex-col gap-0.5">
                  <p className="text-sm font-semibold text-gray-500 whitespace-nowrap">
                    Last updated from DEQAR/HEI API:
                  </p>
                  <p className="text-sm text-gray-900 whitespace-nowrap">
                    {lastUpdated}
                  </p>
                </div>
              )}

              {/* Website */}
              {website && (
                <div className="flex flex-col gap-0.5">
                  <p className="text-sm font-semibold text-gray-500 whitespace-nowrap">
                    Website:
                  </p>
                  <a
                    href={website}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-sm text-brand-600 hover:text-brand-700 underline decoration-1 underline-offset-2 flex items-center gap-1 group"
                  >
                    <span>{website}</span>
                    <ExternalLink className="w-3.5 h-3.5 opacity-0 group-hover:opacity-100 transition-opacity" />
                  </a>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}

