import { useNavigate } from 'react-router-dom'
import InstitutionSelector from '@/components/features/InstitutionSelector'

export default function LandingPage() {
  const navigate = useNavigate()

  const handleInstitutionSelect = (providerUuid: string) => {
    navigate(`/dashboard/${providerUuid}`)
  }

  return (
    <div className="min-h-screen bg-white">
      <div className="grid lg:grid-cols-2 min-h-screen">
        {/* Left Panel - Info */}
        <div className="bg-white flex items-center justify-center p-8 lg:p-16">
          <div className="max-w-xl">
            <div className="flex flex-col gap-6">
              <div className="w-16 h-16 bg-primary/10 rounded-lg flex items-center justify-center">
                <span className="text-3xl">📊</span>
              </div>
              <h1 className="text-4xl font-bold text-gray-900">
                QualityLink Dashboard
              </h1>
              <p className="text-lg text-gray-700 leading-relaxed">
                QualityLink uses DEQAR (based on OrgReg) and the EWP HEI API as 
                upstream data sources of recognised higher education institutions. 
                The aggregator is currently open to higher education institutions 
                from the European Higher Education Area (EHEA). If your institution 
                is missing, please contact ***. If you are another data provider 
                that has data on learning opportunities and related quality 
                indicators, please contact ***.
              </p>
            </div>
          </div>
        </div>

        {/* Right Panel - Selector */}
        <div className="bg-gray-50 flex items-center justify-center p-8 lg:p-16 border-l border-gray-200">
          <div className="w-full max-w-xl">
            <InstitutionSelector onSelect={handleInstitutionSelect} />
          </div>
        </div>
      </div>
    </div>
  )
}


