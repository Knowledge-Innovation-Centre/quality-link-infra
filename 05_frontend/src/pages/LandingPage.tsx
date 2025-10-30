import { useNavigate } from "react-router-dom";
import InstitutionSelector from "@/components/features/InstitutionSelector";
import qlIcon from "@/assets/images/ql-icon.png";

export default function LandingPage() {
  const navigate = useNavigate();

  const handleInstitutionSelect = (providerUuid: string) => {
    navigate(`/dashboard/${providerUuid}`);
  };

  return (
    <div className="min-h-screen bg-white">
      <div className="grid lg:grid-cols-2 min-h-screen">
        {/* Left Panel - Info */}
        <div className="bg-[#0B223B] flex items-center justify-center p-8 lg:p-16">
          <div className="max-w-xl">
            <div className="flex flex-col gap-6">
              <div className="w-16 h-16 flex items-center justify-center">
                <img src={qlIcon} alt="QualityLink" className="w-full h-full object-contain" />
              </div>
              <h1 className="text-4xl font-bold text-white">QualityLink Dashboard</h1>
              <p className="text-lg text-white/90 leading-relaxed">
                QualityLink uses DEQAR (based on OrgReg) as upstream data source of recognised higher education institutions. The aggregator is currently open to higher education institutions from the
                European Higher Education Area (EHEA).
              </p>
              <p className="text-lg text-white/90 leading-relaxed">
                If your institution is missing, please contact{" "}
                <a href="mailto:contact@quality-link.eu" className="text-white hover:underline font-semibold">
                  contact@quality-link.eu
                </a>
              </p>
              <p className="text-lg text-white/90 leading-relaxed">
                If you are another data provider that has data on learning opportunities and related quality indicators, please contact{" "}
                <a href="mailto:contact@quality-link.eu" className="text-white hover:underline font-semibold">
                  contact@quality-link.eu
                </a>
                .
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
  );
}
