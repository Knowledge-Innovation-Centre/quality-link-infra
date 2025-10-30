import ql from "@/assets/images/QualityLink_logo_horizontal-white.png";
import eu from "@/assets/images/eu.png";

export default function Footer() {
  return (
    <footer className="bg-footer-gradient">
      <div className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8 py-16 relative">
        <div className="max-w-5xl mx-auto">
          <div className="flex items-center justify-between">
            <div className="flex flex-col gap-4">
              <p className="text-turquoise-50 text-lg font-semibold flex items-center">
                Made with support of the{" "}
                <span className="inline-flex items-center gap-2">
                  <img src={ql} alt="lcamp logo" className="w-[130px] ml-2 pt-[1px]" />
                  <span>project</span>
                </span>
              </p>
              {/* <button className="bg-white/10 px-3 py-2 rounded-lg text-white text-sm font-semibold hover:bg-white/20 transition-colors self-start">Visit LCAMP</button> */}
            </div>

            {/* Partner logos placeholder */}
            <img src={eu} alt="eu union" className="self-end w-[300px]" />
          </div>
        </div>
      </div>
    </footer>
  );
}
