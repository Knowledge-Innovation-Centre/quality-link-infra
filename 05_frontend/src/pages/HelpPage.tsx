import Navbar from '@/components/layout/Navbar'
import Footer from '@/components/layout/Footer'
import { Accordion, AccordionItem } from '@/components/ui/Accordion'
import implementationDiagram from '@/assets/images/implementation.png'

export default function HelpPage() {
  return (
    <div className="min-h-screen bg-white flex flex-col">
      <Navbar />

      <main className="flex-1 bg-white">
        {/* Hero Section */}
        <div className="bg-gray-50 border-b border-gray-200 relative">
          {/* Dotted background pattern - gray/50 background with gray/200 dots */}
          <div
            className="absolute inset-0 pointer-events-none"
            style={{
              backgroundImage: `radial-gradient(circle, #e5e7eb 1px, transparent 1px)`,
              backgroundSize: '16px 16px',
            }}
          />

          <div className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8 py-16 relative">
            <div className="max-w-5xl mx-auto">
              <h1 className="text-4xl font-bold text-gray-900 leading-tight">
                Help and Support
              </h1>
              <p className="text-gray-600 mt-2 text-lg">
                Find answers to common questions and technical guidance
              </p>
            </div>
          </div>
        </div>

        <div className="flex flex-col items-center px-20 pt-[60px] pb-20">
          <div className="max-w-5xl mx-auto space-y-8">
            {/* FAQ Section */}
            <section>
              <h2 className="text-2xl font-bold text-gray-900 mb-6">
                Frequently Asked Questions
              </h2>
              
              <Accordion>
                <AccordionItem title="How to upload a manifest file" defaultOpen>
                  <p className="mb-4">
                    To upload a manifest file, you have two main options:
                  </p>
                  <ol className="list-decimal list-inside space-y-2">
                    <li>
                      Place the manifest file at one of these URLs:
                      <ul className="list-disc list-inside ml-6 mt-2">
                        <li>https://YOUR.DOMAIN/.well-known/quality-link-manifest</li>
                        <li>https://YOUR.DOMAIN/.well-known/quality-link-manifest.json</li>
                        <li>https://YOUR.DOMAIN/.well-known/quality-link-manifest.yaml</li>
                      </ul>
                    </li>
                    <li className="mt-2">
                      Or create a DNS TXT record pointing to your manifest file location.
                    </li>
                  </ol>
                </AccordionItem>
                
                <AccordionItem title="My manifest file is not found">
                  <p className="mb-4">
                    If your manifest file is not being found, check the following:
                  </p>
                  <ul className="list-disc list-inside space-y-2">
                    <li>Ensure the file is accessible via HTTPS</li>
                    <li>Verify the file is in the correct location (.well-known directory)</li>
                    <li>Check that your web server is configured to serve .json or .yaml files</li>
                    <li>Confirm there are no CORS issues preventing access</li>
                    <li>Use the "Refresh Discovery" button after making changes</li>
                  </ul>
                </AccordionItem>
                
                <AccordionItem title="There are mistakes in the known identifiers">
                  <p className="mb-4">
                    Known identifiers are sourced from upstream databases including:
                  </p>
                  <ul className="list-disc list-inside space-y-2 mb-4">
                    <li>DEQAR (Database of External Quality Assurance Results)</li>
                    <li>EWP HEI API (Erasmus Without Paper)</li>
                    <li>OrgReg (Organization Registry)</li>
                  </ul>
                  <p>
                    If you notice errors in these identifiers, please contact the respective 
                    upstream data provider to correct the information at the source. Changes 
                    will be reflected in QualityLink after the next synchronization.
                  </p>
                </AccordionItem>

                <AccordionItem title="What data formats are supported?">
                  <p className="mb-4">
                    QualityLink supports the following data formats:
                  </p>
                  <ul className="list-disc list-inside space-y-2">
                    <li>European Learning Model (ELM) format with RDF data</li>
                    <li>OOAPI (Open Education API)</li>
                    <li>Edu-API</li>
                    <li>OCCAPI (Open Course Catalogue API)</li>
                  </ul>
                  <p className="mt-4">
                    All formats are normalized to ELM during the aggregation process to 
                    ensure interoperability across different systems.
                  </p>
                </AccordionItem>

                <AccordionItem title="How often is data refreshed?">
                  <p>
                    The QualityLink aggregator automatically fetches data from your manifest 
                    file at 2:00 AM each day. You can also manually trigger a refresh using 
                    the "Refresh Discovery" button on your dashboard. Please note that 
                    discovery (finding your manifest) and aggregation (fetching your data) 
                    are separate processes that may run at different intervals.
                  </p>
                </AccordionItem>
              </Accordion>
            </section>

            <hr className="border-gray-200" />

            {/* Technical Documentation */}
            <section>
              <h2 className="text-2xl font-bold text-gray-900 mb-6">
                Technical Documentation
              </h2>

              <div className="prose prose-gray max-w-none space-y-8">
                <p className="text-gray-700 leading-relaxed">
                  QualityLink provides the technical infrastructure to create joint catalogues of learning opportunities. This is a foundational part of a European University Alliance's virtual inter-university campus and addresses use case 1 ("Discover") of the European Higher Education Interoperability Framework (HEIF).
                </p>

                <p className="text-gray-700 leading-relaxed">
                  As a technical implementer, you'll be connecting your institution's learning opportunities to Europe-wide platforms using standard, interoperable formats. This guide provides the technical specifications, implementation options, and step-by-step instructions you need to get your data live.
                </p>

                <div className="mt-8">
                  <h3 className="text-xl font-bold text-gray-900 mb-4">
                    What are the implementation options?
                  </h3>
                  <p className="text-gray-700 leading-relaxed">
                    QualityLink's modular architecture provides different options for each of the three main layers. The options can be freely combined with each other, i.e. you can choose different approaches for each layer and also combine different approaches within one alliance.
                  </p>
                </div>

                <div className="mt-8">
                  <img
                    src={implementationDiagram}
                    alt="QualityLink implementation architecture diagram showing the three main layers and options"
                    className="rounded-lg w-full"
                  />
                </div>

                <div className="mt-8">
                  <h3 className="text-xl font-bold text-gray-900 mb-4">
                    1. Publish information
                  </h3>
                  <div className="space-y-4 text-gray-700 leading-relaxed">
                    <div>
                      <p className="mb-2"><strong>1. European Learning Model (ELM) format:</strong> you can serve a file with RDF data using the ELM ontology and aligned with the QualityLink application profile.</p>
                      <p className="mb-2">Requirements: see the QualityLink application profile</p>
                      <p className="mb-2">Validation: use the SHACL validator at https://shacl-play.sparna.fr/play/validate – upload your dataset or enter URL under "Input Data", select "URL" under "Shapes" and enter https://specs.quality-link.eu/resources/ontology-shacl.ttl</p>
                      <p>Benefits: ELM offers maximum semantic richness and direct alignment with European standards; you can generate or export ELM from existing systems</p>
                    </div>

                    <div>
                      <p className="mb-2"><strong>2. API Implementation:</strong> you can implement OOAPI, Edu-API or OCCAPI endpoints to provide course data.</p>
                      <p className="mb-2">Requirements: implement at least the required endpoints as specified in the Data Exchange Specification</p>
                      <p>Benefits: you can use existing API infrastructure or expose data real-time from existing systems; all data is normalised to ELM during aggregation</p>
                    </div>

                    <p className="italic">NB: you can use one or several standards within an alliance. Even if different alliance members use different standards, all aggregated data is converted to ELM.</p>
                  </div>
                </div>

                <div className="mt-8">
                  <h3 className="text-xl font-bold text-gray-900 mb-4">
                    2. Aggregate data
                  </h3>
                  <div className="space-y-4 text-gray-700 leading-relaxed">
                    <p>There are three deployment options in terms of data aggregation:</p>

                    <div>
                      <p className="mb-2"><strong>1. Hosted QualityLink aggregator:</strong> the QualityLink consortium hosts an aggregator that European higher education institutions or alliances may freely use.</p>
                      <p className="mb-2">✓ Benefits: this option allows immediate deployment without infrastructure investment; we recommend this option for getting started.</p>
                      <p>✗ Limitations: you have some options to customise the aggregation process and schedule, but cannot adapt the software directly.</p>
                    </div>

                    <div>
                      <p className="mb-2"><strong>2. Self-deployed aggregator:</strong> you can run your own instance of the QualityLink aggregator software since it is open source.</p>
                      <p className="mb-2">✓ Benefits: you can have full control over the aggregation process without any external dependencies and you can customise the software if needed.</p>
                      <p>✗ Limitations: this option requires additional technical expertise and additional resources for infrastructure and maintenance.</p>
                    </div>

                    <div>
                      <p className="mb-2"><strong>3. Custom aggregator:</strong> you can build your own aggregator based on the technical specifications.</p>
                      <p className="mb-2">✓ Benefits: this option allows maximum flexibility and can cater for specific requirements that the QualityLink aggregator cannot meet; it gives full ownership and can be integrated with existing systems.</p>
                      <p>✗ Limitations: this option requires significant own development effort and resources for infrastructure and maintenance.</p>
                    </div>

                    <p className="italic">NB: You can always change from the hosted aggregator to one of the other options at a later stage if needed.</p>
                  </div>
                </div>

                <div className="mt-8">
                  <h3 className="text-xl font-bold text-gray-900 mb-4">
                    3. Public joint catalogue
                  </h3>
                  <div className="space-y-4 text-gray-700 leading-relaxed">
                    <p>There are three approaches how you can publish your joint catalogue:</p>

                    <div>
                      <p className="mb-2"><strong>1. QualityLink platform:</strong> you can use a filtered view of the Europe-wide platform</p>
                      <p className="mb-2">✓ Benefits: this option can be used quickly and immediately once your data is aggregated; as it does not require any own infrastructure or development this is a good option to get started.</p>
                      <p>✗ Limitations: this option only works with the hosted QualityLink aggregator; customisation and alliance branding are limited.</p>
                    </div>

                    <div>
                      <p className="mb-2"><strong>2. Self-deployed catalogue:</strong> you can deploy the QualityLink platform yourself, as it is open source</p>
                      <p className="mb-2">✓ Benefits: this option can be deployed easily on your own infrastructure/private cloud; you can customise the user experience and branding according to your needs.</p>
                      <p>✗ Limitations: it requires some technical expertise and resources for infrastructure and maintenance; customisations require additional development work.</p>
                    </div>

                    <div>
                      <p className="mb-2"><strong>3. Bespoke development:</strong> you can develop and deploy your own public catalogue frontend.</p>
                      <p className="mb-2">✓ Benefits: this option works with all aggregation options and allows full customisation; you could use the API of the QualityLink aggregator or a custom API of your own aggregator.</p>
                      <p>✗ Limitations: this option implies significant development effort and requires resources for development, infrastructure and maintenance.</p>
                    </div>
                  </div>
                </div>

                <div className="mt-8">
                  <h3 className="text-xl font-bold text-gray-900 mb-4">
                    What are the practical steps?
                  </h3>
                  <div className="space-y-4 text-gray-700 leading-relaxed">
                    <p>
                      <strong>1.</strong> Publish your learning opportunities data in a supported format: <a href="https://europa.eu/europass/elm-browser/index.html" className="text-brand-base hover:underline">ELM</a>, <a href="https://openonderwijsapi.nl/#/" className="text-brand-base hover:underline">OOAPI</a>, <a href="https://www.1edtech.org/standards/edu-api" className="text-brand-base hover:underline">Edu-API</a> or <a href="https://occapi.uni-foundation.eu/" className="text-brand-base hover:underline">OCCAPI</a>. See the <a href="https://specs.quality-link.eu/data_exchange.html" className="text-brand-base hover:underline">Data Exchange Specification</a> for details.
                    </p>

                    <p>
                      <strong>2.</strong> Create a manifest file: this is a JSON or YAML file that indicates the type and location of your data source. You can also configure how frequently your data will be refreshed and whether any authentication is necessary. See the <a href="https://specs.quality-link.eu/discovery.html" className="text-brand-base hover:underline">Data Source Discovery Specification</a> for details.
                    </p>

                    <p>
                      <strong>3.</strong> Check your domain search order: go to the QualityLink dashboard (launch in September 2025) and review the domain search order for your institution. You will need to place/link the manifest file under/from one of these domains.
                    </p>

                    <div>
                      <p className="mb-2"><strong>4.</strong> Place the manifest file: you have two options where to place the manifest file.</p>
                      <div className="ml-6 space-y-2">
                        <div>
                          <p className="mb-1">a. At one of the URLs:</p>
                          <ul className="list-none ml-4 space-y-1 font-mono text-sm">
                            <li>https://YOUR.DOMAIN/.well-known/quality-link-manifest</li>
                            <li>https://YOUR.DOMAIN/.well-known/quality-link-manifest.json</li>
                            <li>https://YOUR.DOMAIN/.well-known/quality-link-manifest.yaml</li>
                          </ul>
                        </div>
                        <div>
                          <p className="mb-1">b. At another URL and to create a DNS TXT record under one of the searched domains, using the value:</p>
                          <p className="ml-4 font-mono text-sm">v=qldiscover1; m=https://YOUR.OTHER.DOMAIN/SOME/PATH/…</p>
                        </div>
                      </div>
                    </div>

                    <p>
                      <strong>5.</strong> Trigger aggregation: return to the QualityLink dashboard (launch in September 2025) and trigger aggregation for the first time. The dashboard also allows you to verify that manifest file was found and interpreted correctly.
                    </p>

                    <p>
                      <strong>6.</strong> Lean back and relax: your learning opportunities now appear in the QualityLink pilot platform (launch in the autumn of 2025).
                    </p>
                  </div>
                </div>
              </div>
            </section>
          </div>
        </div>
      </main>

      <Footer />
    </div>
  )
}


