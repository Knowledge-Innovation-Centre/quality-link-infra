import Navbar from '@/components/layout/Navbar'
import Footer from '@/components/layout/Footer'
import { Accordion, AccordionItem } from '@/components/ui/Accordion'

export default function HelpPage() {
  return (
    <div className="min-h-screen bg-white flex flex-col">
      <Navbar />

      <main className="flex-1 bg-white">
        {/* Hero Section */}
        <div className="bg-gray-50 border-b border-gray-200">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-16">
            <h1 className="text-4xl font-bold text-gray-900">
              Help and Support
            </h1>
            <p className="text-gray-600 mt-2 text-lg">
              Find answers to common questions and technical guidance
            </p>
          </div>
        </div>

        <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
          <div className="space-y-12">
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

            {/* Technical Documentation */}
            <section>
              <h2 className="text-2xl font-bold text-gray-900 mb-6">
                Technical Documentation
              </h2>
              
              <div className="prose prose-gray max-w-none">
                <p className="text-gray-700 leading-relaxed">
                  QualityLink provides the technical infrastructure to create joint 
                  catalogues of learning opportunities. This is a foundational part 
                  of a European University Alliance's virtual inter-university campus 
                  and addresses use case 1 ("Discover") of the European Higher Education 
                  Interoperability Framework (HEIF).
                </p>

                <h3 className="text-xl font-bold text-gray-900 mt-8 mb-4">
                  What are the implementation options?
                </h3>
                <p className="text-gray-700 leading-relaxed">
                  QualityLink's modular architecture provides different options for 
                  each of the three main layers. The options can be freely combined 
                  with each other.
                </p>

                <div className="mt-6 bg-gray-100 rounded-lg p-6">
                  <p className="text-sm text-gray-600">
                    For complete technical specifications and implementation guides, 
                    please refer to the full documentation.
                  </p>
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


