import Button from '../ui/Button'

export default function Footer() {
  return (
    <footer className="bg-footer-gradient py-16">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="max-w-4xl">
          <div className="flex items-center justify-between">
            <div className="flex flex-col gap-4">
              <p className="text-turquoise-50 text-lg font-semibold">
                Made with support of the{' '}
                <span className="inline-flex items-center gap-2">
                  <span className="inline-block w-16 h-5 bg-white/20 rounded" />
                  <span>project</span>
                </span>
              </p>
              <button className="bg-white/10 px-3 py-2 rounded-lg text-white text-sm font-semibold hover:bg-white/20 transition-colors self-start">
                Visit LCAMP
              </button>
            </div>
            
            {/* Partner logos placeholder */}
            <div className="w-80 h-16 bg-white/10 rounded" />
          </div>
        </div>
      </div>
    </footer>
  )
}


