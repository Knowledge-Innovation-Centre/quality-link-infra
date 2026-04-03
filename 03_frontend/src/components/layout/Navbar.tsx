import { Link, useLocation } from 'react-router-dom'

interface NavbarProps {
  showActions?: boolean
}

export default function Navbar({ showActions = false }: NavbarProps) {
  const location = useLocation()

  const navItems = [
    { to: '/', label: 'Dashboard', match: (path: string) => path === '/' || path.startsWith('/dashboard') },
    { to: '/manifest-builder', label: 'Manifest Builder', match: (path: string) => path === '/manifest-builder' },
    { to: '/help', label: 'Help', match: (path: string) => path === '/help' },
  ]

  return (
    <nav className="bg-primary text-white shadow-md">
      <div className="max-w-10xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between items-center h-20">
          {/* Logo and Navigation */}
          <div className="flex items-center gap-4">
            <div className="w-10 h-10 bg-white rounded flex items-center justify-center">
              {/* Logo placeholder */}
              <span className="text-primary font-bold text-xl">Q</span>
            </div>
            <div className="h-8 w-px bg-white/30" />
            <div className="flex items-center gap-2">
              {navItems.map((item) => {
                const isActive = item.match(location.pathname)
                return (
                  <div
                    key={item.to}
                    className={`px-3 py-2 rounded-lg transition-colors ${
                      isActive ? 'bg-white/25' : 'bg-white/10 hover:bg-white/20'
                    }`}
                  >
                    <Link to={item.to} className="text-white text-sm font-medium">
                      {item.label}
                    </Link>
                  </div>
                )
              })}
            </div>
          </div>

          {/* CTA Buttons */}
          {showActions && (
            <div className="flex items-center gap-3">
              
              <button className="bg-white/10 px-3 py-2 rounded-lg text-white text-sm font-medium hover:bg-white/20 transition-colors">
                Sign in
              </button>
              <button className="bg-white/10 px-3 py-2 rounded-lg text-white text-sm font-medium hover:bg-white/20 transition-colors">
                Create an account
              </button>
            </div>
          )}
        </div>
      </div>
    </nav>
  )
}


